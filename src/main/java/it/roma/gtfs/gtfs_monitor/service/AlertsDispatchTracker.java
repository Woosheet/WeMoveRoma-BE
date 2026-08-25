package it.roma.gtfs.gtfs_monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Mantiene su file lo stato "alert gia' notificati": un set di stringhe
 * id+inizio_epoch (per distinguere ripetizioni dello stesso alert con periodi
 * diversi). All'avvio carica dal file; ogni nuova osservazione marca + persiste.
 *
 * Persistente cosi' una restart del backend NON ri-spara notifiche per alert
 * gia' visti. Il file e' un JSON semplice: {"seen": ["id1@123456", ...]}.
 */
@Slf4j
@Component
public class AlertsDispatchTracker {

    @Value("${notifications.state-file:${user.home}/.wemoveroma/alerts-last-seen.json}")
    private String stateFilePath;

    private final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);
    private final ReentrantLock lock = new ReentrantLock();
    private final Set<String> seen = new HashSet<>();

    @PostConstruct
    void load() {
        Path p = Path.of(stateFilePath);
        if (!Files.isRegularFile(p)) {
            log.info("[AlertsTracker] state file inesistente ({}), parto vuoto.", p);
            return;
        }
        try {
            State s = mapper.readValue(p.toFile(), State.class);
            if (s.seen != null) {
                seen.addAll(s.seen);
            }
            log.info("[AlertsTracker] caricati {} alert id da {}", seen.size(), p);
        } catch (IOException e) {
            log.warn("[AlertsTracker] lettura {} fallita ({}); parto vuoto.", p, e.toString());
        }
    }

    /**
     * @return true se l'id non era ancora stato visto (= "nuovo" da notificare).
     *         Marca lo stato e persiste su file.
     */
    public boolean markIfNew(String id) {
        if (id == null || id.isBlank()) return false;
        lock.lock();
        try {
            if (!seen.add(id)) {
                return false;
            }
            persistInternal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Lista immutabile per uso diagnostico. */
    public Set<String> snapshot() {
        lock.lock();
        try {
            return Collections.unmodifiableSet(new HashSet<>(seen));
        } finally {
            lock.unlock();
        }
    }

    private void persistInternal() {
        Path p = Path.of(stateFilePath);
        try {
            Path parent = p.getParent();
            if (parent != null && !Files.isDirectory(parent)) {
                Files.createDirectories(parent);
            }
            mapper.writeValue(p.toFile(), new State(seen));
        } catch (IOException e) {
            log.warn("[AlertsTracker] persist {} fallita: {}", p, e.toString());
        }
    }

    /** DTO interno per (de)serializzazione. Espone seen pubblicamente per Jackson. */
    @SuppressWarnings("unused")
    static final class State {
        public Set<String> seen;

        public State() { this.seen = new HashSet<>(); }
        State(Set<String> seen) { this.seen = seen; }
    }

    /** Esposto per uso in test. */
    Map<String, Object> debugInfo() {
        return Map.of("size", seen.size(), "stateFile", stateFilePath);
    }
}
