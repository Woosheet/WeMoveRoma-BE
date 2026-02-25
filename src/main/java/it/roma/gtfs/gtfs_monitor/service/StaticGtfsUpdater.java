package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Slf4j
@Service
public class StaticGtfsUpdater {

    private final GtfsProperties props;
    private final WebClient http;
    private volatile String etag;
    private volatile String lastModified;
    private final GtfsIndexService indexService;

    private volatile boolean refreshFailed = false;
    private final AtomicBoolean refreshInProgress = new AtomicBoolean(false);

    public StaticGtfsUpdater(GtfsProperties props,
                             @Qualifier("staticGtfsWebClient") WebClient http,
                             GtfsIndexService indexService) throws IOException {

        this.props = props;
        this.http = http;
        this.indexService = indexService;

        if (props.staticProps() == null || props.staticProps().dataDir() == null) {
            throw new IllegalStateException("gtfs.static-props.data-dir non configurato");
        }

        Files.createDirectories(Path.of(props.staticProps().dataDir()));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        Thread t = new Thread(() -> runRefresh("startup"), "static-gtfs-startup-refresh");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Refresh programmato: tutti i giorni alle 06:40 e alle 20:00 (Europe/Rome).
     * Se fallisce, il flag refreshFailed resta true e parte il meccanismo di retry ogni 5 minuti.
     */
    @Scheduled(cron = "0 40 6,20 * * *", zone = "Europe/Rome")
    public void scheduledDailyRefresh() {
        runRefresh("cron");
    }

    /**
     * Job di retry: ogni 5 minuti controlla se l’ultimo refresh è fallito.
     * Se refreshFailed == true → ritenta; se va bene rimette refreshFailed = false e smette.
     */
    @Scheduled(fixedDelay = 5 * 60 * 1000L, initialDelay = 5 * 60 * 1000L, zone = "Europe/Rome")
    public void scheduledRetryIfFailed() {
        if (!refreshFailed) {
            return;
        }
        log.warn("[StaticGTFS] Ultimo refresh FALLITO: provo un retry ogni 5 minuti…");
        runRefresh("retry-5m");
    }

    // -------------------------------------------------------
    //  LOGICA DI REFRESH (riusata da startup / cron / retry)
    // -------------------------------------------------------
    private void runRefresh(String reason) {
        if (!refreshInProgress.compareAndSet(false, true)) {
            log.warn("[StaticGTFS] Refresh già in corso, skip ({})", reason);
            return;
        }

        String url = props.staticProps().url();
        log.info("[StaticGTFS] Inizio refresh ({}) URL={}", reason, url);

        // in modo pessimistico consideriamo fallito, finché non completiamo con successo
        refreshFailed = true;

        try {
            http.get()
                    .uri(url)
                    .headers(h -> {
                        if (etag != null) {
                            log.info("[StaticGTFS] Inviato If-None-Match={}", etag);
                            h.setIfNoneMatch(etag);
                        }
                        if (lastModified != null) {
                            // lastModified che salviamo è già ISO/Instant oppure raw, quindi qui stiamo cauti
                            try {
                                // se è in formato RFC_1123 la parse funziona
                                ZonedDateTime zdt = ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME);
                                h.setIfModifiedSince(zdt.toInstant());
                            } catch (Exception ex2) {
                                // altrimenti lo mandiamo come stringa grezza
                                h.set(HttpHeaders.IF_MODIFIED_SINCE, lastModified);
                            }
                        }
                    })
                    .retrieve()
                    .toEntity(byte[].class)
                    .flatMap(response -> {
                        log.info("[StaticGTFS] Ricevuta risposta HTTP {} con ETag={} Last-Modified={}",
                                response.getStatusCode(),
                                response.getHeaders().getETag(),
                                response.getHeaders().getLastModified());

                        return handleZipResponse(response);
                    })
                    .onErrorResume(e -> {
                        // ERRORE (es: 502 Bad Gateway, timeout, ecc.)
                        log.error("[StaticGTFS] Errore durante refresh ({}): {}", reason, e.toString());
                        // refreshFailed resta true → il job ogni 5 minuti continuerà a ritentare
                        return Mono.empty();
                    })
                    .block();
        } finally {
            refreshInProgress.set(false);
            log.info("[StaticGTFS] Refresh ({}) terminato (refreshFailed = {})", reason, refreshFailed);
        }
    }

    private Mono<Void> handleZipResponse(ResponseEntity<byte[]> resp) {

        HttpStatusCode status = resp.getStatusCode();
        log.info("[StaticGTFS] handleZipResponse avviato. HTTP={}", status.value());

        // 304 → nessun aggiornamento, ma NON è un errore → disattiviamo il retry
        if (status.value() == 304) {
            log.warn("[StaticGTFS] Nessun aggiornamento (304 Not Modified)");
            refreshFailed = false;
            return Mono.empty();
        }

        if (resp.getBody() == null) {
            log.warn("[StaticGTFS] Corpo vuoto, nessun aggiornamento");
            // lo consideriamo un fallimento → refreshFailed = true
            refreshFailed = true;
            return Mono.empty();
        }

        this.etag = resp.getHeaders().getFirst(HttpHeaders.ETAG);
        this.lastModified = resp.getHeaders().getFirst(HttpHeaders.LAST_MODIFIED);

        log.info("[StaticGTFS] Nuovi header: ETag={} Last-Modified={}", etag, lastModified);

        try {
            Path tmp = Files.createTempFile("gtfs_static_", ".zip");
            log.info("[StaticGTFS] Creato file temporaneo: {}", tmp);

            Files.write(tmp, resp.getBody());
            log.info("[StaticGTFS] Zip scaricato. Dimensione={} bytes", resp.getBody().length);

            Path targetDir = Path.of(props.staticProps().dataDir());
            log.info("[StaticGTFS] Estrazione zip in directory: {}", targetDir);

            extractZipAtomically(tmp, targetDir);
            log.info("[StaticGTFS] Estrazione completata");

            Files.deleteIfExists(tmp);
            log.info("[StaticGTFS] File temporaneo eliminato");

            log.info("[StaticGTFS] Ricostruzione indici in corso");
            indexService.rebuildIndexes();
            log.info("[StaticGTFS] Indici ricostruiti correttamente");

            // QUI consideriamo il refresh riuscito → stop retry
            refreshFailed = false;

        } catch (Exception e) {
            log.error("[StaticGTFS] Errore durante estrazione zip: {}", e.toString());
            // fallimento → lasciamo refreshFailed = true per attivare i retry ogni 5'
            refreshFailed = true;
        }

        return Mono.empty();
    }

    private void extractZipAtomically(Path zipPath, Path targetDir) throws IOException {

        log.info("[StaticGTFS] Estrazione ZIP atomica iniziata. zipPath={} targetDir={}", zipPath, targetDir);

        Path newDir = targetDir.getParent().resolve(targetDir.getFileName() + "_new");
        log.info("[StaticGTFS] Directory temporanea di estrazione: {}", newDir);

        if (Files.exists(newDir)) {
            log.warn("[StaticGTFS] newDir esiste già. Rimozione in corso");
            deleteRecursively(newDir);
        }

        Files.createDirectories(newDir);
        Path normalizedNewDir = newDir.toAbsolutePath().normalize();

        try (ZipFile zip = new ZipFile(zipPath.toFile())) {
            log.info("[StaticGTFS] ZIP aperto. Numero entry={}", zip.size());

            Enumeration<? extends ZipEntry> entries = zip.entries();

            while (entries.hasMoreElements()) {
                ZipEntry e = entries.nextElement();
                if (e.isDirectory()) continue;

                Path out = normalizedNewDir.resolve(e.getName()).normalize();
                if (!out.startsWith(normalizedNewDir)) {
                    throw new IOException("[StaticGTFS] ZIP entry path traversal rilevato: " + e.getName());
                }
                Files.createDirectories(out.getParent());

                log.debug("[StaticGTFS] Estrazione file: {}", e.getName());

                try (InputStream in = zip.getInputStream(e)) {
                    Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }

        Path backup = targetDir.getParent().resolve(targetDir.getFileName() + "_bak");
        log.info("[StaticGTFS] Directory di backup: {}", backup);

        if (Files.exists(backup)) {
            log.warn("[StaticGTFS] Backup esistente trovato. Rimozione in corso");
            deleteRecursively(backup);
        }

        if (Files.exists(targetDir)) {
            log.info("[StaticGTFS] Spostamento directory esistente in backup");
            Files.move(targetDir, backup, StandardCopyOption.ATOMIC_MOVE);
            log.info("[StaticGTFS] Backup creato: {}", backup);
        }

        log.info("[StaticGTFS] Promozione newDir a targetDir tramite atomic move");
        Files.move(newDir, targetDir, StandardCopyOption.ATOMIC_MOVE);

        if (Files.exists(backup)) {
            log.info("[StaticGTFS] Rimozione backup");
            deleteRecursively(backup);
        }

        log.info("[StaticGTFS] Salvataggio stato JSON");
        saveStateJson();

        log.info("[StaticGTFS] Estrazione ZIP atomica completata");
    }

    private void deleteRecursively(Path p) throws IOException {
        if (!Files.exists(p)) return;
        Files.walk(p)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(path -> {
                    try { Files.deleteIfExists(path); } catch (IOException ignored) {}
                });
    }

    private void saveStateJson() {
        try {
            Path dataDir = Path.of(props.staticProps().dataDir());
            Path stateFile = dataDir.resolve(".gtfs_static_state.json");

            String json = """
        {
            "etag": "%s",
            "last_modified": "%s",
            "fetched_at": "%s"
        }
        """.formatted(
                    etag == null ? "" : etag.replace("\"", ""),
                    lastModified == null ? "" : lastModified,
                    Instant.now()
            );

            Files.writeString(stateFile, json,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        } catch (Exception e) {
            log.error("[StaticGTFS] Errore salvataggio stato JSON: {}", e.toString());
        }
    }
}
