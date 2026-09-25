package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiLinePunctualityDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Storico del servizio osservato: passaggi alle fermate e corse, su Postgres.
 *
 * IL DATABASE HA UN THREAD SUO. I @Scheduled condividono un pool di sei thread
 * (spring.task.scheduling.pool.size) con i poll di mezzi, arrivi e avvisi. Il
 * campionamento resta li' perche' e' solo memoria: legge l'istantanea che
 * TripUpdatesService ha gia' scaricato (mai una nuova: un download in piu'
 * raddoppierebbe i poll verso romamobilita.it, che ha un filtro anti-bot). Le
 * scritture no: con un Postgres lento o spento occuperebbero thread del pool, e
 * i poll che tengono viva la mappa aspetterebbero.
 *
 * IL BACKEND NON DIPENDE DAL DATABASE. Parte anche se Postgres non risponde
 * (initialization-fail-timeout=-1), la migrazione Flyway si tenta a ogni giro
 * finche' non riesce, e i passaggi aspettano in una coda limitata. Oltre il
 * limite si perdono i piu' vecchi, con un avviso nei log: mappa e arrivi valgono
 * piu' dello storico.
 *
 * MANUTENZIONE, ogni ora: partizioni del mese in corso e del successivo,
 * aggregato dei due giorni precedenti in puntualita_giornaliera (idempotente),
 * eliminazione di passaggi e corse oltre i 90 giorni.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PunctualityService {

    static final ZoneId ROMA = ZoneId.of("Europe/Rome");
    /** Quanti giorni di passaggi si tengono: e' anche il limite degli intervalli che l'API accetta. */
    public static final int GIORNI_CONSERVATI = 90;

    /** In anticipo: sotto -1 minuto. Chi arriva in orario il bus l'ha gia' perso. */
    static final int ANTICIPO_SOTTO_SEC = -60;
    /** In ritardo: da 3 minuti. Sotto, e' la tolleranza di un servizio urbano. */
    static final int RITARDO_DA_SEC = 180;
    /** Forte ritardo: da 10 minuti. */
    static final int FORTE_RITARDO_DA_SEC = 600;

    /**
     * Circa 90 minuti di passaggi (650 al minuto a meta' giornata, settembre 2026)
     * e una dozzina di MB. In produzione l'heap e' di 768 MB con uscita al primo
     * OutOfMemoryError: un Postgres spento a lungo deve costare i passaggi piu'
     * vecchi, non il backend.
     */
    static final int CODA_MASSIMA = 60_000;
    static final int LOTTO = 5_000;
    private static final long INTERVALLO_AVVISI_MILLIS = 10 * 60 * 1000L;
    private static final Pattern PARTIZIONE = Pattern.compile("passaggi_(\\d{4})_(\\d{2})");

    private final TripUpdatesService tripUpdatesService;
    private final GtfsIndexService indexService;
    private final JdbcTemplate jdbc;
    private final DataSource dataSource;

    @Value("${punctuality.enabled:true}")
    private boolean enabled;

    private final PunctualityCollector collector = new PunctualityCollector();
    private final ConcurrentLinkedQueue<PunctualityCollector.Passaggio> coda = new ConcurrentLinkedQueue<>();
    private final AtomicInteger inCoda = new AtomicInteger();
    private final AtomicLong scartati = new AtomicLong();
    private final Set<YearMonth> partizioniPronte = ConcurrentHashMap.newKeySet();

    private ScheduledExecutorService scrittore;
    private volatile boolean schemaPronto;
    private volatile long ultimoAvvisoMillis;
    // Solo dal thread di scrittura.
    private long giri;
    private long ultimaManutenzioneMillis;
    private LocalDate ultimoGiornoAggregato;

    /** Passaggi di un giorno e di un'ora, gia' divisi per categoria. */
    record RigaOraria(LocalDate giorno, int ora, int campioni, int anticipo, int puntuale, int ritardo, int forteRitardo) {}

    @PostConstruct
    void avvia() {
        if (!enabled) {
            log.info("[Puntualita] disattivata (punctuality.enabled=false)");
            return;
        }
        scrittore = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "puntualita-db");
            t.setDaemon(true);
            return t;
        });
        scrittore.scheduleWithFixedDelay(this::scrivi, 20, 15, TimeUnit.SECONDS);
    }

    @PreDestroy
    void chiudi() {
        if (scrittore == null) {
            return;
        }
        scrittore.shutdown();
        try {
            scrittore.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Ultimo salvataggio: quello che e' in coda andrebbe perso col processo.
        if (schemaPronto) {
            try {
                scriviPassaggi(Integer.MAX_VALUE);
                scriviCorse();
            } catch (RuntimeException e) {
                log.warn("[Puntualita] salvataggio finale fallito: {}", e.toString());
            }
        }
    }

    @Scheduled(
            fixedDelayString = "${punctuality.sample-millis:15000}",
            initialDelayString = "${punctuality.sample-millis:15000}"
    )
    public void campiona() {
        if (!enabled) {
            return;
        }
        List<TripUpdateDTO> righe = tripUpdatesService.snapshot();
        if (righe == null || righe.isEmpty()) {
            return;
        }
        for (PunctualityCollector.Passaggio p : collector.osserva(righe, Instant.now())) {
            if (inCoda.incrementAndGet() > CODA_MASSIMA && coda.poll() != null) {
                inCoda.decrementAndGet();
                scartati.incrementAndGet();
            }
            coda.add(p);
        }
    }

    /**
     * Puntualita' della linea fra due date comprese. Vuoto se lo storico non e'
     * disponibile: database spento, o funzione disattivata.
     */
    public Optional<ApiLinePunctualityDTO> riepilogo(String linea, LocalDate dal, LocalDate al) {
        if (!enabled || !schemaPronto) {
            return Optional.empty();
        }
        try {
            List<RigaOraria> righe = jdbc.query("""
                    SELECT giorno,
                           extract(hour FROM osservato_il AT TIME ZONE 'Europe/Rome')::int AS ora,
                           count(*) AS campioni,
                           count(*) FILTER (WHERE ritardo_sec < ?) AS anticipo,
                           count(*) FILTER (WHERE ritardo_sec >= ? AND ritardo_sec < ?) AS puntuale,
                           count(*) FILTER (WHERE ritardo_sec >= ? AND ritardo_sec < ?) AS ritardo,
                           count(*) FILTER (WHERE ritardo_sec >= ?) AS forte_ritardo
                    FROM passaggi
                    WHERE linea = ? AND giorno BETWEEN ? AND ?
                    GROUP BY 1, 2
                    """,
                    (rs, i) -> new RigaOraria(
                            rs.getDate("giorno").toLocalDate(), rs.getInt("ora"), rs.getInt("campioni"),
                            rs.getInt("anticipo"), rs.getInt("puntuale"), rs.getInt("ritardo"), rs.getInt("forte_ritardo")),
                    ANTICIPO_SOTTO_SEC, ANTICIPO_SOTTO_SEC, RITARDO_DA_SEC, RITARDO_DA_SEC, FORTE_RITARDO_DA_SEC,
                    FORTE_RITARDO_DA_SEC, linea, Date.valueOf(dal), Date.valueOf(al));
            return Optional.of(componi(linea, dal, al, righe, Instant.now()));
        } catch (DataAccessException e) {
            avvisa("lettura della puntualita' della linea " + linea + " fallita", e);
            return Optional.empty();
        }
    }

    /**
     * Dalle righe per giorno e ora al riepilogo: il totale di tutti i giorni, e gli
     * stessi passaggi divisi per tipo di giorno (ServiceDayType). Senza database, per
     * poterla provare.
     */
    static ApiLinePunctualityDTO componi(String linea, LocalDate dal, LocalDate al, List<RigaOraria> righe,
                                         Instant adesso) {
        int[][] totale = new int[24][5];
        Set<LocalDate> giorni = new HashSet<>();
        Map<String, int[][]> perTipo = new HashMap<>();
        Map<String, Set<LocalDate>> giorniPerTipo = new HashMap<>();
        for (RigaOraria r : righe) {
            if (r.ora() < 0 || r.ora() > 23) {
                continue;
            }
            String tipo = ServiceDayType.di(r.giorno());
            int[] valori = {r.campioni(), r.anticipo(), r.puntuale(), r.ritardo(), r.forteRitardo()};
            somma(totale[r.ora()], valori);
            somma(perTipo.computeIfAbsent(tipo, k -> new int[24][5])[r.ora()], valori);
            giorni.add(r.giorno());
            giorniPerTipo.computeIfAbsent(tipo, k -> new HashSet<>()).add(r.giorno());
        }

        List<ApiLinePunctualityDTO.DayType> tipiOut = new ArrayList<>();
        for (String tipo : ServiceDayType.TUTTI) {
            List<ApiLinePunctualityDTO.Hour> ore = ore(perTipo.getOrDefault(tipo, new int[24][5]));
            tipiOut.add(new ApiLinePunctualityDTO.DayType(
                    tipo, giorniPerTipo.getOrDefault(tipo, Set.of()).size(), campioni(ore), ore));
        }
        List<ApiLinePunctualityDTO.Hour> oreTotali = ore(totale);
        return new ApiLinePunctualityDTO(
                linea, dal.toString(), al.toString(), giorni.size(), campioni(oreTotali),
                new ApiLinePunctualityDTO.Thresholds(
                        ANTICIPO_SOTTO_SEC / 60.0, RITARDO_DA_SEC / 60.0, FORTE_RITARDO_DA_SEC / 60.0),
                oreTotali, tipiOut, adesso);
    }

    private static List<ApiLinePunctualityDTO.Hour> ore(int[][] valori) {
        List<ApiLinePunctualityDTO.Hour> out = new ArrayList<>();
        for (int ora = 0; ora < 24; ora++) {
            int[] v = valori[ora];
            if (v[0] > 0) {
                out.add(new ApiLinePunctualityDTO.Hour(ora, v[0], v[1], v[2], v[3], v[4]));
            }
        }
        return out;
    }

    private static int campioni(List<ApiLinePunctualityDTO.Hour> ore) {
        return ore.stream().mapToInt(ApiLinePunctualityDTO.Hour::samples).sum();
    }

    private static void somma(int[] destinazione, int[] valori) {
        for (int i = 0; i < valori.length; i++) {
            destinazione[i] += valori[i];
        }
    }

    // ---- thread di scrittura -------------------------------------------------

    private void scrivi() {
        try {
            if (!assicuraSchema()) {
                return;
            }
            scriviPassaggi(LOTTO * 4);
            if (++giri % 4 == 0) {
                scriviCorse();
            }
            if (System.currentTimeMillis() - ultimaManutenzioneMillis > 60 * 60 * 1000L) {
                manutenzione();
                ultimaManutenzioneMillis = System.currentTimeMillis();
            }
            long persi = scartati.getAndSet(0);
            if (persi > 0) {
                log.warn("[Puntualita] coda piena: {} passaggi scartati", persi);
            }
        } catch (RuntimeException e) {
            avvisa("scrittura fallita", e);
        }
    }

    private boolean assicuraSchema() {
        if (schemaPronto) {
            return true;
        }
        try {
            Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
            schemaPronto = true;
            log.info("[Puntualita] database pronto");
            return true;
        } catch (RuntimeException e) {
            avvisa("database non raggiungibile, riprovo fra 15 secondi", e);
            return false;
        }
    }

    private void scriviPassaggi(int massimo) {
        List<PunctualityCollector.Passaggio> lotto = new ArrayList<>();
        PunctualityCollector.Passaggio p;
        while (lotto.size() < massimo && (p = coda.poll()) != null) {
            inCoda.decrementAndGet();
            lotto.add(p);
        }
        if (lotto.isEmpty()) {
            return;
        }
        try {
            lotto.stream().map(x -> YearMonth.from(giorno(x.primaVistaCorsa()))).distinct()
                    .forEach(this::assicuraPartizione);
            for (int da = 0; da < lotto.size(); da += LOTTO) {
                List<PunctualityCollector.Passaggio> parte = lotto.subList(da, Math.min(lotto.size(), da + LOTTO));
                jdbc.batchUpdate("""
                        INSERT INTO passaggi (giorno, osservato_il, linea, route_id, corsa, fermata, veicolo, ritardo_sec)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, parte, parte.size(), (ps, x) -> {
                    ps.setDate(1, Date.valueOf(giorno(x.primaVistaCorsa())));
                    ps.setObject(2, OffsetDateTime.ofInstant(x.quando(), ZoneOffset.UTC));
                    ps.setString(3, linea(x.routeId()));
                    ps.setString(4, x.routeId());
                    ps.setString(5, x.corsa());
                    ps.setString(6, x.fermataId());
                    ps.setString(7, x.veicolo());
                    ps.setInt(8, (int) Math.round(x.ritardoMin() * 60));
                });
            }
        } catch (DataAccessException e) {
            // Tornano in coda: il prossimo giro riprova. Il limite della coda vale comunque.
            for (PunctualityCollector.Passaggio x : lotto) {
                if (inCoda.incrementAndGet() > CODA_MASSIMA) {
                    inCoda.decrementAndGet();
                    scartati.incrementAndGet();
                    continue;
                }
                coda.add(x);
            }
            throw e;
        }
    }

    private void scriviCorse() {
        List<PunctualityCollector.CorsaOsservata> corse = collector.estraiCorse();
        if (corse.isEmpty()) {
            return;
        }
        // Se fallisce, i contatori dei passaggi di questo giro si perdono: sono un
        // riassunto, i passaggi veri restano nella loro tabella.
        jdbc.batchUpdate("""
                INSERT INTO corse_osservate (giorno, corsa, linea, route_id, veicolo, prima_vista, ultima_vista,
                                             fermate_registrate, ultimo_ritardo_sec)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (giorno, corsa) DO UPDATE SET
                    veicolo = COALESCE(EXCLUDED.veicolo, corse_osservate.veicolo),
                    prima_vista = LEAST(corse_osservate.prima_vista, EXCLUDED.prima_vista),
                    ultima_vista = GREATEST(corse_osservate.ultima_vista, EXCLUDED.ultima_vista),
                    fermate_registrate = corse_osservate.fermate_registrate + EXCLUDED.fermate_registrate,
                    ultimo_ritardo_sec = COALESCE(EXCLUDED.ultimo_ritardo_sec, corse_osservate.ultimo_ritardo_sec)
                """, corse, LOTTO, (ps, c) -> {
            ps.setDate(1, Date.valueOf(giorno(c.primaVista())));
            ps.setString(2, c.corsa());
            ps.setString(3, linea(c.routeId()));
            ps.setString(4, c.routeId());
            ps.setString(5, c.veicolo());
            ps.setObject(6, OffsetDateTime.ofInstant(c.primaVista(), ZoneOffset.UTC));
            ps.setObject(7, OffsetDateTime.ofInstant(c.ultimaVista(), ZoneOffset.UTC));
            ps.setInt(8, c.fermateNuove());
            if (c.ultimoRitardoMin() == null) {
                ps.setNull(9, java.sql.Types.INTEGER);
            } else {
                ps.setInt(9, (int) Math.round(c.ultimoRitardoMin() * 60));
            }
        });
    }

    private void manutenzione() {
        YearMonth mese = YearMonth.from(oggi());
        assicuraPartizione(mese);
        assicuraPartizione(mese.plusMonths(1));

        LocalDate ieri = oggi().minusDays(1);
        if (ultimoGiornoAggregato == null || ultimoGiornoAggregato.isBefore(ieri)) {
            int righe = jdbc.update("""
                    INSERT INTO puntualita_giornaliera (giorno, linea, ora, campioni, anticipo, puntuale, ritardo, forte_ritardo)
                    SELECT giorno, linea, extract(hour FROM osservato_il AT TIME ZONE 'Europe/Rome')::smallint,
                           count(*),
                           count(*) FILTER (WHERE ritardo_sec < ?),
                           count(*) FILTER (WHERE ritardo_sec >= ? AND ritardo_sec < ?),
                           count(*) FILTER (WHERE ritardo_sec >= ? AND ritardo_sec < ?),
                           count(*) FILTER (WHERE ritardo_sec >= ?)
                    FROM passaggi
                    WHERE giorno BETWEEN ? AND ?
                    GROUP BY 1, 2, 3
                    ON CONFLICT (giorno, linea, ora) DO UPDATE SET
                        campioni = EXCLUDED.campioni, anticipo = EXCLUDED.anticipo, puntuale = EXCLUDED.puntuale,
                        ritardo = EXCLUDED.ritardo, forte_ritardo = EXCLUDED.forte_ritardo
                    """,
                    ANTICIPO_SOTTO_SEC, ANTICIPO_SOTTO_SEC, RITARDO_DA_SEC, RITARDO_DA_SEC, FORTE_RITARDO_DA_SEC,
                    FORTE_RITARDO_DA_SEC, Date.valueOf(ieri.minusDays(1)), Date.valueOf(ieri));
            ultimoGiornoAggregato = ieri;
            log.info("[Puntualita] aggregati {} e {}: {} righe", ieri.minusDays(1), ieri, righe);
        }

        LocalDate limite = oggi().minusDays(GIORNI_CONSERVATI);
        for (String nome : jdbc.queryForList("""
                SELECT c.relname FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname = 'passaggi'
                """, String.class)) {
            Matcher m = PARTIZIONE.matcher(nome);
            if (!m.matches()) {
                continue;
            }
            YearMonth ym = YearMonth.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
            // Si stacca solo un mese finito per intero prima del limite.
            if (!ym.plusMonths(1).atDay(1).isAfter(limite)) {
                jdbc.execute("DROP TABLE IF EXISTS " + nome);
                partizioniPronte.remove(ym);
                log.info("[Puntualita] eliminata la partizione {}", nome);
            }
        }
        jdbc.update("DELETE FROM corse_osservate WHERE giorno < ?", Date.valueOf(limite));
    }

    private void assicuraPartizione(YearMonth mese) {
        if (partizioniPronte.contains(mese)) {
            return;
        }
        // Nome e date calcolati qui, mai presi da un input: la concatenazione e' sicura.
        jdbc.execute("CREATE TABLE IF NOT EXISTS passaggi_%d_%02d PARTITION OF passaggi FOR VALUES FROM ('%s') TO ('%s')"
                .formatted(mese.getYear(), mese.getMonthValue(), mese.atDay(1), mese.plusMonths(1).atDay(1)));
        partizioniPronte.add(mese);
    }

    private String linea(String routeId) {
        String linea = indexService.publicLineByRouteId(routeId);
        return linea == null || linea.isBlank() ? routeId : linea;
    }

    /** Giorno di servizio di una corsa: la data, a Roma, in cui e' comparsa nel feed. */
    private static LocalDate giorno(Instant primaVista) {
        return primaVista.atZone(ROMA).toLocalDate();
    }

    public static LocalDate oggi() {
        return LocalDate.now(ROMA);
    }

    /** Un database spento produrrebbe un avviso ogni 15 secondi: al massimo uno ogni dieci minuti. */
    private void avvisa(String cosa, Exception e) {
        long adesso = System.currentTimeMillis();
        if (adesso - ultimoAvvisoMillis > INTERVALLO_AVVISI_MILLIS) {
            ultimoAvvisoMillis = adesso;
            log.warn("[Puntualita] {}: {}", cosa, e.toString());
        } else {
            log.debug("[Puntualita] {}: {}", cosa, e.toString());
        }
    }
}
