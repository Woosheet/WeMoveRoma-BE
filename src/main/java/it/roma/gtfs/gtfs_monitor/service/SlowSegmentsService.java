package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiSlowSegmentsDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Array;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dove i bus perdono tempo: l'aggregato giornaliero dei tratti fra due fermate.
 *
 * COSA SI MISURA. Per ogni corsa, fra una fermata e la successiva, di quanto e'
 * cresciuto il ritardo rispetto all'orario. Sommato su tutte le corse di un
 * giorno dice dove si perde tempo; diviso per le corse, quanto perde ciascun bus.
 * Non e' solo traffico: pesano anche semafori, salite e deviazioni.
 *
 * Verificato sullo storico di produzione (settembre 2026): solo l'8% dei tratti
 * ha lo stesso ritardo alle due fermate, quindi il feed lo misura fermata per
 * fermata e non lo trascina. In testa alla classifica escono i tratti che
 * arrivano a Piazza Venezia, dove ci sono i cantieri della metro C.
 *
 * TRE ESCLUSIONI, tutte nella query:
 *  - la prima fermata osservata di ogni corsa: una partenza in ritardo dal
 *    capolinea non e' tempo perso lungo la strada;
 *  - i tratti in cui fra le due osservazioni passano 20 minuti o piu': il mezzo
 *    e' sparito dal feed, e la differenza di ritardo non riguarda quel tratto;
 *  - le righe con meno di 3 corse nella fascia: rumore, e righe da conservare.
 *
 * L'aggregato gira sul thread del database di PunctualityService, dentro la sua
 * manutenzione oraria: stesso schema (lo crea la sua migrazione Flyway), stessa
 * regola di non tenere occupati i thread dei poll.
 *
 * LA LETTURA (/api/v1/catalog/slow-segments) somma i giorni di un tipo e una
 * fascia, e aggiunge nomi delle fermate e strada percorsa, dal GTFS. Il
 * risultato resta in memoria finche' non cambiano l'aggregato o il GTFS: e' lo
 * stesso per tutti, e cambia una volta al giorno.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SlowSegmentsService {

    /** Fasce orarie, sull'ora di arrivo al secondo capo del tratto. */
    public static final String MATTINA = "mattina";
    public static final String SERA = "sera";
    public static final String RESTO = "resto";
    public static final List<String> FASCE = List.of(MATTINA, RESTO, SERA);
    /** Tutte le fasce insieme: solo in lettura. */
    public static final String TUTTE = "tutte";

    /** Sotto questa soglia di corse per giorno e fascia la riga non si salva. */
    static final int CORSE_MINIME = 3;
    /** Oltre questo intervallo fra due osservazioni il tratto non si conta. */
    static final int INTERVALLO_MASSIMO_MIN = 20;

    /** In lettura, sotto questo numero di corse sull'intero periodo il tratto non si mostra. */
    static final int CORSE_MINIME_PERIODO = 10;
    private static final int RISPOSTE_IN_MEMORIA = 48;

    private final JdbcTemplate jdbc;
    private final GtfsIndexService gtfs;
    /** Campo e non costante solo perche' i test possano abbassarla. */
    int corseMinimePeriodo = CORSE_MINIME_PERIODO;

    @Value("${punctuality.enabled:true}")
    private boolean enabled;

    /** Cresce a ogni aggregazione: invalida le risposte in memoria. */
    private final AtomicLong versioneAggregato = new AtomicLong();
    private final Map<String, Risposta> risposte = new ConcurrentHashMap<>();
    /** Strada fra due fermate, per la versione del GTFS in `versioneGeometrie`. */
    private final Map<String, List<double[]>> geometrie = new ConcurrentHashMap<>();
    private volatile Instant versioneGeometrie = Instant.EPOCH;

    private record Risposta(long aggregato, Instant gtfs, ApiSlowSegmentsDTO dto) {}

    /** Un tratto sommato sul periodo richiesto. */
    record Tratto(String da, String a, long corse, long secPersi, double secP90, List<String> linee) {}

    /** I tratti di un periodo, e in quanti giorni di quel periodo c'erano dati. */
    record Periodo(int giorni, List<Tratto> tratti) {}

    /**
     * Circa 30 mila righe al giorno (stima dallo storico di settembre 2026),
     * intorno ai 4 MB: tenerle per sempre costerebbe 1,5 GB l'anno. 400 giorni
     * bastano a confrontare lo stesso mese dell'anno prima.
     */
    @Value("${punctuality.segments-retention-days:400}")
    private int giorniConservati;

    /**
     * Ricalcola un giorno di servizio. Idempotente: si puo' ripetere, e lo si
     * ripete il giorno dopo per contare le corse notturne finite dopo mezzanotte.
     *
     * @return righe scritte
     */
    public int aggregaGiorno(LocalDate giorno) {
        int righe = jdbc.update("""
                INSERT INTO tratti_giornalieri (giorno, da, a, fascia, corse, sec_persi, sec_persi_p90, linee)
                SELECT giorno, da, a, fascia,
                       count(*),
                       sum(delta),
                       round(percentile_cont(0.9) WITHIN GROUP (ORDER BY delta))::int,
                       array_agg(DISTINCT linea ORDER BY linea)
                FROM (
                    SELECT giorno, linea, fermata AS a,
                           lag(fermata) OVER w AS da,
                           ritardo_sec - lag(ritardo_sec) OVER w AS delta,
                           osservato_il - lag(osservato_il) OVER w AS intervallo,
                           row_number() OVER w AS n,
                           CASE
                               WHEN extract(hour FROM osservato_il AT TIME ZONE 'Europe/Rome') BETWEEN 7 AND 9 THEN ?
                               WHEN extract(hour FROM osservato_il AT TIME ZONE 'Europe/Rome') BETWEEN 16 AND 19 THEN ?
                               ELSE ?
                           END AS fascia
                    FROM passaggi
                    WHERE giorno = ?
                    WINDOW w AS (PARTITION BY corsa ORDER BY osservato_il)
                ) t
                -- n > 2: il tratto che parte dalla prima fermata osservata e' la partenza dal capolinea.
                WHERE n > 2 AND da IS NOT NULL AND da <> a AND intervallo < make_interval(mins => ?)
                GROUP BY giorno, da, a, fascia
                HAVING count(*) >= ?
                ON CONFLICT (giorno, da, a, fascia) DO UPDATE SET
                    corse = EXCLUDED.corse, sec_persi = EXCLUDED.sec_persi,
                    sec_persi_p90 = EXCLUDED.sec_persi_p90, linee = EXCLUDED.linee
                """,
                MATTINA, SERA, RESTO, Date.valueOf(giorno), INTERVALLO_MASSIMO_MIN, CORSE_MINIME);
        // Dopo la scrittura: prima, una lettura nel mezzo salverebbe i dati vecchi
        // con la versione nuova, e resterebbero in memoria fino al giorno dopo.
        versioneAggregato.incrementAndGet();
        return righe;
    }

    /**
     * Giorni con passaggi ma senza aggregato, fino a `finoA` compreso: al primo
     * avvio dopo il rilascio sono tutti quelli gia' nello storico. Si leggono da
     * corse_osservate, che ha una riga per corsa invece che per fermata.
     */
    public List<LocalDate> giorniMancanti(LocalDate finoA) {
        return jdbc.queryForList("""
                SELECT DISTINCT giorno FROM corse_osservate WHERE giorno <= ?
                EXCEPT
                SELECT DISTINCT giorno FROM tratti_giornalieri
                ORDER BY 1
                """, Date.class, Date.valueOf(finoA))
                .stream().map(Date::toLocalDate).toList();
    }

    /** Giorni di aggregato conservati: anche il limite delle date che la lettura accetta. */
    public int giorniConservati() {
        return giorniConservati;
    }

    /** Toglie i giorni oltre la conservazione. */
    public int eliminaVecchi(LocalDate oggi) {
        return jdbc.update("DELETE FROM tratti_giornalieri WHERE giorno < ?",
                Date.valueOf(oggi.minusDays(giorniConservati)));
    }

    // ---- lettura ---------------------------------------------------------------

    /**
     * I tratti in cui si perde tempo, per tipo di giorno (ServiceDayType) e fascia
     * (una di FASCE, o TUTTE), fra due date comprese. Con `linea`, solo i tratti
     * su cui passa: i numeri restano quelli di tutte le corse del tratto, perche'
     * l'aggregato non divide per linea — e sulla stessa strada le linee perdono
     * tempo insieme. Vuoto se lo storico non e' disponibile: database spento, o
     * funzione disattivata.
     */
    public Optional<ApiSlowSegmentsDTO> tratti(String tipo, String fascia, LocalDate dal, LocalDate al, String linea) {
        if (!enabled) {
            return Optional.empty();
        }
        String chiave = tipo + "|" + fascia + "|" + dal + "|" + al + "|" + (linea == null ? "" : linea);
        long aggregato = versioneAggregato.get();
        Instant versioneGtfs = gtfs.dataVersion();
        Risposta pronta = risposte.get(chiave);
        if (pronta != null && pronta.aggregato() == aggregato && pronta.gtfs().equals(versioneGtfs)) {
            return Optional.of(pronta.dto());
        }

        List<LocalDate> giorni = dal.datesUntil(al.plusDays(1)).filter(g -> ServiceDayType.di(g).equals(tipo)).toList();
        Periodo periodo;
        try {
            periodo = leggi(giorni, fascia, linea);
        } catch (DataAccessException e) {
            log.warn("[Tratti] lettura fallita: {}", e.toString());
            return Optional.empty();
        }

        if (!versioneGtfs.equals(versioneGeometrie)) {
            geometrie.clear();
            versioneGeometrie = versioneGtfs;
        }
        LocalDate oggi = PunctualityService.oggi();
        List<ApiSlowSegmentsDTO.Segment> segmenti = new ArrayList<>();
        for (Tratto t : periodo.tratti()) {
            GtfsIndexService.Stop da = gtfs.stopByIdOrNull(t.da());
            GtfsIndexService.Stop a = gtfs.stopByIdOrNull(t.a());
            if (da == null || a == null || da.lat() == null || a.lat() == null) {
                continue; // fermata non piu' nel GTFS: non si saprebbe dove disegnarla
            }
            List<double[]> strada = geometrie.computeIfAbsent(t.da() + "|" + t.a(), k -> {
                List<double[]> s = gtfs.shapeBetweenStops(t.da(), t.a(), oggi);
                return s.isEmpty()
                        ? List.of(new double[]{da.lon(), da.lat()}, new double[]{a.lon(), a.lat()})
                        : s;
            });
            double lunghezza = 0;
            for (int i = 1; i < strada.size(); i++) {
                lunghezza += GtfsIndexService.metri(strada.get(i - 1), strada.get(i));
            }
            segmenti.add(new ApiSlowSegmentsDTO.Segment(
                    t.da(), da.name(), t.a(), a.name(),
                    t.linee().stream().sorted(PER_LINEA).toList(),
                    decimale((double) t.corse() / periodo.giorni()),
                    decimale(t.secPersi() / 60.0 / periodo.giorni()),
                    decimale(t.secPersi() / 60.0 / t.corse()),
                    decimale(t.secP90() / 60.0),
                    (int) Math.round(lunghezza),
                    strada));
        }

        ApiSlowSegmentsDTO dto = new ApiSlowSegmentsDTO(tipo, fascia, linea, dal.toString(), al.toString(),
                periodo.giorni(), segmenti, Instant.now());
        if (risposte.size() >= RISPOSTE_IN_MEMORIA) {
            risposte.clear();
        }
        risposte.put(chiave, new Risposta(aggregato, versioneGtfs, dto));
        return Optional.of(dto);
    }

    /**
     * Somma l'aggregato sui giorni dati: solo i tratti in cui, contando tutte le
     * corse, si perde tempo, dal peggiore. Il 90° percentile non si puo' sommare:
     * e' la media dei valori giornalieri pesata sulle corse.
     */
    Periodo leggi(List<LocalDate> giorni, String fascia, String linea) {
        if (giorni.isEmpty()) {
            return new Periodo(0, List.of());
        }
        Date[] date = giorni.stream().map(Date::valueOf).toArray(Date[]::new);
        Integer osservati = jdbc.query(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "SELECT count(DISTINCT giorno) FROM tratti_giornalieri WHERE giorno = ANY(?)");
            ps.setArray(1, con.createArrayOf("date", date));
            return ps;
        }, rs -> rs.next() ? rs.getInt(1) : 0);
        if (osservati == null || osservati == 0) {
            return new Periodo(0, List.of());
        }
        List<Tratto> tratti = jdbc.query(con -> {
            PreparedStatement ps = con.prepareStatement("""
                    WITH r AS (
                        SELECT * FROM tratti_giornalieri
                        WHERE giorno = ANY(?) AND (? = 'tutte' OR fascia = ?)
                          AND (?::text IS NULL OR ? = ANY(linee))
                    ), somme AS (
                        SELECT da, a, sum(corse) AS corse, sum(sec_persi) AS sec_persi,
                               sum(sec_persi_p90::bigint * corse)::float8 / sum(corse) AS sec_p90
                        FROM r
                        GROUP BY da, a
                        HAVING sum(sec_persi) > 0 AND sum(corse) >= ?
                    ), linee AS (
                        SELECT da, a, array_agg(DISTINCT l) AS linee
                        FROM r CROSS JOIN LATERAL unnest(r.linee) AS l
                        GROUP BY da, a
                    )
                    SELECT s.da, s.a, s.corse, s.sec_persi, s.sec_p90, l.linee
                    FROM somme s JOIN linee l USING (da, a)
                    ORDER BY s.sec_persi DESC
                    """);
            ps.setArray(1, con.createArrayOf("date", date));
            ps.setString(2, fascia);
            ps.setString(3, fascia);
            ps.setString(4, linea);
            ps.setString(5, linea);
            ps.setInt(6, corseMinimePeriodo);
            return ps;
        }, (rs, i) -> {
            Array linee = rs.getArray("linee");
            return new Tratto(rs.getString("da"), rs.getString("a"), rs.getLong("corse"), rs.getLong("sec_persi"),
                    rs.getDouble("sec_p90"), Arrays.asList((String[]) linee.getArray()));
        });
        return new Periodo(osservati, tratti);
    }

    private static double decimale(double x) {
        return Math.round(x * 10) / 10.0;
    }

    /** 8, 64, 492, H, n8: prima i numeri in ordine numerico, poi il resto. */
    private static final Comparator<String> PER_LINEA = Comparator
            .comparing((String l) -> !l.matches("\\d+.*"))
            .thenComparingInt(l -> {
                String cifre = l.replaceAll("^(\\d*).*", "$1");
                return cifre.isEmpty() ? 0 : Integer.parseInt(cifre.length() > 6 ? cifre.substring(0, 6) : cifre);
            })
            .thenComparing(Comparator.naturalOrder());
}
