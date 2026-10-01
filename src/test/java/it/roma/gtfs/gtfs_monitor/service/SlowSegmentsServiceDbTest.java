package it.roma.gtfs.gtfs_monitor.service;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * L'aggregato dei tratti su un Postgres vero: la query usa funzioni finestra e
 * percentile_cont, che solo Postgres ha. Opt-in, come il test sul feed reale:
 *
 *   ./mvnw test -Dtest=SlowSegmentsServiceDbTest -Dstorico.db=jdbc:postgresql://127.0.0.1:5433/wemoveroma_verifica
 *
 * (utente wemoveroma, password vuota: -Dstorico.user / -Dstorico.password per
 * cambiarli). Lavora in uno schema suo, creato dalle migrazioni e poi eliminato.
 */
class SlowSegmentsServiceDbTest {

    private static final ZoneId ROMA = ZoneId.of("Europe/Rome");
    private static final LocalDate GIORNO = LocalDate.of(2026, 7, 21);

    private String url;
    private String schema;
    private JdbcTemplate jdbc;
    private SlowSegmentsService service;

    @BeforeEach
    void setUp() {
        url = System.getProperty("storico.db");
        Assumptions.assumeTrue(url != null, "Abilitare con -Dstorico.db=jdbc:postgresql://...");
        String utente = System.getProperty("storico.user", "wemoveroma");
        String password = System.getProperty("storico.password", "");
        schema = "prova_tratti_" + UUID.randomUUID().toString().substring(0, 8);

        Flyway.configure().dataSource(url, utente, password).schemas(schema)
                .locations("classpath:db/migration").load().migrate();
        DriverManagerDataSource ds = new DriverManagerDataSource(
                url + (url.contains("?") ? "&" : "?") + "currentSchema=" + schema, utente, password);
        jdbc = new JdbcTemplate(ds);
        jdbc.execute("CREATE TABLE passaggi_2026_07 PARTITION OF passaggi FOR VALUES FROM ('2026-07-01') TO ('2026-08-01')");

        service = new SlowSegmentsService(jdbc, null);
        ReflectionTestUtils.setField(service, "giorniConservati", 400);

        // Mattina, tratto S2 -> S3: tre corse di due linee perdono 120, 180 e 60 secondi.
        // S1 -> S2 non si conta (partenza dal capolinea), S3 -> S4 recupera.
        corsa("T1", "64", "08:00", "S1", 0, "S2", 60, "S3", 180, "S4", 150);
        corsa("T2", "64", "08:10", "S1", 30, "S2", 90, "S3", 270, "S4", 270);
        corsa("T3", "85", "08:20", "S1", 0, "S2", 0, "S3", 60, "S4", 30);
        // Sera, stesso tratto: tre corse della 85, 60 secondi ciascuna.
        corsa("T4", "85", "17:00", "S1", 0, "S2", 0, "S3", 60);
        corsa("T5", "85", "17:10", "S1", 0, "S2", 0, "S3", 60);
        corsa("T6", "85", "17:20", "S1", 0, "S2", 0, "S3", 60);
        // Solo due corse sul tratto S7 -> S8: sotto la soglia, non si salva.
        corsa("T7", "990", "12:00", "S6", 0, "S7", 0, "S8", 300);
        corsa("T8", "990", "12:10", "S6", 0, "S7", 0, "S8", 300);
    }

    @AfterEach
    void tearDown() {
        if (jdbc != null) {
            jdbc.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }

    @Test
    void sommaIlTempoPersoPerTrattoEFascia() {
        service.aggregaGiorno(GIORNO);

        Map<String, Object> mattina = riga("S2", "S3", "mattina");
        assertEquals(3, mattina.get("corse"));
        assertEquals(360L, mattina.get("sec_persi"));
        // 90° percentile di 60, 120, 180 con interpolazione lineare: 168.
        assertEquals(168, mattina.get("sec_persi_p90"));
        assertEquals("{64,85}", mattina.get("linee").toString());

        Map<String, Object> recupero = riga("S3", "S4", "mattina");
        assertEquals(-60L, recupero.get("sec_persi"));

        Map<String, Object> sera = riga("S2", "S3", "sera");
        assertEquals(3, sera.get("corse"));
        assertEquals(180L, sera.get("sec_persi"));
        assertEquals("{85}", sera.get("linee").toString());
    }

    @Test
    void escludeCapolineaBuchiNelFeedETrattiConPocheCorse() {
        // Una corsa sparita dal feed per 25 minuti fra S2 e S3, tre volte.
        corsaConBuco("T9");
        corsaConBuco("T10");
        corsaConBuco("T11");

        service.aggregaGiorno(GIORNO);

        List<String> tratti = jdbc.queryForList(
                "SELECT da || '>' || a || ' ' || fascia FROM tratti_giornalieri ORDER BY 1", String.class);
        assertEquals(List.of("S2>S3 mattina", "S2>S3 sera", "S3>S4 mattina"), tratti);
    }

    @Test
    void ripetereLoStessoGiornoNonDuplica() {
        service.aggregaGiorno(GIORNO);
        service.aggregaGiorno(GIORNO);

        assertEquals(3, jdbc.queryForObject("SELECT count(*) FROM tratti_giornalieri", Integer.class));
        assertEquals(360L, riga("S2", "S3", "mattina").get("sec_persi"));
    }

    @Test
    void trovaIGiorniDaRecuperareEToglieQuelliVecchi() {
        jdbc.update("INSERT INTO corse_osservate (giorno, corsa, linea, route_id, prima_vista, ultima_vista) "
                + "VALUES (?, 'T1', '64', '64', now(), now()), (?, 'X1', '64', '64', now(), now())",
                Date.valueOf(GIORNO), Date.valueOf(GIORNO.plusDays(1)));

        assertEquals(List.of(GIORNO, GIORNO.plusDays(1)), service.giorniMancanti(GIORNO.plusDays(1)));
        service.aggregaGiorno(GIORNO);
        assertEquals(List.of(GIORNO.plusDays(1)), service.giorniMancanti(GIORNO.plusDays(1)));
        assertEquals(List.of(), service.giorniMancanti(GIORNO));

        // 400 giorni dopo il 21 luglio 2026 quel giorno esce.
        assertEquals(0, service.eliminaVecchi(GIORNO.plusDays(400)));
        assertTrue(service.eliminaVecchi(GIORNO.plusDays(401)) > 0);
    }

    @Test
    void laLetturaSommaLeFasceESoloITrattiInCuiSiPerdeTempo() {
        service.aggregaGiorno(GIORNO);
        service.corseMinimePeriodo = 3;

        SlowSegmentsService.Periodo tutte = service.leggi(List.of(GIORNO), "tutte", null);
        assertEquals(1, tutte.giorni());
        // S3 -> S4 recupera (-60 s): fuori. S2 -> S3: mattina 360 + sera 180.
        assertEquals(1, tutte.tratti().size());
        SlowSegmentsService.Tratto t = tutte.tratti().getFirst();
        assertEquals("S2", t.da());
        assertEquals(6, t.corse());
        assertEquals(540, t.secPersi());
        // 90° percentile pesato sulle corse: (168 * 3 + 60 * 3) / 6.
        assertEquals(114.0, t.secP90(), 1e-9);
        assertEquals(List.of("64", "85"), t.linee().stream().sorted().toList());

        SlowSegmentsService.Tratto sera = service.leggi(List.of(GIORNO), "sera", null).tratti().getFirst();
        assertEquals(180, sera.secPersi());
        assertEquals(List.of("85"), sera.linee());

        // Un giorno senza aggregato: nessun giorno osservato, nessun tratto.
        SlowSegmentsService.Periodo vuoto = service.leggi(List.of(GIORNO.plusDays(1)), "tutte", null);
        assertEquals(0, vuoto.giorni());
        assertTrue(vuoto.tratti().isEmpty());
    }

    @Test
    void conLaLineaSoloITrattiSuCuiPassa() {
        service.aggregaGiorno(GIORNO);
        service.corseMinimePeriodo = 3;

        // La 85 passa su S2 -> S3 sia la mattina sia la sera: il tratto c'e', con
        // tutte le corse del tratto (anche quelle della 64), non solo le sue.
        SlowSegmentsService.Tratto t = service.leggi(List.of(GIORNO), "tutte", "85").tratti().getFirst();
        assertEquals("S2", t.da());
        assertEquals(6, t.corse());
        // La 990 ha solo S7 -> S8, sotto la soglia del giorno: nessun tratto.
        assertTrue(service.leggi(List.of(GIORNO), "tutte", "990").tratti().isEmpty());
        // La 64 la sera non passa su S2 -> S3.
        assertTrue(service.leggi(List.of(GIORNO), "sera", "64").tratti().isEmpty());
    }

    @Test
    void sottoLaSogliaDelPeriodoIlTrattoNonSiMostra() {
        service.aggregaGiorno(GIORNO);
        // 6 corse nella giornata, la soglia predefinita e' 10.
        assertTrue(service.leggi(List.of(GIORNO), "tutte", null).tratti().isEmpty());
    }

    // ---- dati ----------------------------------------------------------------

    /** Una corsa: fermate a tre minuti l'una dall'altra, con il ritardo a ciascuna. */
    private void corsa(String id, String linea, String partenza, Object... fermateERitardi) {
        LocalTime ora = LocalTime.parse(partenza);
        for (int i = 0; i < fermateERitardi.length; i += 2) {
            passaggio(id, linea, (String) fermateERitardi[i], (Integer) fermateERitardi[i + 1], ora.plusMinutes(3L * i / 2));
        }
    }

    private void corsaConBuco(String id) {
        passaggio(id, "64", "S1", 0, LocalTime.of(9, 0));
        passaggio(id, "64", "S5", 0, LocalTime.of(9, 3));
        passaggio(id, "64", "S9", 900, LocalTime.of(9, 28));
    }

    private void passaggio(String corsa, String linea, String fermata, int ritardoSec, LocalTime ora) {
        jdbc.update("""
                INSERT INTO passaggi (giorno, osservato_il, linea, route_id, corsa, fermata, veicolo, ritardo_sec)
                VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
                """,
                Date.valueOf(GIORNO),
                OffsetDateTime.ofInstant(GIORNO.atTime(ora).atZone(ROMA).toInstant(), ZoneOffset.UTC),
                linea, linea, corsa, fermata, ritardoSec);
    }

    private Map<String, Object> riga(String da, String a, String fascia) {
        return jdbc.queryForMap("SELECT * FROM tratti_giornalieri WHERE da = ? AND a = ? AND fascia = ?", da, a, fascia);
    }
}
