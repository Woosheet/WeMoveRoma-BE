package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Orari alla fermata: prima corsa, ultima e intervallo, palina per palina.
 *
 * I due casi che contano sono quelli che avevano rotto le pagine vere:
 * l'orario alla fermata di mezzo, che non e' quello del capolinea, e la linea
 * circolare che tocca due volte la stessa palina.
 */
class GtfsIndexServiceStopSchedulesTest {

    private static final LocalDate GIORNO = LocalDate.of(2026, 7, 21);

    @TempDir
    Path dataDir;

    private GtfsIndexService service;

    @BeforeEach
    void setUp() throws IOException {
        write("stops.txt", """
                stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,wheelchair_boarding,stop_timezone,location_type,parent_station
                S1,C1,Capolinea,,41.9,12.5,,1,,0,
                S2,C2,Fermata Di Mezzo,,41.91,12.51,,1,,0,
                """);
        write("routes.txt", """
                route_id,route_short_name,route_long_name
                R1,64,Linea 64
                R2,40,Linea 40
                """);
        write("trips.txt", """
                route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,exceptional
                R1,SVC1,T1,Termini,,0,,,1,0
                R1,SVC1,T2,Termini,,0,,,1,0
                R1,SVC1,T3,Termini,,0,,,1,0
                R2,SVC1,G1,Giro,,0,,,1,0
                R2,SVC1,G2,Giro,,0,,,1,0
                R2,SVC1,G3,Giro,,0,,,1,0
                """);
        write("calendar_dates.txt", """
                service_id,date,exception_type
                SVC1,20260721,1
                """);
        /*
         * La 64 parte da S1 e passa da S2 venti minuti dopo: sono le due colonne
         * che la pagina della fermata e quella della linea devono saper
         * distinguere. La 40 e' circolare e torna al capolinea a fine giro.
         */
        write("stop_times.txt", """
                trip_id,arrival_time,departure_time,stop_id,stop_sequence
                T1,06:00:00,06:00:00,S1,1
                T1,06:20:00,06:20:00,S2,2
                T2,06:30:00,06:30:00,S1,1
                T2,06:50:00,06:50:00,S2,2
                T3,07:00:00,07:00:00,S1,1
                T3,07:20:00,07:20:00,S2,2
                G1,08:00:00,08:00:00,S1,1
                G1,08:10:00,08:10:00,S2,2
                G1,08:20:00,08:20:00,S1,3
                G2,08:30:00,08:30:00,S1,1
                G2,08:40:00,08:40:00,S2,2
                G2,08:50:00,08:50:00,S1,3
                G3,09:00:00,09:00:00,S1,1
                G3,09:10:00,09:10:00,S2,2
                G3,09:20:00,09:20:00,S1,3
                """);

        GtfsProperties props = new GtfsProperties(
                new GtfsProperties.StaticProps(null, dataDir.toString(), 0L),
                null
        );
        service = new GtfsIndexService(props);
        service.init();
        service.rebuildIndexes();
    }

    private GtfsIndexService.StopSchedule riga(String stopId, String linea) {
        List<GtfsIndexService.StopSchedule> righe = service.stopSchedules(GIORNO).stream()
                .filter(s -> s.stopId().equals(stopId) && s.line().equals(linea))
                .toList();
        assertEquals(1, righe.size(), "attesa una riga sola per " + stopId + "/" + linea);
        return righe.getFirst();
    }

    @Test
    @DisplayName("alla fermata di mezzo gli orari sono i suoi, non quelli del capolinea")
    void orariDellaFermataNonDelCapolinea() {
        GtfsIndexService.StopSchedule capolinea = riga("S1", "64");
        assertEquals(6 * 3600, capolinea.schedule().firstDepartureSeconds());
        assertEquals(7 * 3600, capolinea.schedule().lastDepartureSeconds());

        GtfsIndexService.StopSchedule mezzo = riga("S2", "64");
        assertEquals(6 * 3600 + 20 * 60, mezzo.schedule().firstDepartureSeconds());
        assertEquals(7 * 3600 + 20 * 60, mezzo.schedule().lastDepartureSeconds());
        assertEquals(3, mezzo.schedule().tripCount());
        assertEquals(30, mezzo.schedule().typicalHeadwayMinutes());
    }

    @Test
    @DisplayName("la circolare conta una volta sola la corsa che torna al capolinea")
    void circolareNonRaddoppiaLeCorse() {
        GtfsIndexService.StopSchedule giro = riga("S1", "40");
        // Tre corse, non sei: il rientro delle 08:20 e' la stessa corsa delle 08:00.
        assertEquals(3, giro.schedule().tripCount());
        assertEquals(30, giro.schedule().typicalHeadwayMinutes());
        assertEquals(8 * 3600, giro.schedule().firstDepartureSeconds());
        // L'ultima corsa e' l'ultima PARTENZA, non il rientro delle 09:20.
        assertEquals(9 * 3600, giro.schedule().lastDepartureSeconds());
    }

    @Test
    @DisplayName("un giorno senza corse non produce righe")
    void giornoSenzaServizio() {
        assertTrue(service.stopSchedules(GIORNO.plusDays(1)).isEmpty());
        assertTrue(service.stopSchedules(null).isEmpty());
    }

    @Test
    @DisplayName("con meno di tre corse l'intervallo resta vuoto invece di inventarsi un numero")
    void intervalloSoloConAbbastanzaCorse() throws IOException {
        write("stop_times.txt", """
                trip_id,arrival_time,departure_time,stop_id,stop_sequence
                T1,06:00:00,06:00:00,S1,1
                T1,06:20:00,06:20:00,S2,2
                T2,06:30:00,06:30:00,S1,1
                T2,06:50:00,06:50:00,S2,2
                """);
        service.rebuildIndexes();

        GtfsIndexService.StopSchedule due = riga("S2", "64");
        assertEquals(2, due.schedule().tripCount());
        assertNull(due.schedule().typicalHeadwayMinutes());
    }

    @Test
    @DisplayName("gli orari non toccano la cache degli indici che usano arrivi e fermate vicine")
    void nonSvuotaLaCacheDelGiorno() throws Exception {
        // Il warm-up avviato da rebuildIndexes() gira su un altro thread e mette in
        // cache oggi e domani. Se finisse fra la fotografia e il confronto le chiavi
        // cambierebbero senza che stopSchedules c'entri: lo si lascia finire prima.
        service.awaitWarmup();
        Map<LocalDate, ?> prima = Map.copyOf(dateInCache());
        // Il caso da proteggere e' proprio questo, cache piena dei giorni scaldati:
        // a cache vuota il test non vedrebbe l'espulsione di oggi.
        assertFalse(prima.isEmpty(), "il warm-up deve aver gia' messo in cache i suoi giorni");

        service.stopSchedules(GIORNO);
        // Prima il conto passava dalla cache per-data, che tiene solo tre giorni:
        // chiedere una data lontana buttava fuori quella di oggi. Ora la cache
        // deve restare com'era, con dentro i giorni scaldati all'avvio.
        assertEquals(prima.keySet(), dateInCache().keySet(), "il calcolo non deve toccare la cache condivisa");
    }

    @Test
    @DisplayName("la stessa data si calcola una volta sola, finche' il feed non cambia")
    void risultatoRiusato() throws IOException {
        List<GtfsIndexService.StopSchedule> prima = service.stopSchedules(GIORNO);
        assertSame(prima, service.stopSchedules(GIORNO));

        service.rebuildIndexes();
        assertNotSame(prima, service.stopSchedules(GIORNO), "dopo un feed nuovo va ricalcolato");
    }

    @SuppressWarnings("unchecked")
    private Map<LocalDate, ?> dateInCache() throws Exception {
        Field f = GtfsIndexService.class.getDeclaredField("scheduledByDateRef");
        f.setAccessible(true);
        return ((AtomicReference<Map<LocalDate, ?>>) f.get(service)).get();
    }

    private void write(String nome, String contenuto) throws IOException {
        Files.writeString(dataDir.resolve(nome), contenuto);
    }
}
