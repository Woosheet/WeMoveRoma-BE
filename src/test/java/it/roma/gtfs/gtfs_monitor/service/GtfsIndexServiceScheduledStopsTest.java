package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test di caratterizzazione del lookup stop_times per singolo trip
 * (percorso veloce via indice offset + fallback a scansione completa).
 */
class GtfsIndexServiceScheduledStopsTest {

    private static final ZoneId ROME_ZONE = ZoneId.of("Europe/Rome");
    private static final LocalDate SERVICE_DATE = LocalDate.of(2026, 7, 21);

    @TempDir
    Path dataDir;

    private GtfsIndexService service;

    @BeforeEach
    void setUp() throws IOException {
        write("stops.txt", """
                stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,wheelchair_boarding,stop_timezone,location_type,parent_station
                S1,C1,Prima Fermata,,41.9,12.5,,1,,0,
                S2,C2,Seconda Fermata,,41.91,12.51,,1,,0,
                S3,C3,Terza Fermata,,41.92,12.52,,1,,0,
                S4,C4,Quarta Fermata,,41.93,12.53,,1,,0,
                """);
        write("routes.txt", """
                route_id,route_short_name,route_long_name
                R1,64,Linea 64
                """);
        write("trips.txt", """
                route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,exceptional
                R1,SVC1,T1,Termini,,0,,,1,0
                R1,SVC1,T2,San Pietro,,1,,,1,0
                """);
        write("calendar_dates.txt", """
                service_id,date,exception_type
                SVC1,20260721,1
                """);
        write("stop_times.txt", """
                trip_id,arrival_time,departure_time,stop_id,stop_sequence
                T1,08:00:00,08:00:30,S1,1
                T1,08:05:00,08:05:30,S2,2
                T1,08:10:00,08:10:30,S3,3
                T2,09:00:00,09:00:00,S4,1
                T2,09:04:00,09:04:00,S1,2
                """);

        GtfsProperties props = new GtfsProperties(
                new GtfsProperties.StaticProps(null, dataDir.toString(), 0L),
                null
        );
        service = new GtfsIndexService(props);
        service.init();
        service.rebuildIndexes();
    }

    @Test
    void scheduledStopsForTripReturnsOnlyTripRowsInStopSequenceOrder() {
        Instant when = SERVICE_DATE.atTime(7, 0).atZone(ROME_ZONE).toInstant();

        List<GtfsIndexService.ScheduledTripStop> stops = service.scheduledStopsForTrip("T1", when);

        assertEquals(List.of("S1", "S2", "S3"), stopIds(stops));
        assertTrue(stops.stream().allMatch(s -> "T1".equals(s.tripId())));
        assertEquals("Prima Fermata", stops.getFirst().stopName());
        assertEquals("64", stops.getFirst().line());
        assertEquals(41.9, stops.getFirst().lat(), 1e-4);
        assertEquals(12.5, stops.getFirst().lon(), 1e-4);

        long serviceStart = SERVICE_DATE.atStartOfDay(ROME_ZONE).toEpochSecond();
        assertEquals(Instant.ofEpochSecond(serviceStart + 8 * 3600), stops.getFirst().arrivalTime());
        assertEquals(Instant.ofEpochSecond(serviceStart + 8 * 3600 + 30), stops.getFirst().departureTime());
        assertEquals(Instant.ofEpochSecond(serviceStart + 8 * 3600 + 10 * 60), stops.getLast().arrivalTime());
    }

    @Test
    void scheduledStopsForTripFallsForwardToNextActiveServiceDate() {
        // Il 20/07 SVC1 non e' attivo: il metodo prova il giorno successivo (attivo).
        Instant when = SERVICE_DATE.minusDays(1).atTime(12, 0).atZone(ROME_ZONE).toInstant();

        List<GtfsIndexService.ScheduledTripStop> stops = service.scheduledStopsForTrip("T1", when);

        assertEquals(List.of("S1", "S2", "S3"), stopIds(stops));
        long serviceStart = SERVICE_DATE.atStartOfDay(ROME_ZONE).toEpochSecond();
        assertEquals(Instant.ofEpochSecond(serviceStart + 8 * 3600), stops.getFirst().arrivalTime());
    }

    @Test
    void scheduledNextStopsForTripKeepsRecentlyPassedStopWithinTolerance() {
        // now = 08:04 -> soglia 07:59: S1 (08:00) rientra nella tolleranza di 5 minuti.
        Instant now = SERVICE_DATE.atTime(8, 4).atZone(ROME_ZONE).toInstant();

        List<GtfsIndexService.ScheduledTripStop> stops = service.scheduledNextStopsForTrip("T1", now, 10);

        assertEquals(List.of("S1", "S2", "S3"), stopIds(stops));
    }

    @Test
    void scheduledNextStopsForTripSkipsPastStopsAndHonoursLimit() {
        // now = 08:07 -> soglia 08:02: S1 (08:00) esclusa, S2 e S3 incluse.
        Instant now = SERVICE_DATE.atTime(8, 7).atZone(ROME_ZONE).toInstant();

        assertEquals(List.of("S2", "S3"), stopIds(service.scheduledNextStopsForTrip("T1", now, 10)));
        assertEquals(List.of("S2"), stopIds(service.scheduledNextStopsForTrip("T1", now, 1)));
    }

    @Test
    void staleOffsetIndexFallsBackToFullScanOfCurrentFile() throws IOException {
        Instant when = SERVICE_DATE.atTime(7, 0).atZone(ROME_ZONE).toInstant();
        List<GtfsIndexService.ScheduledTripStop> before = service.scheduledStopsForTrip("T1", when);

        // Modifica stop_times.txt SENZA rebuild: l'indice offset diventa stale (size diversa)
        // e il lookup deve ricadere sulla scansione completa del file aggiornato.
        Files.writeString(dataDir.resolve("stop_times.txt"),
                "T2,09:08:00,09:08:00,S2,3\n", StandardOpenOption.APPEND);

        assertEquals(before, service.scheduledStopsForTrip("T1", when));
        assertEquals(List.of("S4", "S1", "S2"), stopIds(service.scheduledStopsForTrip("T2", when)));
    }

    @Test
    void unknownTripReturnsEmptyList() {
        Instant when = SERVICE_DATE.atTime(7, 0).atZone(ROME_ZONE).toInstant();

        assertEquals(List.of(), service.scheduledStopsForTrip("T999", when));
        assertEquals(List.of(), service.scheduledNextStopsForTrip("T999", when, 10));
    }

    private static List<String> stopIds(List<GtfsIndexService.ScheduledTripStop> stops) {
        return stops.stream().map(GtfsIndexService.ScheduledTripStop::stopId).toList();
    }

    private void write(String fileName, String content) throws IOException {
        Files.writeString(dataDir.resolve(fileName), content);
    }
}
