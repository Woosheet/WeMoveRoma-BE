package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Misura opt-in sul feed reale in data/gtfs_static: confronta il percorso veloce
 * (indice offset) con il fallback a scansione completa e ne verifica l'equivalenza.
 * Eseguire con: ./mvnw test -Dtest=GtfsIndexServiceStopTimesPerfTest -Dgtfs.stopTimesPerf=true
 */
class GtfsIndexServiceStopTimesPerfTest {

    private static final Path REAL_DATA_DIR = Path.of("data", "gtfs_static");

    @Test
    @SuppressWarnings("unchecked")
    void measureRealFeedTripLookup() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("gtfs.stopTimesPerf"),
                "Abilitare con -Dgtfs.stopTimesPerf=true");
        Assumptions.assumeTrue(Files.exists(REAL_DATA_DIR.resolve("stop_times.txt")),
                "Feed reale non presente in " + REAL_DATA_DIR);

        GtfsProperties props = new GtfsProperties(
                new GtfsProperties.StaticProps(null, REAL_DATA_DIR.toString(), 0L),
                null
        );
        GtfsIndexService service = new GtfsIndexService(props);
        service.init();

        long tRebuild = System.nanoTime();
        service.rebuildIndexes();
        System.out.printf("rebuildIndexes (incluso indice offset): %d ms%n", (System.nanoTime() - tRebuild) / 1_000_000);

        String tripId = firstTripIdFromStopTimes();
        GtfsIndexService.Trip trip = service.tripById(tripId).orElseThrow();
        LocalDate serviceDate = firstActiveDateForService(trip.serviceId());
        assertNotNull(serviceDate, "Nessuna data attiva in calendar_dates.txt per service " + trip.serviceId());
        System.out.printf("trip=%s service=%s data=%s%n", tripId, trip.serviceId(), serviceDate);

        Method perDate = GtfsIndexService.class.getDeclaredMethod(
                "scheduledStopsForTripOnDate", GtfsIndexService.Trip.class, LocalDate.class);
        perDate.setAccessible(true);

        // Percorso veloce (indice offset).
        List<GtfsIndexService.ScheduledTripStop> fast = (List<GtfsIndexService.ScheduledTripStop>) perDate.invoke(service, trip, serviceDate);
        assertFalse(fast.isEmpty());
        int iterations = 50;
        long tFast = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            perDate.invoke(service, trip, serviceDate);
        }
        double fastMs = (System.nanoTime() - tFast) / 1_000_000.0 / iterations;
        System.out.printf("percorso veloce (indice offset): %.3f ms/lookup su %d iterazioni, %d fermate%n",
                fastMs, iterations, fast.size());

        // Percorso lento (scansione completa), forzato svuotando l'indice offset.
        Field refField = GtfsIndexService.class.getDeclaredField("stopTimesOffsetsRef");
        refField.setAccessible(true);
        AtomicReference<Object> offsetsRef = (AtomicReference<Object>) refField.get(service);
        Object previous = offsetsRef.get();
        Field emptyField = Class.forName("it.roma.gtfs.gtfs_monitor.service.GtfsIndexService$StopTimesOffsetIndex")
                .getDeclaredField("EMPTY");
        emptyField.setAccessible(true);
        offsetsRef.set(emptyField.get(null));
        try {
            long tSlow = System.nanoTime();
            List<GtfsIndexService.ScheduledTripStop> slow = (List<GtfsIndexService.ScheduledTripStop>) perDate.invoke(service, trip, serviceDate);
            double slowMs = (System.nanoTime() - tSlow) / 1_000_000.0;
            System.out.printf("percorso lento (scansione completa): %.0f ms/lookup%n", slowMs);
            assertEquals(slow, fast, "Il percorso veloce deve produrre lo stesso output della scansione completa");
        } finally {
            offsetsRef.set(previous);
        }
    }

    private static String firstTripIdFromStopTimes() throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(REAL_DATA_DIR.resolve("stop_times.txt"))) {
            reader.readLine(); // header
            String line = reader.readLine();
            return line.substring(0, line.indexOf(','));
        }
    }

    private static LocalDate firstActiveDateForService(String serviceId) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(REAL_DATA_DIR.resolve("calendar_dates.txt"))) {
            String line = reader.readLine(); // header
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 3 && serviceId.equals(parts[0].trim()) && "1".equals(parts[2].trim())) {
                    return LocalDate.parse(parts[1].trim(), DateTimeFormatter.BASIC_ISO_DATE);
                }
            }
        }
        return null;
    }
}
