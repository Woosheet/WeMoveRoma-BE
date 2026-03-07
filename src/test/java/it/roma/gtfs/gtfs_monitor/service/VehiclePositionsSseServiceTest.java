package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class VehiclePositionsSseServiceTest {

    @Test
    void simulateVehiclesKeepsReturningPredicted990LForReportedStreamInstant() throws Exception {
        VehiclePositionsSseService sseService = newSseService();
        Method simulateVehicles = VehiclePositionsSseService.class.getDeclaredMethod(
                "simulateVehicles",
                String.class,
                String.class,
                Instant.class,
                int.class
        );
        simulateVehicles.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<VehiclePositionDTO> vehicles = (List<VehiclePositionDTO>) simulateVehicles.invoke(
                sseService,
                "990L",
                "STAZ.NE METRO CIPRO (MA)",
                Instant.parse("2026-03-07T19:14:06.952Z"),
                1
        );

        assertFalse(vehicles.isEmpty());
        assertEquals("990L", vehicles.getFirst().getLinea());
    }

    @Test
    void filterSnapshotKeepsSpecificPredictedVehicleAliveForSimulatedVehicleSubscriptions() throws Exception {
        VehiclePositionsSseService sseService = newSseService();
        Class<?> subscriptionClass = Class.forName("it.roma.gtfs.gtfs_monitor.service.VehiclePositionsSseService$Subscription");
        Constructor<?> subscriptionCtor = subscriptionClass.getDeclaredConstructor(SseEmitter.class, String.class, String.class, String.class);
        subscriptionCtor.setAccessible(true);
        Object subscription = subscriptionCtor.newInstance(
                new SseEmitter(0L),
                "990L",
                "staz ne metro cipro ma",
                "sim-0#3734-31"
        );

        Method filterSnapshot = VehiclePositionsSseService.class.getDeclaredMethod(
                "filterSnapshot",
                List.class,
                subscriptionClass,
                Instant.class
        );
        filterSnapshot.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<VehiclePositionDTO> vehicles = (List<VehiclePositionDTO>) filterSnapshot.invoke(
                sseService,
                List.of(),
                subscription,
                Instant.parse("2026-03-07T19:14:06.952Z")
        );

        assertFalse(vehicles.isEmpty());
        assertEquals("sim-0#3734-31", vehicles.getFirst().getVeicolo());
        assertEquals("990L", vehicles.getFirst().getLinea());
    }

    private static VehiclePositionsSseService newSseService() throws Exception {
        GtfsProperties props = new GtfsProperties(
                new GtfsProperties.StaticProps("unused", "data/gtfs_static", 0L),
                new GtfsProperties.RealtimeProps(null, null, null, 5_000L)
        );
        GtfsIndexService indexService = new GtfsIndexService(props);
        indexService.init();
        indexService.rebuildIndexes();
        return new VehiclePositionsSseService(indexService);
    }
}
