package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.DashboardSummaryDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class DashboardService {

    private final VehiclePositionsService vehiclePositionsService;
    private final TripUpdatesService tripUpdatesService;
    private final ServiceAlertsService serviceAlertsService;

    public DashboardSummaryDTO summary(String linea, String destination) {
        List<VehiclePositionDTO> vehicles = vehiclePositionsService.fetch(linea, destination, null);
        int activeLines = (int) vehicles.stream()
                .map(VehiclePositionDTO::getLinea)
                .filter(Objects::nonNull)
                .distinct()
                .count();

        long delayedVehicles = tripUpdatesService.countDelayedVehicles(linea);
        int alertsActive = serviceAlertsService.fetchActiveNow(linea, null).size();
        Instant generatedAt = vehiclePositionsService.lastSnapshotAt();
        if (generatedAt == null) {
            generatedAt = Instant.now();
        }

        return new DashboardSummaryDTO(
                vehicles.size(),
                activeLines,
                delayedVehicles,
                alertsActive,
                generatedAt
        );
    }
}
