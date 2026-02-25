package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;
import java.util.List;

public record ApiVehicleNextStopsResponseDTO(
        String vehicleId,
        List<ApiVehicleNextStopDTO> items,
        int total,
        Instant generatedAt
) {
}
