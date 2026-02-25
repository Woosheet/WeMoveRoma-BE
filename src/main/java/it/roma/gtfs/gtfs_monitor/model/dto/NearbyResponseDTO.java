package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;
import java.util.List;

public record NearbyResponseDTO(
        Double lat,
        Double lon,
        Integer radiusMeters,
        List<NearbyStopDTO> stops,
        Instant generatedAt
) {
}
