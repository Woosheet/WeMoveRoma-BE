package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;
import java.util.List;

public record JourneyPlanResponseDTO(
        String provider,
        JourneyLocationDTO from,
        JourneyLocationDTO to,
        List<JourneyOptionDTO> options,
        String error,
        Instant generatedAt
) {
}
