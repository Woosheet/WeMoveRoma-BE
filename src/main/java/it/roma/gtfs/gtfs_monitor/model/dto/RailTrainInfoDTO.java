package it.roma.gtfs.gtfs_monitor.model.dto;

import java.util.List;

public record RailTrainInfoDTO(
        String source,
        String trainNumber,
        String trainLabel,
        String statusSummary,
        Integer delayMinutes,
        String lastSeenStation,
        String lastSeenTime,
        RailStationPlatformDTO departure,
        RailStationPlatformDTO arrival,
        RailStationPlatformDTO requestedStation,
        List<RailStationPlatformDTO> intermediateStops
) {
}
