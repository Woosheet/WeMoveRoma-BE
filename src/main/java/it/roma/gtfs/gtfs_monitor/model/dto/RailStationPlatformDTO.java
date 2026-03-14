package it.roma.gtfs.gtfs_monitor.model.dto;

public record RailStationPlatformDTO(
        String stationName,
        String scheduledTimeLabel,
        String scheduledTime,
        String actualTimeLabel,
        String actualTime,
        String plannedPlatform,
        String actualPlatform
) {
}
