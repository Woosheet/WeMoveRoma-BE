package it.roma.gtfs.gtfs_monitor.model.dto;

public record JourneyLocationDTO(
        Double lat,
        Double lon,
        String label
) {
}
