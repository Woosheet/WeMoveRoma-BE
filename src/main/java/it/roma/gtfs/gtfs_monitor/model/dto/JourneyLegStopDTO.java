package it.roma.gtfs.gtfs_monitor.model.dto;

public record JourneyLegStopDTO(
        String stopName,
        Double lat,
        Double lon
) {
}
