package it.roma.gtfs.gtfs_monitor.model.dto;

public record ApiStopSearchItemDTO(
        String stopId,
        String stopCode,
        String stopName,
        Double lat,
        Double lon
) {
}
