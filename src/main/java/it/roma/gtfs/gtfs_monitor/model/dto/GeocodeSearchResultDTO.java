package it.roma.gtfs.gtfs_monitor.model.dto;

public record GeocodeSearchResultDTO(
        Double lat,
        Double lon,
        String label,
        String name
) {
}
