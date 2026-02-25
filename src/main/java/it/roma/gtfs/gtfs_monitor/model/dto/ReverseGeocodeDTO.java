package it.roma.gtfs.gtfs_monitor.model.dto;

public record ReverseGeocodeDTO(
        Double lat,
        Double lon,
        String label,
        String street,
        String city,
        String country
) {
}
