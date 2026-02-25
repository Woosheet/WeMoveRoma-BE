package it.roma.gtfs.gtfs_monitor.model.dto;

public record ApiVehicleDTO(
        String vehicleId,
        String line,
        String destination,
        String tripId,
        Double lat,
        Double lon,
        Double speedKmh,
        String lastUpdateAt
) {
}
