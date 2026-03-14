package it.roma.gtfs.gtfs_monitor.model.dto;

public record ApiTripScheduledStopDTO(
        String stopId,
        String stopName,
        String tripId,
        String line,
        Double lat,
        Double lon,
        String arrivalTime,
        String departureTime
) {
}
