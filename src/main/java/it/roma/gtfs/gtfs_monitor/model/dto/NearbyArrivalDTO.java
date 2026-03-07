package it.roma.gtfs.gtfs_monitor.model.dto;

public record NearbyArrivalDTO(
        String line,
        String destination,
        String tripId,
        String stopId,
        String stopName,
        String arrivalTime,
        String departureTime,
        Integer etaMinutes,
        Boolean hasLivePosition,
        String occupancyStatus,
        Boolean wheelchairAccessible
) {
}
