package it.roma.gtfs.gtfs_monitor.model.dto;

import java.util.List;

public record NearbyStopDTO(
        String stopId,
        String stopName,
        Double lat,
        Double lon,
        Integer distanceMeters,
        Integer walkMinutes,
        List<NearbyArrivalDTO> arrivals
) {
}
