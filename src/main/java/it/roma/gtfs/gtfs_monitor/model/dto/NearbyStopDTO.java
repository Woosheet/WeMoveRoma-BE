package it.roma.gtfs.gtfs_monitor.model.dto;

import java.util.List;

public record NearbyStopDTO(
        String stopId,
        String stopName,
        Double lat,
        Double lon,
        Integer distanceMeters,
        Integer walkMinutes,
        /**
         * Linee servite dalla fermata nella giornata di servizio, a prescindere
         * dagli arrivi presenti in questo momento: di notte `arrivals` puo'
         * essere vuoto mentre la fermata resta servita da quelle linee.
         */
        List<String> servedLines,
        List<NearbyArrivalDTO> arrivals
) {
}
