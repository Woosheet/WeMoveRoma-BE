package it.roma.gtfs.gtfs_monitor.model.dto;

import java.util.List;

public record JourneyLegDTO(
        String mode,
        String line,
        String routeShortName,
        Boolean realtime,
        String headsign,
        String fromName,
        String toName,
        String startTime,
        String endTime,
        Integer durationMinutes,
        Integer realtimeDelayMinutes,
        Double fromLat,
        Double fromLon,
        Double toLat,
        Double toLon,
        String geometryPoints,
        List<JourneyLegStopDTO> stops
) {
}
