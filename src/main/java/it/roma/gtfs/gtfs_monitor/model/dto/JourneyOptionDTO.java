package it.roma.gtfs.gtfs_monitor.model.dto;

import java.util.List;

public record JourneyOptionDTO(
        Integer durationMinutes,
        Integer walkMinutes,
        Integer waitingMinutes,
        Integer transfers,
        String startTime,
        String endTime,
        List<JourneyLegDTO> legs
) {
}
