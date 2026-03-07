package it.roma.gtfs.gtfs_monitor.model.dto;

public record ApiPlannerLiveStopFocusDTO(
        ApiVehicleDTO vehicle,
        String etaLabel,
        String etaTime,
        Long etaEpochMillis,
        String source,
        Boolean hasLivePosition,
        String occupancyStatus,
        Boolean wheelchairAccessible
) {
}
