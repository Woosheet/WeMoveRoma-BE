package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;

public record DashboardSummaryDTO(
        int visibleVehicles,
        int activeLines,
        long delayedVehicles,
        int alertsActive,
        Instant generatedAt
) {
}
