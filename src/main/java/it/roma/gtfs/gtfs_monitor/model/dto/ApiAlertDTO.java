package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;

public record ApiAlertDTO(
        String alertId,
        String line,
        String severity,
        String title,
        String description,
        Instant startsAt,
        Instant endsAt,
        Instant updatedAt
) {
}
