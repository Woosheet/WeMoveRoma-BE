package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;
import java.util.List;

public record ApiListResponseDTO<T>(
        List<T> items,
        int total,
        Instant generatedAt
) {
}
