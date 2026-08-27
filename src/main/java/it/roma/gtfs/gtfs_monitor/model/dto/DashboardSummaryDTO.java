package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;

public record DashboardSummaryDTO(
        int visibleVehicles,
        int activeLines,
        long delayedVehicles,
        int alertsActive,
        Instant generatedAt,
        /**
         * Last-Modified del feed GTFS statico, come lo dichiara Roma Mobilita'.
         * Serve a chi rigenera gli snapshot per le pagine pubbliche: confronta
         * questo valore con quello dell'ultima rigenerazione e rifa' il lavoro
         * solo se il feed e' stato davvero ripubblicato. Null se non noto.
         */
        String feedLastModified
) {
}
