package it.roma.gtfs.gtfs_monitor.model.dto;

public record SearchSuggestionDTO(
        String type,
        String value,
        String label,
        int score,
        String line,
        String destination
) {
}
