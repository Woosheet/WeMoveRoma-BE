package it.roma.gtfs.gtfs_monitor.model.dto;

import java.util.List;

public record ApiTripShapeDTO(
        String tripId,
        List<Point> points
) {
    public record Point(Double lat, Double lon, Integer sequence) {}
}
