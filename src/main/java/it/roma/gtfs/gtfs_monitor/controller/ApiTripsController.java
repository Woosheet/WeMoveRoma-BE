package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiTripShapeDTO;
import it.roma.gtfs.gtfs_monitor.service.GtfsIndexService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/trips")
public class ApiTripsController {

    private final GtfsIndexService gtfsIndexService;

    @GetMapping("/{tripId}/shape")
    public ApiTripShapeDTO shape(@PathVariable String tripId) {
        List<ApiTripShapeDTO.Point> points = gtfsIndexService.shapeByTripId(tripId).stream()
                .map(p -> new ApiTripShapeDTO.Point(
                        p.lat() != null ? p.lat().doubleValue() : null,
                        p.lon() != null ? p.lon().doubleValue() : null,
                        p.sequence()))
                .toList();
        log.debug("GET /api/v1/trips/{}/shape -> {} points", tripId, points.size());
        return new ApiTripShapeDTO(tripId, points);
    }
}
