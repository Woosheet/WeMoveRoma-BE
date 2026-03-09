package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiListResponseDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiStopSearchItemDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.NearbyStopDTO;
import it.roma.gtfs.gtfs_monitor.service.NearbyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/stops")
public class ApiStopsController {
    private final NearbyService nearbyService;

    @GetMapping
    public ApiListResponseDTO<ApiStopSearchItemDTO> listStops(
            @RequestParam(required = false) Double minLat,
            @RequestParam(required = false) Double maxLat,
            @RequestParam(required = false) Double minLon,
            @RequestParam(required = false) Double maxLon,
            @RequestParam(required = false) Integer limit
    ) {
        List<ApiStopSearchItemDTO> items = nearbyService.listStops(minLat, maxLat, minLon, maxLon, limit);
        return new ApiListResponseDTO<>(items, items.size(), Instant.now());
    }

    @GetMapping("/search")
    public ApiListResponseDTO<ApiStopSearchItemDTO> searchStops(
            @RequestParam String q,
            @RequestParam(required = false) Integer limit
    ) {
        List<ApiStopSearchItemDTO> items = nearbyService.searchStops(q, limit);
        return new ApiListResponseDTO<>(items, items.size(), Instant.now());
    }

    @GetMapping("/{stopId}/arrivals")
    public NearbyStopDTO getStopArrivals(
            @PathVariable String stopId,
            @RequestParam(required = false) Integer limit
    ) {
        log.debug("GET /api/v1/stops/{}/arrivals limit={}", stopId, limit);
        return nearbyService.stopArrivals(stopId, limit);
    }
}
