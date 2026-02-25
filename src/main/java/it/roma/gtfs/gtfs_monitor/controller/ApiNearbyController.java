package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.NearbyResponseDTO;
import it.roma.gtfs.gtfs_monitor.service.NearbyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/nearby")
public class ApiNearbyController {
    private final NearbyService nearbyService;

    @GetMapping
    public NearbyResponseDTO nearby(
            @RequestParam double lat,
            @RequestParam double lon,
            @RequestParam(required = false) Integer radiusMeters,
            @RequestParam(required = false) Integer limitStops,
            @RequestParam(required = false) Integer limitArrivalsPerStop
    ) {
        log.debug("GET /api/v1/nearby lat={} lon={} radius={}", lat, lon, radiusMeters);
        return nearbyService.nearby(lat, lon, radiusMeters, limitStops, limitArrivalsPerStop);
    }
}
