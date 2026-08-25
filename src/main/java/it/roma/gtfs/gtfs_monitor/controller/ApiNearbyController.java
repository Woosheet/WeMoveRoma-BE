package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.NearbyResponseDTO;
import it.roma.gtfs.gtfs_monitor.service.NearbyService;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@Validated
@RequiredArgsConstructor
@RequestMapping("/api/v1/nearby")
public class ApiNearbyController {
    private final NearbyService nearbyService;

    @GetMapping
    public NearbyResponseDTO nearby(
            @RequestParam @DecimalMin("-90") @DecimalMax("90") double lat,
            @RequestParam @DecimalMin("-180") @DecimalMax("180") double lon,
            @RequestParam(required = false) @Positive Integer radiusMeters,
            @RequestParam(required = false) @Positive Integer limitStops,
            @RequestParam(required = false) @Positive Integer limitArrivalsPerStop
    ) {
        // NaN e infiniti superano i vincoli di intervallo (ogni confronto con NaN
        // e' falso), ma poi degenerano nel calcolo della distanza e producono
        // risultati arbitrari: vanno fermati esplicitamente.
        if (!Double.isFinite(lat) || !Double.isFinite(lon)) {
            throw new IllegalArgumentException("lat e lon devono essere numeri finiti");
        }
        log.debug("GET /api/v1/nearby lat={} lon={} radius={}", lat, lon, radiusMeters);
        return nearbyService.nearby(lat, lon, radiusMeters, limitStops, limitArrivalsPerStop);
    }
}
