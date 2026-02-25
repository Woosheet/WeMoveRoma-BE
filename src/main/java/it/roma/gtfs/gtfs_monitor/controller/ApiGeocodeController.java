package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.ReverseGeocodeDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.GeocodeSearchResultDTO;
import it.roma.gtfs.gtfs_monitor.service.GeocodeSearchService;
import it.roma.gtfs.gtfs_monitor.service.ReverseGeocodeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/geocode")
public class ApiGeocodeController {
    private final ReverseGeocodeService reverseGeocodeService;
    private final GeocodeSearchService geocodeSearchService;

    @GetMapping("/search")
    public java.util.List<GeocodeSearchResultDTO> search(
            @RequestParam String q,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Double biasLat,
            @RequestParam(required = false) Double biasLon
    ) {
        log.debug("GET /api/v1/geocode/search q={}", q);
        return geocodeSearchService.search(q, limit, biasLat, biasLon);
    }

    @GetMapping("/reverse")
    public ReverseGeocodeDTO reverse(
            @RequestParam double lat,
            @RequestParam double lon
    ) {
        log.debug("GET /api/v1/geocode/reverse lat={} lon={}", lat, lon);
        return reverseGeocodeService.reverse(lat, lon);
    }
}
