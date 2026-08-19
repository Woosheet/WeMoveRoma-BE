package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiListResponseDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiStopSearchItemDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.NearbyStopDTO;
import it.roma.gtfs.gtfs_monitor.service.GtfsIndexService;
import it.roma.gtfs.gtfs_monitor.service.NearbyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/stops")
public class ApiStopsController {
    private final NearbyService nearbyService;
    private final GtfsIndexService indexService;

    @GetMapping
    public ApiListResponseDTO<ApiStopSearchItemDTO> listStops(
            @RequestParam(required = false) Double minLat,
            @RequestParam(required = false) Double maxLat,
            @RequestParam(required = false) Double minLon,
            @RequestParam(required = false) Double maxLon,
            @RequestParam(required = false) Integer limit
    ) {
        List<ApiStopSearchItemDTO> items = nearbyService.listStops(minLat, maxLat, minLon, maxLon, limit);
        // Versione dei dati, non istante della richiesta: con Instant.now() la
        // risposta cambiava a ogni chiamata e l'ETag non produceva mai un 304.
        return new ApiListResponseDTO<>(items, items.size(), indexService.dataVersion());
    }

    @GetMapping("/search")
    public ApiListResponseDTO<ApiStopSearchItemDTO> searchStops(
            @RequestParam String q,
            @RequestParam(required = false) Integer limit
    ) {
        List<ApiStopSearchItemDTO> items = nearbyService.searchStops(q, limit);
        return new ApiListResponseDTO<>(items, items.size(), indexService.dataVersion());
    }

    /**
     * Arrivi a una fermata. {@code 404} se la fermata non esiste, {@code 200} con
     * {@code arrivals} vuoto se esiste ma non ha corse in arrivo: sono due cose
     * diverse e il client deve poterle distinguere.
     */
    @GetMapping("/{stopId}/arrivals")
    public NearbyStopDTO getStopArrivals(
            @PathVariable String stopId,
            @RequestParam(required = false) Integer limit
    ) {
        log.debug("GET /api/v1/stops/{}/arrivals limit={}", stopId, limit);
        requireStaticData();
        return nearbyService.stopArrivals(stopId, limit);
    }

    /** Con gli indici ancora vuoti nessun id esiste: 503, non 404. */
    private void requireStaticData() {
        if (!indexService.isStaticDataLoaded()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Dati GTFS non ancora caricati, riprovare tra qualche istante");
        }
    }
}
