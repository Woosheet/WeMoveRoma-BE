package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.config.ResourceNotFoundException;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiTripShapeDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiTripScheduledStopDTO;
import it.roma.gtfs.gtfs_monitor.service.GtfsIndexService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
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
                        (double) p.lat(),
                        (double) p.lon(),
                        p.sequence()))
                .toList();
        log.debug("GET /api/v1/trips/{}/shape -> {} points", tripId, points.size());
        return new ApiTripShapeDTO(tripId, points);
    }

    /**
     * Fermate programmate di una corsa. {@code 404} se la corsa non esiste,
     * {@code 200 []} se esiste ma non e' programmata nella data di riferimento
     * (tipico delle linee che non girano tutti i giorni): prima entrambi i casi
     * davano {@code 200 []} e il client non poteva distinguerli.
     */
    @GetMapping("/{tripId}/stops")
    public List<ApiTripScheduledStopDTO> stops(
            @PathVariable String tripId,
            @RequestParam(required = false) String when
    ) {
        requireExistingTrip(tripId);
        Instant reference = parseInstantOrNow(when);
        List<ApiTripScheduledStopDTO> items = gtfsIndexService.scheduledStopsForTrip(tripId, reference).stream()
                .map(stop -> new ApiTripScheduledStopDTO(
                        stop.stopId(),
                        stop.stopName(),
                        stop.tripId(),
                        stop.line(),
                        stop.lat(),
                        stop.lon(),
                        stop.arrivalTime() != null ? stop.arrivalTime().toString() : null,
                        stop.departureTime() != null ? stop.departureTime().toString() : null
                ))
                .toList();
        log.debug("GET /api/v1/trips/{}/stops -> {} stops", tripId, items.size());
        return items;
    }

    /**
     * 404 se la corsa non esiste. Con gli indici ancora vuoti (refresh di startup in
     * corso) nessun id esisterebbe: li' si risponde 503, non 404 — non e' colpa del client.
     */
    private void requireExistingTrip(String tripId) {
        if (!gtfsIndexService.isStaticDataLoaded()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Dati GTFS non ancora caricati, riprovare tra qualche istante");
        }
        if (gtfsIndexService.tripByIdOrNull(tripId) == null) {
            throw ResourceNotFoundException.trip(tripId);
        }
    }

    private static Instant parseInstantOrNow(String value) {
        if (value == null || value.isBlank()) {
            return Instant.now();
        }
        try {
            return Instant.parse(value);
        } catch (Exception ignored) {
            return Instant.now();
        }
    }
}
