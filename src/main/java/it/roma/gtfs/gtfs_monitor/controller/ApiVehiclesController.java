package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiListResponseDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiVehicleNextStopDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiVehicleNextStopsResponseDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiVehicleDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import it.roma.gtfs.gtfs_monitor.service.GtfsIndexService;
import it.roma.gtfs.gtfs_monitor.service.TripUpdatesService;
import it.roma.gtfs.gtfs_monitor.service.VehiclePositionsService;
import it.roma.gtfs.gtfs_monitor.service.VehiclePositionsSseService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/vehicles")
public class ApiVehiclesController {
    private static final DateTimeFormatter ROME_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");

    private final VehiclePositionsService vehiclePositionsService;
    private final VehiclePositionsSseService vehiclePositionsSseService;
    private final GtfsIndexService gtfsIndexService;
    private final TripUpdatesService tripUpdatesService;

    @GetMapping
    public ApiListResponseDTO<ApiVehicleDTO> getVehicles(
            @RequestParam(required = false) String linea,
            @RequestParam(required = false) String destination,
            @RequestParam(required = false) Integer limit
    ) {
        log.debug("GET /api/v1/vehicles linea={} destination={} limit={}", linea, destination, limit);
        List<ApiVehicleDTO> items = vehiclePositionsService.fetch(linea, destination, limit).stream()
                .map(this::toApiDto)
                .toList();
        Instant generatedAt = vehiclePositionsService.lastSnapshotAt();
        return new ApiListResponseDTO<>(items, items.size(), generatedAt != null ? generatedAt : Instant.now());
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamVehicles() {
        var snapshot = vehiclePositionsService.fetch(null, null, null);
        Instant generatedAt = vehiclePositionsService.lastSnapshotAt();
        return vehiclePositionsSseService.subscribe(snapshot, generatedAt != null ? generatedAt : Instant.now());
    }

    private ApiVehicleDTO toApiDto(VehiclePositionDTO dto) {
        return new ApiVehicleDTO(
                dto.getVeicolo(),
                gtfsIndexService.publicLineByRouteId(dto.getLinea()),
                dto.getCapolinea(),
                dto.getCorsa(),
                dto.getLat(),
                dto.getLon(),
                dto.getVelocitaKmh(),
                dto.getTimestamp()
        );
    }

    @GetMapping("/{vehicleId}/next-stops")
    public ApiVehicleNextStopsResponseDTO getVehicleNextStops(
            @PathVariable String vehicleId,
            @RequestParam(required = false) Integer limit
    ) {
        log.debug("GET /api/v1/vehicles/{}/next-stops limit={}", vehicleId, limit);
        List<ApiVehicleNextStopDTO> items = tripUpdatesService.nextStopsByVehicle(vehicleId, limit).stream()
                .map(this::toNextStopDto)
                .toList();
        return new ApiVehicleNextStopsResponseDTO(vehicleId, items, items.size(), Instant.now());
    }

    private ApiVehicleNextStopDTO toNextStopDto(TripUpdateDTO dto) {
        GtfsIndexService.Stop stop = gtfsIndexService.stopByIdOrNull(dto.getFermataId());
        return new ApiVehicleNextStopDTO(
                dto.getFermataId(),
                dto.getFermataNome(),
                dto.getCorsa(),
                dto.getLinea(),
                stop != null && stop.lat() != null ? stop.lat().doubleValue() : null,
                stop != null && stop.lon() != null ? stop.lon().doubleValue() : null,
                toIso(dto.getArrivo()),
                toIso(dto.getPartenza()),
                dto.getRitardoArrivoMin(),
                dto.getRitardoPartenzaMin()
        );
    }

    private static String toIso(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return ZonedDateTime.parse(value, ROME_TS).toInstant().toString();
        } catch (Exception e) {
            return value;
        }
    }
}
