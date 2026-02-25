package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import it.roma.gtfs.gtfs_monitor.service.VehiclePositionsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
public class VehiclePositionsController {

    private final VehiclePositionsService service;

    @GetMapping("/vehicle-positions")
    public List<VehiclePositionDTO> getVehiclePositions(
            @RequestParam( required = false) String routeId,
            @RequestParam( required = false) Integer limit
    ) {
        log.debug("GET /vehicle-positions routeId={} limit={}", routeId, limit);
        return service.fetch(routeId, limit);
    }
}
