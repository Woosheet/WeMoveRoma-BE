package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;
import it.roma.gtfs.gtfs_monitor.service.TripUpdatesService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/trip-updates")
@RequiredArgsConstructor
public class TripUpdatesController {

    private final TripUpdatesService svc;

    @GetMapping
    public List<TripUpdateDTO> getTripUpdates(
            @RequestParam(required = false) String routeId,
            @RequestParam(required = false) Integer limit
    ) {
        log.debug("GET /trip-updates routeId={} limit={}", routeId, limit);

        return svc.fetch(routeId, limit);
    }
}
