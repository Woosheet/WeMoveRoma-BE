package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.ServiceAlertDTO;
import it.roma.gtfs.gtfs_monitor.service.ServiceAlertsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/service-alerts")
public class ServiceAlertsController {

    private final ServiceAlertsService service;

    @GetMapping
    public List<ServiceAlertDTO> getAlerts(
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) String routeId,
            @RequestParam(required = false) String tripId,
            @RequestParam(required = false) String stopId,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to
    ) {
        log.debug("GET /service-alerts limit={} routeId={} tripId={} stopId={} from={} to={}",
                limit, routeId, tripId, stopId, from, to);

        return service.fetch(limit, routeId, tripId, stopId, from , to);
    }
}
