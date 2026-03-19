package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.JourneyPlanResponseDTO;
import it.roma.gtfs.gtfs_monitor.service.JourneyPlannerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/journey")
public class ApiJourneyController {
    private final JourneyPlannerService journeyPlannerService;

    @GetMapping("/plan")
    public JourneyPlanResponseDTO plan(
            @RequestParam double fromLat,
            @RequestParam double fromLon,
            @RequestParam double toLat,
            @RequestParam double toLon,
            @RequestParam(required = false) String fromLabel,
            @RequestParam(required = false) String toLabel,
            @RequestParam(required = false) Integer numItineraries,
            @RequestParam(required = false) String timeMode,
            @RequestParam(required = false) String when,
            @RequestParam(required = false) String modes
    ) {
        log.debug("GET /api/v1/journey/plan from={},{} to={},{}", fromLat, fromLon, toLat, toLon);
        return journeyPlannerService.plan(fromLat, fromLon, fromLabel, toLat, toLon, toLabel, numItineraries, timeMode, when, modes);
    }
}
