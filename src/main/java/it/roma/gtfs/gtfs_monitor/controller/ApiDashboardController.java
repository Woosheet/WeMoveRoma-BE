package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.DashboardSummaryDTO;
import it.roma.gtfs.gtfs_monitor.service.DashboardService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/dashboard")
public class ApiDashboardController {

    private final DashboardService dashboardService;

    @GetMapping("/summary")
    public DashboardSummaryDTO summary(
            @RequestParam(required = false) String linea,
            @RequestParam(required = false) String destination
    ) {
        log.debug("GET /api/v1/dashboard/summary linea={} destination={}", linea, destination);
        return dashboardService.summary(linea, destination);
    }
}
