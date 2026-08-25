package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiAlertDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiListResponseDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ServiceAlertDTO;
import it.roma.gtfs.gtfs_monitor.service.AlertSeverityResolver;
import it.roma.gtfs.gtfs_monitor.service.ServiceAlertsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/alerts")
public class ApiAlertsController {

    private static final ZoneId ROME = ZoneId.of("Europe/Rome");

    private final ServiceAlertsService serviceAlertsService;

    @GetMapping
    public ApiListResponseDTO<ApiAlertDTO> getAlerts(
            @RequestParam(required = false) String linea,
            @RequestParam(required = false, defaultValue = "true") Boolean active,
            @RequestParam(required = false) Integer limit
    ) {
        log.debug("GET /api/v1/alerts linea={} active={} limit={}", linea, active, limit);
        List<ServiceAlertDTO> raw = Boolean.FALSE.equals(active)
                ? serviceAlertsService.fetch(limit, linea, null, null, LocalDate.now(ROME), LocalDate.now(ROME))
                : serviceAlertsService.fetchActiveNow(linea, limit);

        List<ApiAlertDTO> items = raw.stream().map(ApiAlertsController::toApiDto).toList();
        return new ApiListResponseDTO<>(items, items.size(), Instant.now());
    }

    private static ApiAlertDTO toApiDto(ServiceAlertDTO dto) {
        // Un avviso puo' riguardare piu' linee: esporne una sola (com'era prima)
        // faceva perdere il 75% delle associazioni linea-avviso, e le fermate o
        // gli itinerari sulle linee "scartate" non mostravano nulla.
        List<String> lines = dto.getRouteIds() == null ? List.of() : List.copyOf(dto.getRouteIds());
        String line = lines.isEmpty() ? null : lines.getFirst();
        // Il feed di Roma non popola mai severity_level: senza derivarla dall'effetto
        // ogni avviso resterebbe "info" e un servizio sospeso peserebbe come una nota.
        String declaredSeverity = dto.getSeverita();
        return new ApiAlertDTO(
                dto.getId(),
                line,
                lines,
                AlertSeverityResolver.resolve(declaredSeverity, dto.getEffettoCodice()),
                AlertSeverityResolver.source(declaredSeverity),
                declaredSeverity,
                dto.getCausa(),
                dto.getEffetto(),
                dto.getCausaCodice(),
                dto.getEffettoCodice(),
                dto.getTitolo(),
                dto.getDescrizione(),
                dto.getInizio(),
                dto.getFine(),
                Instant.now()
        );
    }
}
