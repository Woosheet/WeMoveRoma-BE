package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.config.ResourceNotFoundException;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiLinePatternDTO;
import it.roma.gtfs.gtfs_monitor.service.GtfsIndexService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.Comparator;
import java.time.LocalDate;
import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/catalog")
public class ApiCatalogController {

    private final GtfsIndexService gtfsIndexService;

    @GetMapping("/lines")
    public List<String> lines() {
        List<String> out = gtfsIndexService.publicLines().stream()
                .filter(s -> s != null && !s.isBlank())
                .sorted(lineComparator())
                .toList();
        log.debug("GET /api/v1/catalog/lines -> {}", out.size());
        return out;
    }

    /**
     * Percorso della linea: le fermate in ordine, una sequenza per direzione.
     *
     * Alimenta le pagine pubbliche per linea. Restituisce 404 se la linea non
     * esiste nel feed: un elenco vuoto sarebbe indistinguibile da una linea
     * reale senza corse attive.
     */
    @GetMapping("/lines/{line}/pattern")
    public ApiLinePatternDTO linePattern(
            @PathVariable String line,
            /**
             * Giornata di servizio per gli orari. Assente: nessun orario, solo il
             * percorso — che e' l'unica parte davvero indipendente dalla data.
             */
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        // Con gli indici ancora vuoti nessuna linea esiste: 503, non 404 (stessa
        // convenzione di ApiStopsController e ApiTripsController).
        if (!gtfsIndexService.isStaticDataLoaded()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Dati GTFS non ancora caricati, riprovare tra qualche istante");
        }
        if (gtfsIndexService.routeIdsByPublicLine(line).isEmpty()) {
            throw new ResourceNotFoundException("Linea", line);
        }

        List<ApiLinePatternDTO.ApiLineDirectionDTO> direzioni = gtfsIndexService.linePatterns(line, date).stream()
                .map(p -> new ApiLinePatternDTO.ApiLineDirectionDTO(
                        p.directionId(),
                        p.headsign(),
                        p.stops().size(),
                        p.stops().stream()
                                .map(s -> new ApiLinePatternDTO.ApiLineStopDTO(
                                        s.stopId(), s.stopName(), s.lat(), s.lon()))
                                .toList(),
                        p.schedule() == null ? null : new ApiLinePatternDTO.ApiLineScheduleDTO(
                                p.schedule().serviceDate().toString(),
                                ApiLinePatternDTO.ApiLineScheduleDTO.orario(p.schedule().firstDepartureSeconds()),
                                ApiLinePatternDTO.ApiLineScheduleDTO.orario(p.schedule().lastDepartureSeconds()),
                                p.schedule().tripCount(),
                                p.schedule().typicalHeadwayMinutes()),
                        // La corsa campione porta con se' il proprio tracciato: e'
                        // quello che permette di disegnare il percorso sulle strade.
                        gtfsIndexService.shapeByTripId(p.sampleTripId()).stream()
                                .map(sp -> ApiLinePatternDTO.ApiLatLonDTO.of(sp.lat(), sp.lon()))
                                .toList()))
                .toList();
        log.debug("GET /api/v1/catalog/lines/{}/pattern -> {} direzioni", line, direzioni.size());
        return new ApiLinePatternDTO(line, gtfsIndexService.modeForLine(line), direzioni);
    }

    @GetMapping("/destinations")
    public List<String> destinations(@RequestParam String linea) {
        List<String> out = gtfsIndexService.destinationsByPublicLine(linea).stream()
                .filter(s -> s != null && !s.isBlank())
                .sorted(String.CASE_INSENSITIVE_ORDER)
                .toList();
        log.debug("GET /api/v1/catalog/destinations linea={} -> {}", linea, out.size());
        return out;
    }

    private static Comparator<String> lineComparator() {
        return (a, b) -> {
            boolean aNum = a.chars().allMatch(Character::isDigit);
            boolean bNum = b.chars().allMatch(Character::isDigit);
            if (aNum && bNum) {
                return Integer.compare(Integer.parseInt(a), Integer.parseInt(b));
            }
            if (aNum != bNum) return aNum ? -1 : 1;
            return String.CASE_INSENSITIVE_ORDER.compare(a, b);
        };
    }
}
