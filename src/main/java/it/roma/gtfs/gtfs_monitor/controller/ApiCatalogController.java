package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.service.GtfsIndexService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Comparator;
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
