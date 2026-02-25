package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.SearchSuggestionDTO;
import it.roma.gtfs.gtfs_monitor.service.SearchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/search")
public class ApiSearchController {

    private final SearchService searchService;

    @GetMapping("/suggestions")
    public List<SearchSuggestionDTO> suggestions(
            @RequestParam String q,
            @RequestParam(required = false) Integer limit
    ) {
        log.debug("GET /api/v1/search/suggestions q={} limit={}", q, limit);
        return searchService.suggestions(q, limit);
    }
}
