package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.StrikeDTO;
import it.roma.gtfs.gtfs_monitor.service.StrikeService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/strikes")
@RequiredArgsConstructor
public class ApiStrikesController {

    private final StrikeService strikeService;

    /** Lista degli scioperi rilevanti (gia' filtrati per settore+regione lato server). */
    @GetMapping
    public List<StrikeDTO> list() {
        return strikeService.snapshot();
    }

    /** Force-refresh manuale (utile per debug). Non protetto: aggiungere auth se serve. */
    @PostMapping("/refresh")
    public List<StrikeDTO> refresh() {
        return strikeService.refreshNow();
    }
}
