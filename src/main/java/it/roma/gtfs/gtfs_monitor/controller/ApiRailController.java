package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.RailTrainInfoDTO;
import it.roma.gtfs.gtfs_monitor.service.RailTrainInfoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/rail")
public class ApiRailController {
    private final RailTrainInfoService railTrainInfoService;

    @GetMapping("/train-info")
    public ResponseEntity<RailTrainInfoDTO> trainInfo(
            @RequestParam String trainNumber,
            @RequestParam(required = false) String stationName,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime referenceTime
    ) {
        log.debug("GET /api/v1/rail/train-info trainNumber={} stationName={} referenceTime={}", trainNumber, stationName, referenceTime);
        return railTrainInfoService.lookupTrainInfo(trainNumber, stationName, referenceTime)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }
}
