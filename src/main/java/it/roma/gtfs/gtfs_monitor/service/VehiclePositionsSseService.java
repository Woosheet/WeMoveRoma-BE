package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiListResponseDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiVehicleDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class VehiclePositionsSseService {
    private final GtfsIndexService gtfsIndexService;
    private final Set<SseEmitter> emitters = ConcurrentHashMap.newKeySet();

    public SseEmitter subscribe(List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        SseEmitter emitter = new SseEmitter(0L);
        emitters.add(emitter);

        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> {
            emitters.remove(emitter);
            emitter.complete();
        });
        emitter.onError((ex) -> emitters.remove(emitter));

        sendSnapshot(emitter, snapshot, generatedAt);
        return emitter;
    }

    public void publish(List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        if (emitters.isEmpty()) {
            return;
        }
        for (SseEmitter emitter : List.copyOf(emitters)) {
            if (!sendSnapshot(emitter, snapshot, generatedAt)) {
                emitters.remove(emitter);
            }
        }
    }

    private boolean sendSnapshot(SseEmitter emitter, List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        try {
            emitter.send(SseEmitter.event()
                    .name("vehicles")
                    .data(toApiResponse(snapshot, generatedAt)));
            return true;
        } catch (IOException e) {
            try {
                emitter.completeWithError(e);
            } catch (Exception ignored) {
                // no-op
            }
            log.debug("[VehiclePositionsSse] emitter closed: {}", e.toString());
            return false;
        }
    }

    private ApiListResponseDTO<ApiVehicleDTO> toApiResponse(List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        List<ApiVehicleDTO> items = snapshot.stream().map(this::toApiDto).toList();
        return new ApiListResponseDTO<>(items, items.size(), generatedAt != null ? generatedAt : Instant.now());
    }

    private ApiVehicleDTO toApiDto(VehiclePositionDTO dto) {
        return new ApiVehicleDTO(
                dto.getVeicolo(),
                gtfsIndexService.publicLineByRouteId(dto.getLinea()),
                dto.getCapolinea(),
                dto.getCorsa(),
                dto.getLat(),
                dto.getLon(),
                dto.getVelocitaKmh(),
                dto.getTimestamp()
        );
    }
}
