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
import java.text.Normalizer;
import java.util.Locale;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class VehiclePositionsSseService {
    private final GtfsIndexService gtfsIndexService;
    private final Set<Subscription> emitters = ConcurrentHashMap.newKeySet();

    public SseEmitter subscribe(List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        return subscribe(snapshot, generatedAt, null, null, null);
    }

    public SseEmitter subscribe(
            List<VehiclePositionDTO> snapshot,
            Instant generatedAt,
            String linea,
            String destination,
            String vehicleId
    ) {
        SseEmitter emitter = new SseEmitter(0L);
        Subscription subscription = new Subscription(emitter, linea, normalizeText(destination), vehicleId);
        emitters.add(subscription);

        emitter.onCompletion(() -> emitters.remove(subscription));
        emitter.onTimeout(() -> {
            emitters.remove(subscription);
            emitter.complete();
        });
        emitter.onError((ex) -> emitters.remove(subscription));

        sendSnapshot(subscription, snapshot, generatedAt);
        return emitter;
    }

    public void publish(List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        if (emitters.isEmpty()) {
            return;
        }
        for (Subscription subscription : List.copyOf(emitters)) {
            if (!sendSnapshot(subscription, snapshot, generatedAt)) {
                emitters.remove(subscription);
            }
        }
    }

    private boolean sendSnapshot(Subscription subscription, List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        try {
            subscription.emitter().send(SseEmitter.event()
                    .name("vehicles")
                    .data(toApiResponse(filterSnapshot(snapshot, subscription), generatedAt)));
            return true;
        } catch (IOException e) {
            try {
                subscription.emitter().completeWithError(e);
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
                dto.getTimestamp(),
                dto.getOccupancyStatus(),
                dto.getWheelchairAccessible()
        );
    }

    private List<VehiclePositionDTO> filterSnapshot(List<VehiclePositionDTO> snapshot, Subscription subscription) {
        return snapshot.stream().filter(dto -> {
            if (subscription.vehicleId() != null && !subscription.vehicleId().isBlank()) {
                if (!subscription.vehicleId().equals(dto.getVeicolo())) return false;
            }
            if (subscription.linea() != null && !subscription.linea().isBlank()) {
                if (!gtfsIndexService.matchesLine(subscription.linea(), dto.getLinea())) return false;
            }
            if (subscription.normalizedDestination() != null) {
                if (!subscription.normalizedDestination().equals(normalizeText(dto.getCapolinea()))) return false;
            }
            return true;
        }).toList();
    }

    private static String normalizeText(String value) {
        if (value == null) return null;
        String normalized = Normalizer.normalize(value, Normalizer.Form.NFD)
                .replaceAll("\\p{M}+", "")
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^\\p{Alnum}]+", " ")
                .trim()
                .replaceAll("\\s+", " ");
        return normalized.isEmpty() ? null : normalized;
    }

    private record Subscription(
            SseEmitter emitter,
            String linea,
            String normalizedDestination,
            String vehicleId
    ) {}
}
