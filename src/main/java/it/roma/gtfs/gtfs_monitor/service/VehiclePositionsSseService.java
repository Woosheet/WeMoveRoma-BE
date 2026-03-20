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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Locale;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class VehiclePositionsSseService {
    private static final ZoneId ROME = ZoneId.of("Europe/Rome");
    private static final DateTimeFormatter ISO_ROME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX").withZone(ROME);

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
                    .data(toApiResponse(filterSnapshot(snapshot, subscription, generatedAt), generatedAt)));
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

    private List<VehiclePositionDTO> filterSnapshot(List<VehiclePositionDTO> snapshot, Subscription subscription, Instant generatedAt) {
        List<VehiclePositionDTO> filtered = snapshot.stream().filter(dto -> {
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

        if (!filtered.isEmpty()) {
            return filtered;
        }
        if (subscription.vehicleId() != null && !subscription.vehicleId().isBlank()) {
            if (subscription.vehicleId().startsWith("sim-")) {
                VehiclePositionDTO predicted = simulateVehicleByTripId(subscription.vehicleId().substring(4), generatedAt);
                return predicted != null ? List.of(predicted) : List.of();
            }
            return List.of();
        }
        if (subscription.linea() == null || subscription.linea().isBlank()) {
            return List.of();
        }
        return simulateVehicles(subscription.linea(), subscription.normalizedDestination(), generatedAt, 1);
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

    private List<VehiclePositionDTO> simulateVehicles(String linea, String destination, Instant generatedAt, int limit) {
        Instant now = generatedAt != null ? generatedAt : Instant.now();
        List<VehiclePositionDTO> out = new ArrayList<>();
        List<GtfsIndexService.SimulatedTrip> simulatedTrips = gtfsIndexService.simulatedTrips(linea, destination, now, limit);
        if (simulatedTrips.isEmpty() && destination != null && !destination.isBlank()) {
            simulatedTrips = gtfsIndexService.simulatedTrips(linea, null, now, limit);
        }

        for (GtfsIndexService.SimulatedTrip simulated : simulatedTrips) {
            VehiclePositionDTO dto = toPredictedVehicle(simulated, now);
            if (dto != null) {
                out.add(dto);
            }
        }

        return List.copyOf(out);
    }

    private VehiclePositionDTO simulateVehicleByTripId(String tripId, Instant generatedAt) {
        Instant now = generatedAt != null ? generatedAt : Instant.now();
        return gtfsIndexService.simulatedTripById(tripId, now)
                .map(simulated -> toPredictedVehicle(simulated, now))
                .orElse(null);
    }

    private VehiclePositionDTO toPredictedVehicle(GtfsIndexService.SimulatedTrip simulated, Instant now) {
        long nowEpochSeconds = now.getEpochSecond();
        double progress = tripProgress(nowEpochSeconds, simulated.startEpochSeconds(), simulated.endEpochSeconds());
        Position position = interpolateOnShape(simulated.shape(), progress);
        if (position == null) {
            return null;
        }

        return VehiclePositionDTO.builder()
                .linea(simulated.line())
                .corsa(simulated.tripId())
                .veicolo("sim-" + simulated.tripId())
                .lat(position.lat())
                .lon(position.lon())
                .velocitaKmh(null)
                .timestamp(ISO_ROME.format(now))
                .capolinea(simulated.destination())
                .occupancyStatus(null)
                .wheelchairAccessible(simulated.wheelchairAccessible())
                .build();
    }

    private static double tripProgress(long nowEpochSeconds, long startEpochSeconds, long endEpochSeconds) {
        if (endEpochSeconds <= startEpochSeconds) return 0d;
        double raw = (double) (nowEpochSeconds - startEpochSeconds) / (double) (endEpochSeconds - startEpochSeconds);
        return Math.max(0d, Math.min(1d, raw));
    }

    private static Position interpolateOnShape(List<GtfsIndexService.ShapePoint> shape, double progress) {
        if (shape == null || shape.isEmpty()) return null;
        if (shape.size() == 1) {
            GtfsIndexService.ShapePoint point = shape.get(0);
            return new Position(point.lat(), point.lon());
        }

        double scaled = progress * (shape.size() - 1);
        int lowerIndex = (int) Math.floor(scaled);
        int upperIndex = Math.min(shape.size() - 1, lowerIndex + 1);
        double fraction = scaled - lowerIndex;

        GtfsIndexService.ShapePoint a = shape.get(lowerIndex);
        GtfsIndexService.ShapePoint b = shape.get(upperIndex);
        double lat = a.lat() + (b.lat() - a.lat()) * fraction;
        double lon = a.lon() + (b.lon() - a.lon()) * fraction;
        return new Position(lat, lon);
    }

    private record Subscription(
            SseEmitter emitter,
            String linea,
            String normalizedDestination,
            String vehicleId
    ) {}

    private record Position(double lat, double lon) {}
}
