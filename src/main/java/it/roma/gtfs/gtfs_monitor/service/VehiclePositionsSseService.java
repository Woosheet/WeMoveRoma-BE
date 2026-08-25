package it.roma.gtfs.gtfs_monitor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiListResponseDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiVehicleDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class VehiclePositionsSseService {
    private static final ZoneId ROME = ZoneId.of("Europe/Rome");
    private static final DateTimeFormatter ISO_ROME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX").withZone(ROME);

    private final GtfsIndexService gtfsIndexService;
    private final ObjectMapper objectMapper;
    private final Set<Subscription> emitters = ConcurrentHashMap.newKeySet();

    /**
     * Executor singolo dedicato all'invio SSE: emitter.send è bloccante, quindi un client
     * lento non deve mai bloccare il thread di polling GTFS-RT. Un solo thread garantisce
     * anche l'ordine sequenziale degli eventi verso ogni emitter.
     */
    private final ExecutorService sendExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "vehicle-positions-sse-send");
        t.setDaemon(true);
        return t;
    });

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

        // Snapshot iniziale sullo stesso executor dei tick: ordine sequenziale garantito.
        enqueue(() -> {
            if (!sendSnapshot(subscription, snapshot, generatedAt)) {
                emitters.remove(subscription);
            }
        });
        return emitter;
    }

    public void publish(List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        if (emitters.isEmpty()) {
            return;
        }
        List<Subscription> targets = List.copyOf(emitters);
        // L'invio (mapping DTO + serializzazione JSON + send bloccante) avviene sull'executor
        // dedicato: il chiamante (thread di polling) ritorna subito.
        enqueue(() -> dispatch(targets, snapshot, generatedAt));
    }

    @PreDestroy
    public void shutdown() {
        sendExecutor.shutdownNow();
    }

    private void enqueue(Runnable task) {
        try {
            sendExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            log.debug("[VehiclePositionsSse] executor spento, invio scartato: {}", e.toString());
        }
    }

    private void dispatch(List<Subscription> targets, List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        Map<String, String> normalizedCapolineaCache = buildNormalizedCapolineaCache(targets, snapshot);
        // Un solo payload serializzato per gruppo di filtri identici: nel caso comune
        // (nessun filtro) tutti i subscriber condividono la stessa stringa JSON.
        Map<FilterKey, String> payloadByFilter = new HashMap<>();
        for (Subscription subscription : targets) {
            if (!emitters.contains(subscription)) {
                continue; // rimosso nel frattempo (completion/timeout/errore)
            }
            FilterKey key = subscription.filterKey();
            String json = payloadByFilter.get(key);
            if (json == null) {
                json = toJson(toApiResponse(
                        filterSnapshot(snapshot, subscription, generatedAt, normalizedCapolineaCache),
                        generatedAt));
                if (json == null) {
                    continue; // errore di serializzazione già loggato
                }
                payloadByFilter.put(key, json);
            }
            if (!sendJson(subscription, json)) {
                emitters.remove(subscription);
            }
        }
    }

    /**
     * Precalcola una volta per tick la normalizzazione (regex NFD) del capolinea di ogni
     * veicolo, invece di rifarla per ogni subscriber. Costruita solo se serve davvero.
     */
    private Map<String, String> buildNormalizedCapolineaCache(List<Subscription> targets, List<VehiclePositionDTO> snapshot) {
        boolean needed = targets.stream().anyMatch(s -> s.normalizedDestination() != null);
        if (!needed) {
            return Map.of();
        }
        Map<String, String> cache = new HashMap<>();
        for (VehiclePositionDTO dto : snapshot) {
            String capolinea = dto.getCapolinea();
            if (capolinea != null && !cache.containsKey(capolinea)) {
                cache.put(capolinea, normalizeText(capolinea));
            }
        }
        return cache;
    }

    private boolean sendSnapshot(Subscription subscription, List<VehiclePositionDTO> snapshot, Instant generatedAt) {
        String json = toJson(toApiResponse(filterSnapshot(snapshot, subscription, generatedAt), generatedAt));
        if (json == null) {
            return true; // errore di serializzazione già loggato, l'emitter resta attivo
        }
        return sendJson(subscription, json);
    }

    private boolean sendJson(Subscription subscription, String json) {
        try {
            subscription.emitter().send(SseEmitter.event()
                    .name("vehicles")
                    .data(json, MediaType.APPLICATION_JSON));
            return true;
        } catch (Exception e) {
            try {
                subscription.emitter().completeWithError(e);
            } catch (Exception ignored) {
                // no-op
            }
            log.debug("[VehiclePositionsSse] emitter closed: {}", e.toString());
            return false;
        }
    }

    private String toJson(ApiListResponseDTO<ApiVehicleDTO> response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            log.error("[VehiclePositionsSse] serializzazione payload SSE fallita: {}", e.toString());
            return null;
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
        return filterSnapshot(snapshot, subscription, generatedAt, Map.of());
    }

    private List<VehiclePositionDTO> filterSnapshot(
            List<VehiclePositionDTO> snapshot,
            Subscription subscription,
            Instant generatedAt,
            Map<String, String> normalizedCapolineaCache
    ) {
        List<VehiclePositionDTO> filtered = snapshot.stream().filter(dto -> {
            if (subscription.vehicleId() != null && !subscription.vehicleId().isBlank()) {
                if (!subscription.vehicleId().equals(dto.getVeicolo())) return false;
            }
            if (subscription.linea() != null && !subscription.linea().isBlank()) {
                if (!gtfsIndexService.matchesLine(subscription.linea(), dto.getLinea())) return false;
            }
            if (subscription.normalizedDestination() != null) {
                if (!subscription.normalizedDestination().equals(
                        normalizedCapolinea(dto.getCapolinea(), normalizedCapolineaCache))) return false;
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

    private static String normalizedCapolinea(String capolinea, Map<String, String> cache) {
        if (capolinea == null) {
            return null;
        }
        String cached = cache.get(capolinea);
        return cached != null ? cached : normalizeText(capolinea);
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
    ) {
        private FilterKey filterKey() {
            return new FilterKey(linea, normalizedDestination, vehicleId);
        }
    }

    /** Chiave di raggruppamento: subscriber con filtri identici condividono lo stesso payload JSON. */
    private record FilterKey(String linea, String normalizedDestination, String vehicleId) {}

    private record Position(double lat, double lon) {}
}
