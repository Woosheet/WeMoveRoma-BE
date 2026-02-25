package it.roma.gtfs.gtfs_monitor.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.text.Normalizer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@RequiredArgsConstructor
public class VehiclePositionsService {
    private static final long DEFAULT_REFRESH_MILLIS = 5_000L;
    private static final int MAX_LIMIT = 5_000;

    private static final ZoneId ROME = ZoneId.of("Europe/Rome");
    private static final DateTimeFormatter ISO_ROME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss XXX").withZone(ROME);

    private final GtfsProperties props;
    private final WebClient webClient;
    private final GtfsIndexService indexService;
    private final VehiclePositionsSseService vehiclePositionsSseService;
    private final AtomicReference<CacheEntry<List<VehiclePositionDTO>>> cacheRef =
            new AtomicReference<>(CacheEntry.empty());
    private final Object refreshLock = new Object();


    /**
     * @param linea opzionale: filtra per route_id (match esatto)
     * @param limit opzionale: max righe (<=0 o null = nessun limite)
     */
    public List<VehiclePositionDTO> fetch(String linea, Integer limit) {
        return fetch(linea, null, limit);
    }

    public List<VehiclePositionDTO> fetch(String linea, String capolinea, Integer limit) {
        List<VehiclePositionDTO> base = getCachedOrRefresh();
        if (base.isEmpty()) {
            return List.of();
        }

        String normalizedCapolinea = normalizeText(capolinea);
        int max = normalizeLimit(limit);
        List<VehiclePositionDTO> out = new ArrayList<>(max > 0 ? Math.min(max, base.size()) : base.size());
        for (VehiclePositionDTO dto : base) {
            if (linea != null && !indexService.matchesLine(linea, dto.getLinea())) {
                continue;
            }
            if (normalizedCapolinea != null && !Objects.equals(normalizeText(dto.getCapolinea()), normalizedCapolinea)) {
                continue;
            }
            out.add(dto);
            if (max > 0 && out.size() >= max) {
                break;
            }
        }
        return out;
    }

    public Instant lastSnapshotAt() {
        CacheEntry<List<VehiclePositionDTO>> current = cacheRef.get();
        if (isExpired(current)) {
            refreshCache(false);
            current = cacheRef.get();
        }
        return current.isEmpty() ? null : Instant.ofEpochMilli(current.loadedAtMillis());
    }

    @Scheduled(
            fixedDelayString = "${gtfs.realtime.refresh-millis:5000}",
            initialDelayString = "${gtfs.realtime.refresh-millis:5000}"
    )
    public void scheduledRefresh() {
        refreshCache(true);
    }

    private List<VehiclePositionDTO> getCachedOrRefresh() {
        CacheEntry<List<VehiclePositionDTO>> current = cacheRef.get();
        if (!isExpired(current)) {
            return current.data();
        }
        refreshCache(false);
        return cacheRef.get().data();
    }

    private void refreshCache(boolean force) {
        synchronized (refreshLock) {
            CacheEntry<List<VehiclePositionDTO>> current = cacheRef.get();
            if (!force && !isExpired(current)) {
                return;
            }
            try {
                List<VehiclePositionDTO> fresh = fetchRemoteSnapshot();
                if (!fresh.isEmpty() || current.isEmpty()) {
                    long loadedAt = System.currentTimeMillis();
                    List<VehiclePositionDTO> immutable = List.copyOf(fresh);
                    cacheRef.set(new CacheEntry<>(immutable, loadedAt));
                    vehiclePositionsSseService.publish(immutable, Instant.ofEpochMilli(loadedAt));
                }
            } catch (Exception e) {
                log.warn("[VehiclePositions] refresh cache fallito: {}", e.toString());
            }
        }
    }

    private List<VehiclePositionDTO> fetchRemoteSnapshot() {
        String url = Objects.requireNonNull(props.realtime().vehiclePositionsUrl(),
                "gtfs.realtime.vehiclePositionsUrl mancante");

        byte[] body = webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(byte[].class)
                .block();

        if (body == null || body.length == 0) {
            log.warn("[VehiclePositions] body vuoto da {}", url);
            return List.of();
        }

        try {
            var feed = GtfsRealtime.FeedMessage.parseFrom(body);
            List<VehiclePositionDTO> out = new ArrayList<>(Math.max(32, feed.getEntityCount()));

            for (var ent : feed.getEntityList()) {
                if (!ent.hasVehicle()) continue;
                var v = ent.getVehicle();

                String routeId = v.hasTrip() ? v.getTrip().getRouteId() : null;
                String tripId  = v.hasTrip() ? v.getTrip().getTripId()  : null;
                String vehId   = v.hasVehicle() ? v.getVehicle().getId() : null;

                Double lat = (v.hasPosition() && v.getPosition().hasLatitude()) ? (double) v.getPosition().getLatitude() : null;
                Double lon = (v.hasPosition() && v.getPosition().hasLongitude()) ? (double) v.getPosition().getLongitude() : null;

                Double kmh = null;
                if (v.hasPosition() && v.getPosition().hasSpeed()) {
                    kmh = round3(v.getPosition().getSpeed());
                }

                String ts = null;
                if (v.hasTimestamp()) {
                    ts = ISO_ROME.format(Instant.ofEpochSecond(v.getTimestamp()));
                }

                GtfsIndexService.Trip trip = indexService.tripByIdOrNull(tripId);
                String capolinea = (trip != null) ? trip.headsign() : null;

                out.add(VehiclePositionDTO.builder()
                        .linea(routeId)
                        .corsa(tripId)
                        .veicolo(vehId)
                        .lat(lat)
                        .lon(lon)
                        .velocitaKmh(kmh)
                        .timestamp(ts)
                        .capolinea(capolinea)
                        .build());
            }
            return out;

        } catch (InvalidProtocolBufferException pe) {
            log.error("[VehiclePositions] feed protobuf corrotto: {}", pe.toString());
            return List.of();
        } catch (Exception e) {
            log.error("[VehiclePositions] parse error: {}", e.toString());
            return List.of();
        }
    }

    private boolean isExpired(CacheEntry<?> entry) {
        return entry.isEmpty() || (System.currentTimeMillis() - entry.loadedAtMillis()) > refreshMillis();
    }

    private long refreshMillis() {
        long configured = props.realtime() != null ? props.realtime().refreshMillis() : 0L;
        return configured > 0 ? configured : DEFAULT_REFRESH_MILLIS;
    }

    private static int normalizeLimit(Integer limit) {
        if (limit == null || limit <= 0) return -1;
        return Math.min(limit, MAX_LIMIT);
    }

    private static double round3(double value) {
        return Math.round(value * 1000.0d) / 1000.0d;
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

    private record CacheEntry<T>(T data, long loadedAtMillis) {
        private static <T> CacheEntry<T> empty() {
            return new CacheEntry<>(null, 0L);
        }

        private boolean isEmpty() {
            return data == null;
        }
    }
}
