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
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

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

    /** Pausa fra un tentativo e l'altro, come in TripUpdatesService. */
    private static final long RETRY_BACKOFF_MILLIS = 400L;
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
        if (!out.isEmpty()) {
            return out;
        }
        if (linea == null || linea.isBlank()) {
            return List.of();
        }
        return simulateVehicles(linea, capolinea, 1);
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

    /**
     * Stessa gestione dei tentativi di {@code TripUpdatesService}.
     *
     * Prima il ritentativo valeva solo per il protobuf corrotto: un 502 di
     * Roma Mobilita' finiva nel catch generico, si arrendeva subito e loggava
     * ERROR. Gli aggiornamenti delle corse invece ritentavano e passavano.
     * Stesso feed, stesso singhiozzo, due comportamenti: le posizioni dei mezzi
     * saltavano un ciclo e il log si riempiva di ERROR per un guasto passeggero
     * di cui il codice sa gia' riprendersi.
     */
    private List<VehiclePositionDTO> fetchRemoteSnapshot() {
        int attempt = 1;
        while (true) {
            try {
                return fetchRemoteSnapshotOnce();
            } catch (InvalidProtocolBufferException e) {
                if (attempt >= 2) {
                    log.error("[VehiclePositions] feed protobuf corrotto anche al retry: {}", e.toString());
                    return List.of();
                }
                log.warn("[VehiclePositions] protobuf corrotto (tentativo {}), retry immediato: {}", attempt, e.toString());
            } catch (WebClientResponseException.BadGateway | WebClientRequestException e) {
                if (attempt >= 2) {
                    log.warn("[VehiclePositions] refresh cache fallito anche al retry: {}", e.toString());
                    return List.of();
                }
                log.warn("[VehiclePositions] upstream temporaneamente non disponibile (tentativo {}), retry immediato: {}", attempt, e.toString());
            } catch (WebClientResponseException.Forbidden | WebClientResponseException.TooManyRequests e) {
                /*
                 * Il filtro anti-bot davanti al feed ogni tanto respinge il poll.
                 * Non si ritenta: insistere su un rate limit lo peggiora, e il
                 * giro successivo fra 5 secondi e' gia' il tentativo dopo. La
                 * cache precedente resta servita, quindi nessuno se ne accorge.
                 */
                log.warn("[VehiclePositions] poll respinto dall'upstream, servo la cache precedente: {}", e.toString());
                return List.of();
            } catch (Exception e) {
                log.error("[VehiclePositions] fetch/parsing error: {}", e.toString());
                return List.of();
            }

            attempt++;
            sleepBeforeRetry();
        }
    }

    private static void sleepBeforeRetry() {
        try {
            Thread.sleep(RETRY_BACKOFF_MILLIS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private List<VehiclePositionDTO> fetchRemoteSnapshotOnce() throws InvalidProtocolBufferException {
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
            Boolean wheelchairAccessible = wheelchairAccessible(trip);
            String occupancyStatus = v.hasOccupancyStatus() ? v.getOccupancyStatus().name() : null;

            out.add(VehiclePositionDTO.builder()
                    .linea(routeId)
                    .corsa(tripId)
                    .veicolo(vehId)
                    .lat(lat)
                    .lon(lon)
                    .velocitaKmh(kmh)
                    .timestamp(ts)
                    .capolinea(capolinea)
                    .occupancyStatus(occupancyStatus)
                    .wheelchairAccessible(wheelchairAccessible)
                    .build());
        }
        return out;
    }

    private static Boolean wheelchairAccessible(GtfsIndexService.Trip trip) {
        if (trip == null || trip.wheelchair() == null) return null;
        if (trip.wheelchair() == 1) return Boolean.TRUE;
        if (trip.wheelchair() == 2) return Boolean.FALSE;
        return null;
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

    private List<VehiclePositionDTO> simulateVehicles(String linea, String capolinea, int limit) {
        long nowEpochSeconds = Instant.now().getEpochSecond();
        String timestamp = ISO_ROME.format(Instant.ofEpochSecond(nowEpochSeconds));
        List<VehiclePositionDTO> out = new ArrayList<>();
        Instant now = Instant.ofEpochSecond(nowEpochSeconds);
        List<GtfsIndexService.SimulatedTrip> simulatedTrips = indexService.simulatedTrips(linea, capolinea, now, limit);
        if (simulatedTrips.isEmpty() && capolinea != null && !capolinea.isBlank()) {
            simulatedTrips = indexService.simulatedTrips(linea, null, now, limit);
        }

        for (GtfsIndexService.SimulatedTrip simulated : simulatedTrips) {
            double progress = tripProgress(nowEpochSeconds, simulated.startEpochSeconds(), simulated.endEpochSeconds());
            Position pos = interpolateOnShape(simulated.shape(), progress);
            if (pos == null) continue;

            out.add(VehiclePositionDTO.builder()
                    .linea(simulated.line())
                    .corsa(simulated.tripId())
                    .veicolo("sim-" + simulated.tripId())
                    .lat(pos.lat())
                    .lon(pos.lon())
                    .velocitaKmh(null)
                    .timestamp(timestamp)
                    .capolinea(simulated.destination())
                    .occupancyStatus(null)
                    .wheelchairAccessible(simulated.wheelchairAccessible())
                    .build());
        }
        return List.copyOf(out);
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

    private record CacheEntry<T>(T data, long loadedAtMillis) {
        private static <T> CacheEntry<T> empty() {
            return new CacheEntry<>(null, 0L);
        }

        private boolean isEmpty() {
            return data == null;
        }
    }

    private record Position(double lat, double lon) {}
}
