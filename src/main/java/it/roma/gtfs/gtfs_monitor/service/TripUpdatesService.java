package it.roma.gtfs.gtfs_monitor.service;

import com.google.transit.realtime.GtfsRealtime;
import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static it.roma.gtfs.gtfs_monitor.utils.DelayFmt.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class TripUpdatesService {
    private static final long DEFAULT_REFRESH_MILLIS = 5_000L;
    private static final int MAX_LIMIT = 5_000;

    private final GtfsProperties props;
    private final WebClient webClient;
    private final GtfsIndexService indexService;
    private final AtomicReference<CacheEntry<List<TripUpdateDTO>>> cacheRef = new AtomicReference<>(CacheEntry.empty());
    private final Object refreshLock = new Object();
    private static final DateTimeFormatter ROME_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");

    public List<TripUpdateDTO> fetch(String linea, Integer limit) {
        List<TripUpdateDTO> base = getCachedOrRefresh();
        if (base.isEmpty()) {
            return List.of();
        }

        int max = normalizeLimit(limit);
        List<TripUpdateDTO> out = new ArrayList<>(max > 0 ? Math.min(max, base.size()) : base.size());
        for (TripUpdateDTO dto : base) {
            if (linea != null && !Objects.equals(dto.getLinea(), linea)) {
                continue;
            }
            out.add(dto);
            if (max > 0 && out.size() >= max) {
                break;
            }
        }
        return out;
    }

    public long countDelayedVehicles(String linea) {
        List<TripUpdateDTO> items = fetch(linea, null);
        Set<String> delayed = new HashSet<>();
        for (TripUpdateDTO dto : items) {
            Double arr = dto.getRitardoArrivoMin();
            Double dep = dto.getRitardoPartenzaMin();
            boolean hasDelay = (arr != null && arr > 0d) || (dep != null && dep > 0d);
            if (!hasDelay) continue;

            String key = firstNonBlank(dto.getVeicolo(), dto.getCorsa());
            if (key != null) {
                delayed.add(key);
            }
        }
        return delayed.size();
    }

    public List<TripUpdateDTO> nextStopsByVehicle(String vehicleId, Integer limit) {
        if (vehicleId == null || vehicleId.isBlank()) return List.of();

        String target = vehicleId.trim();
        long now = System.currentTimeMillis();
        int max = normalizeLimit(limit);

        List<TripUpdateDTO> base = getCachedOrRefresh();
        if (base.isEmpty()) return List.of();

        List<TripUpdateDTO> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (TripUpdateDTO dto : base) {
            if (!target.equals(dto.getVeicolo())) continue;
            Long bestTimeMs = bestTimeMillis(dto);
            if (bestTimeMs != null && bestTimeMs < (now - 5 * 60_000L)) continue;

            String dedupKey = (dto.getCorsa() == null ? "" : dto.getCorsa()) + "|" +
                    (dto.getFermataId() == null ? "" : dto.getFermataId()) + "|" +
                    (bestTimeMs == null ? "" : bestTimeMs);
            if (!seen.add(dedupKey)) continue;
            out.add(dto);
        }

        out.sort(Comparator.comparingLong(dto -> Optional.ofNullable(bestTimeMillis(dto)).orElse(Long.MAX_VALUE)));
        if (max > 0 && out.size() > max) {
            return List.copyOf(out.subList(0, max));
        }
        return List.copyOf(out);
    }

    @Scheduled(
            fixedDelayString = "${gtfs.realtime.refresh-millis:5000}",
            initialDelayString = "${gtfs.realtime.refresh-millis:5000}"
    )
    public void scheduledRefresh() {
        refreshCache(true);
    }

    private List<TripUpdateDTO> getCachedOrRefresh() {
        CacheEntry<List<TripUpdateDTO>> current = cacheRef.get();
        if (!isExpired(current)) {
            return current.data();
        }
        refreshCache(false);
        return cacheRef.get().data();
    }

    private void refreshCache(boolean force) {
        synchronized (refreshLock) {
            CacheEntry<List<TripUpdateDTO>> current = cacheRef.get();
            if (!force && !isExpired(current)) {
                return;
            }
            try {
                List<TripUpdateDTO> fresh = fetchRemoteSnapshot();
                if (!fresh.isEmpty() || current.isEmpty()) {
                    cacheRef.set(new CacheEntry<>(List.copyOf(fresh), System.currentTimeMillis()));
                }
            } catch (Exception e) {
                log.warn("[TripUpdates] refresh cache fallito: {}", e.toString());
            }
        }
    }

    private List<TripUpdateDTO> fetchRemoteSnapshot() {
        String url = props.realtime().tripUpdatesUrl();
        byte[] body = webClient.get().uri(url)
                .retrieve()
                .bodyToMono(byte[].class)
                .block();

        if (body == null || body.length == 0) {
            log.warn("[TripUpdates] body vuoto da {}", url);
            return List.of();
        }

        try {
            var feed = GtfsRealtime.FeedMessage.parseFrom(body);
            List<TripUpdateDTO> out = new ArrayList<>(Math.max(32, feed.getEntityCount()));

            for (var ent : feed.getEntityList()) {
                if (!ent.hasTripUpdate()) continue;

                var tu = ent.getTripUpdate();
                String routeId = tu.getTrip().getRouteId();
                String tripId  = tu.getTrip().getTripId();
                String vehId   = tu.hasVehicle() ? tu.getVehicle().getId() : null;

                for (var stu : tu.getStopTimeUpdateList()) {
                    String stopId = stu.getStopId();

                    Long arrEpoch = (stu.hasArrival() && stu.getArrival().hasTime()) ? stu.getArrival().getTime() : null;
                    Long depEpoch = (stu.hasDeparture() && stu.getDeparture().hasTime()) ? stu.getDeparture().getTime() : null;

                    String arrivo   = toRomeIso(arrEpoch);
                    String partenza = toRomeIso(depEpoch);

                    Double ritArrMin = (stu.hasArrival() && stu.getArrival().hasDelay()) ? round1(stu.getArrival().getDelay() / 60.0) : null;
                    Double ritPartMin = (stu.hasDeparture() && stu.getDeparture().hasDelay()) ? round1(stu.getDeparture().getDelay() / 60.0) : null;

                    String descArr   = formatDelay(ritArrMin);
                    String descPart  = formatDelay(ritPartMin);

                    GtfsIndexService.Stop stop = indexService.stopByIdOrNull(stopId);
                    String nomeFermata = (stop != null) ? stop.name() : null;


                    out.add(TripUpdateDTO.builder()
                            .linea(routeId)
                            .corsa(tripId)
                            .veicolo(vehId)
                            .fermataId(stopId)
                            .fermataNome(nomeFermata)
                            .arrivo(arrivo)
                            .partenza(partenza)
                            .ritardoArrivoMin(ritArrMin)
                            .ritardoPartenzaMin(ritPartMin)
                            .descArrivo(descArr)
                            .descPartenza(descPart)
                            .build());
                }
            }
            return out;

        } catch (Exception e) {
            log.error("[TripUpdates] parse error: {}", e.toString());
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

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank()) return a;
        if (b != null && !b.isBlank()) return b;
        return null;
    }

    private static Long bestTimeMillis(TripUpdateDTO dto) {
        Long arr = parseIsoMillis(dto.getArrivo());
        Long dep = parseIsoMillis(dto.getPartenza());
        if (arr == null) return dep;
        if (dep == null) return arr;
        return Math.min(arr, dep);
    }

    private static Long parseIsoMillis(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return ZonedDateTime.parse(value, ROME_TS).toInstant().toEpochMilli();
        } catch (Exception ignored) {
            return null;
        }
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
