package it.roma.gtfs.gtfs_monitor.service;

import com.google.transit.realtime.GtfsRealtime;
import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import it.roma.gtfs.gtfs_monitor.model.dto.ServiceAlertDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


@Slf4j
@Service
@RequiredArgsConstructor
public class ServiceAlertsService {
    private static final long DEFAULT_REFRESH_MILLIS = 5_000L;
    private static final int MAX_LIMIT = 5_000;
    private final GtfsProperties props;
    private final WebClient webClient;
    private final AtomicReference<CacheEntry<List<ServiceAlertDTO>>> cacheRef =
            new AtomicReference<>(CacheEntry.empty());
    private final Object refreshLock = new Object();
    private static final ZoneId ROME = ZoneId.of("Europe/Rome");


    private static final Map<String, String> CAUSE_IT = Map.ofEntries(
            Map.entry("UNKNOWN_CAUSE", "Causa sconosciuta"),
            Map.entry("OTHER_CAUSE", "Altra causa"),
            Map.entry("TECHNICAL_PROBLEM", "Problema tecnico"),
            Map.entry("STRIKE", "Sciopero"),
            Map.entry("DEMONSTRATION", "Manifestazione"),
            Map.entry("ACCIDENT", "Incidente"),
            Map.entry("HOLIDAY", "Festività"),
            Map.entry("WEATHER", "Maltempo"),
            Map.entry("MAINTENANCE", "Manutenzione"),
            Map.entry("CONSTRUCTION", "Lavori"),
            Map.entry("POLICE_ACTIVITY", "Intervento delle forze dell’ordine"),
            Map.entry("MEDICAL_EMERGENCY", "Emergenza medica")
    );

    private static final Map<String, String> EFFECT_IT = Map.ofEntries(
            Map.entry("NO_SERVICE", "Servizio sospeso"),
            Map.entry("REDUCED_SERVICE", "Servizio ridotto"),
            Map.entry("SIGNIFICANT_DELAYS", "Ritardi significativi"),
            Map.entry("DETOUR", "Deviazione"),
            Map.entry("ADDITIONAL_SERVICE", "Servizio aggiuntivo"),
            Map.entry("MODIFIED_SERVICE", "Servizio modificato"),
            Map.entry("OTHER_EFFECT", "Altro effetto"),
            Map.entry("UNKNOWN_EFFECT", "Effetto sconosciuto"),
            Map.entry("STOP_MOVED", "Fermata spostata"),
            Map.entry("NO_STOPS", "Fermate non servite"),
            Map.entry("CONSTRUCTION", "Lavori in corso"),
            Map.entry("MAINTENANCE", "Manutenzione"),
            Map.entry("ACCESSIBILITY_ISSUE", "Problema di accessibilità")
    );

    private static String translateEffect(String effect) {
        if (effect == null) return null;
        return EFFECT_IT.getOrDefault(effect, effect);
    }

    private static String translateCause(String cause) {
        if (cause == null) return null;
        return CAUSE_IT.getOrDefault(cause, cause);
    }

    public List<ServiceAlertDTO> fetch(
            Integer limit,
            String routeIdFilter,
            String tripIdFilter,
            String stopIdFilter,
            LocalDate fromDate,
            LocalDate toDate
    ) {
        List<ServiceAlertDTO> base = getCachedOrRefresh();
        if (base.isEmpty()) {
            return List.of();
        }

        Instant rangeStart;
        Instant rangeEnd;
        if (fromDate == null && toDate == null) {
            LocalDate today = LocalDate.now(ROME);
            rangeStart = today.atStartOfDay(ROME).toInstant();
            rangeEnd   = today.plusDays(1).atStartOfDay(ROME).minusNanos(1).toInstant();
        } else {
            LocalDate startD = (fromDate != null) ? fromDate : toDate;
            LocalDate endD   = (toDate != null) ? toDate : fromDate;
            if (endD.isBefore(startD)) {
                LocalDate tmp = startD;
                startD = endD;
                endD = tmp;
            }
            rangeStart = startD.atStartOfDay(ROME).toInstant();
            rangeEnd   = endD.plusDays(1).atStartOfDay(ROME).minusNanos(1).toInstant();
        }

        boolean hasRouteFilter = routeIdFilter != null;
        boolean hasTripFilter = tripIdFilter != null;
        boolean hasStopFilter = stopIdFilter != null;
        int max = normalizeLimit(limit);
        List<ServiceAlertDTO> out = new ArrayList<>(max > 0 ? Math.min(max, base.size()) : base.size());
        for (ServiceAlertDTO dto : base) {
            if (hasRouteFilter && !contains(dto.getRouteIds(), routeIdFilter)) continue;
            if (hasTripFilter && !contains(dto.getTripIds(), tripIdFilter)) continue;
            if (hasStopFilter && !contains(dto.getStopIds(), stopIdFilter)) continue;
            if (!overlaps(dto.getInizio(), dto.getFine(), rangeStart, rangeEnd)) continue;

            out.add(dto);
            if (max > 0 && out.size() >= max) {
                break;
            }
        }
        return out;
    }

    public List<ServiceAlertDTO> fetchActiveNow(String routeIdFilter, Integer limit) {
        List<ServiceAlertDTO> base = getCachedOrRefresh();
        if (base.isEmpty()) return List.of();

        Instant now = Instant.now();
        int max = normalizeLimit(limit);
        List<ServiceAlertDTO> out = new ArrayList<>(max > 0 ? Math.min(max, base.size()) : base.size());
        for (ServiceAlertDTO dto : base) {
            if (routeIdFilter != null && !contains(dto.getRouteIds(), routeIdFilter)) continue;
            if (!overlaps(dto.getInizio(), dto.getFine(), now, now)) continue;
            out.add(dto);
            if (max > 0 && out.size() >= max) break;
        }
        return out;
    }

    @Scheduled(
            fixedDelayString = "${gtfs.realtime.refresh-millis:5000}",
            initialDelayString = "${gtfs.realtime.refresh-millis:5000}"
    )
    public void scheduledRefresh() {
        refreshCache(true);
    }

    private List<ServiceAlertDTO> getCachedOrRefresh() {
        CacheEntry<List<ServiceAlertDTO>> current = cacheRef.get();
        if (!isExpired(current)) {
            return current.data();
        }
        refreshCache(false);
        return cacheRef.get().data();
    }

    private void refreshCache(boolean force) {
        synchronized (refreshLock) {
            CacheEntry<List<ServiceAlertDTO>> current = cacheRef.get();
            if (!force && !isExpired(current)) {
                return;
            }
            try {
                List<ServiceAlertDTO> fresh = fetchRemoteSnapshot();
                if (!fresh.isEmpty() || current.isEmpty()) {
                    cacheRef.set(new CacheEntry<>(List.copyOf(fresh), System.currentTimeMillis()));
                }
            } catch (Exception e) {
                log.warn("[ServiceAlerts] refresh cache fallito: {}", e.toString());
            }
        }
    }

    private List<ServiceAlertDTO> fetchRemoteSnapshot() {
        String url = Objects.requireNonNull(props.realtime().serviceAlertsUrl(), "gtfs.realtime.serviceAlertsUrl mancante");

        byte[] body = webClient.get().uri(url)
                .retrieve()
                .bodyToMono(byte[].class)
                .block();

        if (body == null || body.length == 0) {
            log.warn("[ServiceAlerts] body vuoto da {}", url);
            return List.of();
        }

        try {
            var feed = GtfsRealtime.FeedMessage.parseFrom(body);
            List<ServiceAlertDTO> out = new ArrayList<>(Math.max(16, feed.getEntityCount()));

            for (var ent : feed.getEntityList()) {
                if (!ent.hasAlert()) continue;
                var a = ent.getAlert();

                String header = pickTranslation(a.getHeaderText());
                String desc   = pickTranslation(a.getDescriptionText());

                // scope
                int informedSize = a.getInformedEntityCount();
                Set<String> routes = new HashSet<>(Math.max(4, informedSize));
                Set<String> trips  = new HashSet<>(Math.max(4, informedSize));
                Set<String> stops  = new HashSet<>(Math.max(4, informedSize));

                for (var es : a.getInformedEntityList()) {
                    if (es.hasRouteId()) routes.add(es.getRouteId());
                    if (es.hasTrip() && es.getTrip().hasTripId()) trips.add(es.getTrip().getTripId());
                    if (es.hasStopId()) stops.add(es.getStopId());
                }

                var periods = a.getActivePeriodList().isEmpty() ? List.of(GtfsRealtime.TimeRange.newBuilder().build()) : a.getActivePeriodList();
                List<String> routeIds = routes.isEmpty() ? null : List.copyOf(routes);
                List<String> tripIds = trips.isEmpty() ? null : List.copyOf(trips);
                List<String> stopIds = stops.isEmpty() ? null : List.copyOf(stops);

                String effect = a.hasEffect() ? translateEffect(a.getEffect().name()) : null;
                String cause  = a.hasCause()  ? translateCause(a.getCause().name())  : null;
                String sev    = a.hasSeverityLevel() ? a.getSeverityLevel().name() : null;

                for (var pr : periods) {

                    var start = pr.hasStart() ? Instant.ofEpochSecond(pr.getStart()) : null;
                    var end   = pr.hasEnd()   ? Instant.ofEpochSecond(pr.getEnd())   : null;

                    out.add(ServiceAlertDTO.builder()
                            .id(ent.hasId() ? ent.getId() : null)
                            .titolo(header)
                            .descrizione(desc)
                            .inizio(start)
                            .fine(end)
                            .severita(sev)
                            .causa(cause)
                            .effetto(effect)
                            .routeIds(routeIds)
                            .tripIds(tripIds)
                            .stopIds(stopIds)
                            .build());
                }
            }
            return out;

        } catch (Exception e) {
            log.error("[ServiceAlerts] parse error: {}", e.toString());
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

    private static boolean overlaps(Instant pStart, Instant pEnd, Instant rangeStart, Instant rangeEnd) {
        Instant s = (pStart != null) ? pStart : Instant.MIN;
        Instant e = (pEnd   != null) ? pEnd   : Instant.MAX;
        return !e.isBefore(rangeStart) && !s.isAfter(rangeEnd);
    }

    private static boolean contains(List<String> values, String target) {
        return values != null && values.contains(target);
    }

    private static String pickTranslation(GtfsRealtime.TranslatedString ts) {
        if (ts == null) return null;
        for (var t : ts.getTranslationList()) {
            var lang = t.hasLanguage() ? t.getLanguage().toLowerCase(Locale.ROOT) : "";
            if (lang.equals("it") || lang.equals("it-it")) {
                return t.getText();
            }
        }
        return ts.getTranslationCount() > 0 ? ts.getTranslation(0).getText() : null;
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
