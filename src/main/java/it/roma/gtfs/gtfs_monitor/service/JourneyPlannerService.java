package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.JourneyLegDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.JourneyLegStopDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.JourneyLocationDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.JourneyOptionDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.JourneyPlanResponseDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class JourneyPlannerService {
    private static final int MAX_OTP_OPTIONS = 12;
    private static final String OTP_GTFS_GRAPHQL_QUERY_TEMPLATE = """
            {
              planConnection(
                origin: { label: "%s", location: { coordinate: { latitude: %s, longitude: %s } } }
                destination: { label: "%s", location: { coordinate: { latitude: %s, longitude: %s } } }
                dateTime: { %s: "%s" }
                searchWindow: "PT60M"
                first: %d
              ) {
                edges {
                  node {
                    start
                    end
                    duration
                    legs {
                      mode
                      transitLeg
                      realTime
                      headsign
                      from { name lat lon }
                      to { name lat lon }
                      route { shortName }
                      legGeometry { points }
                      stopCalls {
                        stopLocation {
                          ... on Stop { name lat lon }
                        }
                      }
                      start { estimated { time } }
                      end { estimated { time } }
                      duration
                    }
                  }
                }
              }
            }
            """;

    private final WebClient webClient;

    @Value("${journey.otp.enabled:false}")
    private boolean otpEnabled;

    @Value("${journey.otp.base-url:http://localhost:8081}")
    private String otpBaseUrl;

    @Value("${journey.otp.plan-path:/otp/routers/default/plan}")
    private String otpPlanPath;

    @SuppressWarnings("unchecked")
    public JourneyPlanResponseDTO plan(
            double fromLat,
            double fromLon,
            String fromLabel,
            double toLat,
            double toLon,
            String toLabel,
            Integer numItineraries,
            String timeMode,
            String when
    ) {
        JourneyLocationDTO from = new JourneyLocationDTO(fromLat, fromLon, fromLabel);
        JourneyLocationDTO to = new JourneyLocationDTO(toLat, toLon, toLabel);
        int limit = numItineraries == null || numItineraries <= 0 ? 3 : Math.min(numItineraries, 6);
        int upstreamLimit = Math.min(Math.max(limit * 3, limit + 2), MAX_OTP_OPTIONS);

        if (!otpEnabled) {
            return new JourneyPlanResponseDTO(
                    "otp",
                    from,
                    to,
                    List.of(),
                    "Trip planner non configurato sul server (OTP disattivato).",
                    Instant.now()
            );
        }

        try {
            URI uri = resolveOtpGraphQlUri();
            Instant now = Instant.now();
            TimePreference preference = resolveTimePreference(timeMode, when, now);
            String query = OTP_GTFS_GRAPHQL_QUERY_TEMPLATE.formatted(
                    escapeGraphqlString(firstNonBlank(fromLabel, "Partenza")),
                    trimDecimals(fromLat),
                    trimDecimals(fromLon),
                    escapeGraphqlString(firstNonBlank(toLabel, "Destinazione")),
                    trimDecimals(toLat),
                    trimDecimals(toLon),
                    preference.graphQlField(),
                    preference.when().toString(),
                    upstreamLimit
            );

            Map<String, Object> request = new LinkedHashMap<>();
            request.put("query", query);

            Map<String, Object> payload = webClient.post()
                    .uri(uri)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("User-Agent", "gtfs-monitor/1.0 (journey-planner)")
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block(Duration.ofSeconds(15));

            if (payload == null) {
                return new JourneyPlanResponseDTO("otp", from, to, List.of(), "Risposta vuota da OTP.", now);
            }

            if (payload.get("errors") instanceof List<?> errors && !errors.isEmpty()) {
                String msg = extractGraphqlErrors((List<?>) errors);
                return new JourneyPlanResponseDTO("otp", from, to, List.of(), msg, now);
            }

            Map<String, Object> data = payload.get("data") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
            Map<String, Object> planConnection = data.get("planConnection") instanceof Map<?, ?> m
                    ? (Map<String, Object>) m : Map.of();
            List<Map<String, Object>> edges = planConnection.get("edges") instanceof List<?> l
                    ? (List<Map<String, Object>>) (List<?>) l : List.of();

            List<JourneyOptionDTO> rawOptions = edges.stream()
                    .map(edge -> edge.get("node"))
                    .filter(Map.class::isInstance)
                    .map(node -> toJourneyOption((Map<String, Object>) node))
                    .filter(o -> o.legs() != null && !o.legs().isEmpty())
                    .toList();
            List<JourneyOptionDTO> options = dedupeJourneyOptions(rawOptions, limit);

            String error = options.isEmpty() ? "Nessun percorso trovato." : null;
            return new JourneyPlanResponseDTO("otp", from, to, options, error, now);
        } catch (Exception e) {
            log.debug("[JourneyPlanner] OTP GraphQL request failed: {}", e.toString());
            return new JourneyPlanResponseDTO(
                    "otp", from, to, List.of(),
                    "Trip planner non raggiungibile. Verifica server OTP.",
                    Instant.now()
            );
        }
    }

    private URI resolveOtpGraphQlUri() {
        // OTP 2.x exposes GTFS GraphQL at /otp/gtfs/v1 in most setups.
        return UriComponentsBuilder.fromHttpUrl(otpBaseUrl)
                .path("/otp/gtfs/v1")
                .build(true)
                .toUri();
    }

    @SuppressWarnings("unchecked")
    private JourneyOptionDTO toJourneyOption(Map<String, Object> item) {
        String startTime = toStringOrNull(item.get("start"));
        String endTime = toStringOrNull(item.get("end"));
        Integer durationMin = isoDurationSecondsToMinutes(toStringOrNull(item.get("duration")));

        List<Map<String, Object>> legsRaw = item.get("legs") instanceof List<?> l
                ? (List<Map<String, Object>>) (List<?>) l : List.of();
        List<JourneyLegDTO> legs = new ArrayList<>(legsRaw.size());
        int walkMinutes = 0;
        int transfers = 0;
        int transitLegs = 0;

        for (Map<String, Object> leg : legsRaw) {
            String mode = toStringOrNull(leg.get("mode"));
            Boolean transitLeg = leg.get("transitLeg") instanceof Boolean b ? b : null;
            boolean walk = "foot".equalsIgnoreCase(mode) || "walk".equalsIgnoreCase(mode);
            if (Boolean.TRUE.equals(transitLeg)) {
                walk = false;
            }
            if (!walk) transitLegs++;

            String lineCode = null;
            if (leg.get("route") instanceof Map<?, ?> route) {
                lineCode = firstNonBlank(
                        toStringOrNull(route.get("shortName")),
                        toStringOrNull(route.get("short_name"))
                );
            }
            String headsign = toStringOrNull(leg.get("headsign"));
            Boolean realtime = leg.get("realTime") instanceof Boolean b ? b : null;
            String fromName = null;
            Double fromLat = null;
            Double fromLon = null;
            if (leg.get("from") instanceof Map<?, ?> from) {
                fromName = toStringOrNull(from.get("name"));
                fromLat = toDoubleOrNull(from.get("lat"));
                fromLon = toDoubleOrNull(from.get("lon"));
            }
            String toName = null;
            Double toLat = null;
            Double toLon = null;
            if (leg.get("to") instanceof Map<?, ?> to) {
                toName = toStringOrNull(to.get("name"));
                toLat = toDoubleOrNull(to.get("lat"));
                toLon = toDoubleOrNull(to.get("lon"));
            }

            String legStart = legTimeToIso(leg.get("start"));
            String legEnd = legTimeToIso(leg.get("end"));
            Integer legDuration = floatMinutes(leg.get("duration"));
            if (legDuration == null) {
                legDuration = diffMinutes(legStart, legEnd);
            }
            if (walk && legDuration != null) {
                walkMinutes += Math.max(0, legDuration);
            }

            Integer realtimeDelay = null;
            String geometryPoints = null;
            if (leg.get("legGeometry") instanceof Map<?, ?> geometry) {
                geometryPoints = toStringOrNull(geometry.get("points"));
            }
            List<JourneyLegStopDTO> legStops = extractLegStops(leg.get("stopCalls"));

            legs.add(new JourneyLegDTO(
                    normalizeMode(mode),
                    lineCode,
                    lineCode,
                    realtime,
                    headsign,
                    fromName,
                    toName,
                    legStart,
                    legEnd,
                    legDuration,
                    realtimeDelay,
                    fromLat,
                    fromLon,
                    toLat,
                    toLon,
                    geometryPoints,
                    legStops
            ));
        }

        if (transitLegs > 1) {
            transfers = transitLegs - 1;
        }

        return new JourneyOptionDTO(
                durationMin != null ? durationMin : diffMinutes(startTime, endTime),
                walkMinutes > 0 ? walkMinutes : null,
                null,
                transfers,
                startTime,
                endTime,
                legs
        );
    }

    @SuppressWarnings("unchecked")
    private static List<JourneyLegStopDTO> extractLegStops(Object rawStopCalls) {
        if (!(rawStopCalls instanceof List<?> list) || list.isEmpty()) {
            return List.of();
        }
        List<JourneyLegStopDTO> out = new ArrayList<>();
        String lastKey = null;
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> raw)) continue;
            Map<String, Object> call = (Map<String, Object>) raw;
            if (!(call.get("stopLocation") instanceof Map<?, ?> stopRaw)) continue;
            Map<String, Object> stop = (Map<String, Object>) stopRaw;
            String name = toStringOrNull(stop.get("name"));
            Double lat = toDoubleOrNull(stop.get("lat"));
            Double lon = toDoubleOrNull(stop.get("lon"));
            if (name == null || lat == null || lon == null) continue;
            String key = name + "|" + lat + "|" + lon;
            if (key.equals(lastKey)) continue;
            lastKey = key;
            out.add(new JourneyLegStopDTO(name, lat, lon));
        }
        return out;
    }

    private static String normalizeMode(String mode) {
        if (mode == null) return null;
        if ("foot".equalsIgnoreCase(mode)) return "WALK";
        return mode.toUpperCase();
    }

    private static Integer isoDurationSecondsToMinutes(String isoDuration) {
        if (isoDuration == null || isoDuration.isBlank()) return null;
        try {
            return (int) Math.round(java.time.Duration.parse(isoDuration).toSeconds() / 60.0);
        } catch (Exception e) {
            return null;
        }
    }

    private static Integer floatMinutes(Object value) {
        if (!(value instanceof Number n)) return null;
        return (int) Math.max(0, Math.round(n.doubleValue() / 60.0));
    }

    @SuppressWarnings("unchecked")
    private static String legTimeToIso(Object legTimeObj) {
        if (!(legTimeObj instanceof Map<?, ?> raw)) return null;
        Map<String, Object> legTime = (Map<String, Object>) raw;
        Object estimated = legTime.get("estimated");
        String e = timeHolderToIso(estimated);
        if (e != null) return e;
        return null;
    }

    @SuppressWarnings("unchecked")
    private static String timeHolderToIso(Object obj) {
        if (!(obj instanceof Map<?, ?> raw)) return null;
        Map<String, Object> m = (Map<String, Object>) raw;
        return toStringOrNull(m.get("time"));
    }

    private static Integer diffMinutes(String startIso, String endIso) {
        if (startIso == null || endIso == null) return null;
        try {
            Instant s = Instant.parse(startIso);
            Instant e = Instant.parse(endIso);
            return (int) Math.max(0, Math.round((e.toEpochMilli() - s.toEpochMilli()) / 60000.0));
        } catch (Exception ex) {
            return null;
        }
    }

    private static List<JourneyOptionDTO> dedupeJourneyOptions(List<JourneyOptionDTO> rawOptions, int limit) {
        if (rawOptions.isEmpty()) {
            return List.of();
        }
        List<JourneyOptionDTO> ordered = rawOptions.stream()
                .sorted(Comparator
                        .comparing((JourneyOptionDTO option) -> isWalkOnly(option))
                        .thenComparingInt(JourneyPlannerService::journeyBusPriorityScore)
                        .thenComparingInt(option -> option.durationMinutes() != null ? option.durationMinutes() : Integer.MAX_VALUE)
                        .thenComparingInt(option -> option.walkMinutes() != null ? option.walkMinutes() : Integer.MAX_VALUE)
                        .thenComparingInt(option -> option.transfers() != null ? option.transfers() : Integer.MAX_VALUE))
                .toList();

        Set<String> seen = new LinkedHashSet<>();
        List<JourneyOptionDTO> unique = new ArrayList<>();
        for (JourneyOptionDTO option : ordered) {
            if (!seen.add(journeyOptionSignature(option))) {
                continue;
            }
            unique.add(option);
            if (unique.size() >= limit) {
                break;
            }
        }
        return unique;
    }

    private static String journeyOptionSignature(JourneyOptionDTO option) {
        StringBuilder out = new StringBuilder();
        out.append(option.transfers() != null ? option.transfers() : -1)
                .append('|')
                .append(option.walkMinutes() != null ? option.walkMinutes() : -1)
                .append('|')
                .append(bucketTime(option.startTime()))
                .append('|')
                .append(bucketTime(option.endTime()));

        for (JourneyLegDTO leg : option.legs()) {
            String mode = firstNonBlank(leg.mode(), "NA");
            out.append("||").append(mode);
            if ("WALK".equalsIgnoreCase(mode)) {
                out.append('|')
                        .append(normalizeKey(leg.fromName()))
                        .append('|')
                        .append(normalizeKey(leg.toName()));
                continue;
            }
            out.append('|')
                    .append(normalizeKey(firstNonBlank(leg.routeShortName(), leg.line())))
                    .append('|')
                    .append(normalizeKey(leg.headsign()))
                    .append('|')
                    .append(normalizeKey(leg.fromName()))
                    .append('|')
                    .append(normalizeKey(leg.toName()))
                    .append('|')
                    .append(bucketTime(leg.startTime()))
                    .append('|')
                    .append(bucketTime(leg.endTime()));
        }
        return out.toString();
    }

    private static int bucketTime(String iso) {
        if (iso == null || iso.isBlank()) return -1;
        try {
            long minutes = Instant.parse(iso).getEpochSecond() / 60L;
            return (int) (minutes / 5L);
        } catch (Exception e) {
            return -1;
        }
    }

    private static String normalizeKey(String value) {
        return value == null ? "" : value.trim().toLowerCase();
    }

    private static boolean isWalkOnly(JourneyOptionDTO option) {
        return option.legs() != null
                && !option.legs().isEmpty()
                && option.legs().stream().allMatch(leg -> "WALK".equalsIgnoreCase(leg.mode()));
    }

    private static int journeyBusPriorityScore(JourneyOptionDTO option) {
        if (option.legs() == null || option.legs().isEmpty()) {
            return 100;
        }
        List<String> modes = option.legs().stream()
                .map(JourneyLegDTO::mode)
                .filter(mode -> mode != null && !mode.isBlank() && !"WALK".equalsIgnoreCase(mode))
                .map(String::toUpperCase)
                .toList();
        if (modes.isEmpty()) {
            return 100;
        }

        boolean hasBus = modes.contains("BUS");
        boolean hasRail = modes.contains("SUBWAY") || modes.contains("RAIL") || modes.contains("TRAM");
        int score = 0;
        if (!hasBus) score += 40;
        if (hasRail) score += 8;
        score += (int) modes.stream().filter(mode -> !"BUS".equals(mode)).count() * 2;
        return score;
    }

    private static String extractGraphqlErrors(List<?> errors) {
        StringBuilder sb = new StringBuilder();
        for (Object err : errors) {
            if (err instanceof Map<?, ?> m) {
                String msg = toStringOrNull(m.get("message"));
                if (msg != null) {
                    if (!sb.isEmpty()) sb.append(" | ");
                    sb.append(msg);
                }
            }
        }
        return sb.isEmpty() ? "Errore dal trip planner OTP (GraphQL)." : sb.toString();
    }

    private static String toStringOrNull(Object value) {
        if (value == null) return null;
        String s = String.valueOf(value).trim();
        return s.isBlank() ? null : s;
    }

    private static Double toDoubleOrNull(Object value) {
        if (value instanceof Number n) return n.doubleValue();
        if (value == null) return null;
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (Exception e) {
            return null;
        }
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    private static String trimDecimals(double value) {
        return String.format(java.util.Locale.ROOT, "%.6f", value);
    }

    private static String escapeGraphqlString(String value) {
        if (value == null) return "";
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", " ")
                .replace("\r", " ");
    }

    private static TimePreference resolveTimePreference(String mode, String when, Instant now) {
        Instant target = parseInstantOrNull(when);
        String normalized = mode == null ? "" : mode.trim().toLowerCase();
        if ("arrive-by".equals(normalized) && target != null) {
            return new TimePreference("latestArrival", target);
        }
        if ("depart-at".equals(normalized) && target != null) {
            return new TimePreference("earliestDeparture", target);
        }
        return new TimePreference("earliestDeparture", target != null ? target : now);
    }

    private static Instant parseInstantOrNull(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return Instant.parse(value);
        } catch (Exception ignored) {
            try {
                return OffsetDateTime.parse(value).toInstant();
            } catch (Exception ignoredAgain) {
                return null;
            }
        }
    }

    private record TimePreference(String graphQlField, Instant when) {
    }
}
