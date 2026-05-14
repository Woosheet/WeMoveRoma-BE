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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class JourneyPlannerService {
    private static final Set<String> SUPPORTED_TRANSIT_MODES = Set.of("BUS", "SUBWAY", "TRAM", "RAIL");
    private static final String OTP_GTFS_GRAPHQL_QUERY_TEMPLATE = """
            {
              planConnection(
                origin: { label: "%s", location: { coordinate: { latitude: %s, longitude: %s } } }
                destination: { label: "%s", location: { coordinate: { latitude: %s, longitude: %s } } }
                dateTime: { %s: "%s" }
                searchWindow: "%s"
                %s
                preferences: {
                  street: {
                    walk: {
                      speed: %s
                      reluctance: %s
                      safetyFactor: 1.0
                      boardCost: 600
                    }
                  }
                  transit: {
                    transfer: {
                      cost: %d
                      slack: "PT%dS"
                    }
                    board: { slack: "PT%dS" }
                    alight: { slack: "PT%dS" }
                    waitReluctance: %s
                  }
                }
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
                      route {
                        gtfsId
                        shortName
                        longName
                        agency { gtfsId name }
                      }
                      trip {
                        gtfsId
                        tripShortName
                      }
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

    @Value("${journey.otp.timeout-seconds:15}")
    private long otpTimeoutSeconds;

    @Value("${journey.otp.search-window:PT60M}")
    private String searchWindow;

    @Value("${journey.otp.search-window-fallback:PT120M}")
    private String searchWindowFallback;

    @Value("${journey.otp.max-itineraries:10}")
    private int maxItineraries;

    @Value("${journey.otp.max-upstream-itineraries:20}")
    private int maxUpstreamItineraries;

    @Value("${journey.otp.walk-speed:1.34}")
    private double walkSpeed;

    @Value("${journey.otp.walk-reluctance:1.6}")
    private double walkReluctance;

    @Value("${journey.otp.wait-reluctance:1.0}")
    private double waitReluctance;

    @Value("${journey.otp.transfer-penalty:300}")
    private int transferPenalty;

    @Value("${journey.otp.board-slack-seconds:60}")
    private int boardSlackSeconds;

    @Value("${journey.otp.alight-slack-seconds:30}")
    private int alightSlackSeconds;

    @Value("${journey.otp.stairs-reluctance:1.8}")
    private double stairsReluctance;

    public JourneyPlanResponseDTO plan(
            double fromLat,
            double fromLon,
            String fromLabel,
            double toLat,
            double toLon,
            String toLabel,
            Integer numItineraries,
            String timeMode,
            String when,
            String modes
    ) {
        JourneyLocationDTO from = new JourneyLocationDTO(fromLat, fromLon, fromLabel);
        JourneyLocationDTO to = new JourneyLocationDTO(toLat, toLon, toLabel);

        int hardCap = Math.max(2, maxItineraries);
        int requested = numItineraries == null || numItineraries <= 0 ? 5 : Math.min(numItineraries, hardCap);
        int upstreamLimit = Math.min(Math.max(requested * 3, requested + 4), Math.max(hardCap * 2, maxUpstreamItineraries));

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

        Instant now = Instant.now();
        TimePreference preference = resolveTimePreference(timeMode, when, now);
        String modesClause = buildModesClause(modes);

        List<JourneyOptionDTO> rawOptions = executeQuery(
                fromLat, fromLon, fromLabel,
                toLat, toLon, toLabel,
                preference, modesClause, upstreamLimit, searchWindow
        );

        // Fallback con searchWindow allargata se la prima ricerca è vuota
        boolean fallbackUsed = false;
        if (rawOptions.isEmpty() && !searchWindow.equals(searchWindowFallback)) {
            log.info("[JourneyPlanner] Empty result with searchWindow={}, retry with {}",
                    searchWindow, searchWindowFallback);
            rawOptions = executeQuery(
                    fromLat, fromLon, fromLabel,
                    toLat, toLon, toLabel,
                    preference, modesClause, upstreamLimit, searchWindowFallback
            );
            fallbackUsed = !rawOptions.isEmpty();
        }

        if (rawOptions == null) {
            return new JourneyPlanResponseDTO(
                    "otp", from, to, List.of(),
                    "Trip planner non raggiungibile. Verifica server OTP.",
                    Instant.now()
            );
        }

        List<JourneyOptionDTO> options = dedupeAndEnrich(rawOptions, requested);
        String error = options.isEmpty() ? "Nessun percorso trovato." : null;
        if (fallbackUsed && !options.isEmpty()) {
            log.debug("[JourneyPlanner] Returning {} itineraries via fallback window", options.size());
        }
        return new JourneyPlanResponseDTO("otp", from, to, options, error, now);
    }

    @SuppressWarnings("unchecked")
    private List<JourneyOptionDTO> executeQuery(
            double fromLat, double fromLon, String fromLabel,
            double toLat, double toLon, String toLabel,
            TimePreference preference,
            String modesClause,
            int upstreamLimit,
            String windowIso
    ) {
        try {
            URI uri = resolveOtpGraphQlUri();
            String query = OTP_GTFS_GRAPHQL_QUERY_TEMPLATE.formatted(
                    escapeGraphqlString(firstNonBlank(fromLabel, "Partenza")),
                    trimDecimals(fromLat),
                    trimDecimals(fromLon),
                    escapeGraphqlString(firstNonBlank(toLabel, "Destinazione")),
                    trimDecimals(toLat),
                    trimDecimals(toLon),
                    preference.graphQlField(),
                    preference.when().toString(),
                    windowIso,
                    modesClause,
                    formatNumber(walkSpeed),
                    formatNumber(walkReluctance),
                    transferPenalty,
                    0,
                    boardSlackSeconds,
                    alightSlackSeconds,
                    formatNumber(waitReluctance),
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
                    .block(Duration.ofSeconds(Math.max(1, otpTimeoutSeconds)));

            if (payload == null) {
                return List.of();
            }
            if (payload.get("errors") instanceof List<?> errors && !errors.isEmpty()) {
                log.warn("[JourneyPlanner] OTP GraphQL errors: {}", extractGraphqlErrors(errors));
                // Retry una volta senza il blocco preferences (alcune versioni di OTP differiscono nello schema)
                if (containsPreferencesSchemaError(errors)) {
                    return executeFallbackQueryWithoutPreferences(
                            fromLat, fromLon, fromLabel, toLat, toLon, toLabel,
                            preference, modesClause, upstreamLimit, windowIso
                    );
                }
                return List.of();
            }

            Map<String, Object> data = payload.get("data") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
            Map<String, Object> planConnection = data.get("planConnection") instanceof Map<?, ?> m
                    ? (Map<String, Object>) m : Map.of();
            List<Map<String, Object>> edges = planConnection.get("edges") instanceof List<?> l
                    ? (List<Map<String, Object>>) (List<?>) l : List.of();

            return edges.stream()
                    .map(edge -> edge.get("node"))
                    .filter(Map.class::isInstance)
                    .map(node -> toJourneyOption((Map<String, Object>) node))
                    .filter(o -> o.legs() != null && !o.legs().isEmpty())
                    .toList();
        } catch (Exception e) {
            log.warn("[JourneyPlanner] OTP GraphQL request failed for {} -> {} (window={}): {}",
                    fromLabel, toLabel, windowIso, e.toString(), e);
            return null;
        }
    }

    private static boolean containsPreferencesSchemaError(List<?> errors) {
        for (Object err : errors) {
            if (err instanceof Map<?, ?> m) {
                String msg = toStringOrNull(m.get("message"));
                if (msg == null) continue;
                String lower = msg.toLowerCase(Locale.ROOT);
                if (lower.contains("preferences") || lower.contains("walkreluctance")
                        || lower.contains("transferpenalty") || lower.contains("waitreluctance")
                        || lower.contains("unknown argument") || lower.contains("unknown field")
                        || lower.contains("unknown type")
                        || lower.contains("expected type")
                        || lower.contains("coercingparsevalueexception")
                        || lower.contains("validationerror")
                        || lower.contains("wrongtype")) {
                    return true;
                }
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private List<JourneyOptionDTO> executeFallbackQueryWithoutPreferences(
            double fromLat, double fromLon, String fromLabel,
            double toLat, double toLon, String toLabel,
            TimePreference preference,
            String modesClause,
            int upstreamLimit,
            String windowIso
    ) {
        try {
            URI uri = resolveOtpGraphQlUri();
            String query = """
                    {
                      planConnection(
                        origin: { label: "%s", location: { coordinate: { latitude: %s, longitude: %s } } }
                        destination: { label: "%s", location: { coordinate: { latitude: %s, longitude: %s } } }
                        dateTime: { %s: "%s" }
                        searchWindow: "%s"
                        %s
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
                              route { gtfsId shortName longName agency { gtfsId name } }
                              trip { gtfsId tripShortName }
                              legGeometry { points }
                              stopCalls { stopLocation { ... on Stop { name lat lon } } }
                              start { estimated { time } }
                              end { estimated { time } }
                              duration
                            }
                          }
                        }
                      }
                    }
                    """.formatted(
                    escapeGraphqlString(firstNonBlank(fromLabel, "Partenza")),
                    trimDecimals(fromLat),
                    trimDecimals(fromLon),
                    escapeGraphqlString(firstNonBlank(toLabel, "Destinazione")),
                    trimDecimals(toLat),
                    trimDecimals(toLon),
                    preference.graphQlField(),
                    preference.when().toString(),
                    windowIso,
                    modesClause,
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
                    .block(Duration.ofSeconds(Math.max(1, otpTimeoutSeconds)));
            if (payload == null) return List.of();
            Map<String, Object> data = payload.get("data") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
            Map<String, Object> planConnection = data.get("planConnection") instanceof Map<?, ?> m
                    ? (Map<String, Object>) m : Map.of();
            List<Map<String, Object>> edges = planConnection.get("edges") instanceof List<?> l
                    ? (List<Map<String, Object>>) (List<?>) l : List.of();
            return edges.stream()
                    .map(edge -> edge.get("node"))
                    .filter(Map.class::isInstance)
                    .map(node -> toJourneyOption((Map<String, Object>) node))
                    .filter(o -> o.legs() != null && !o.legs().isEmpty())
                    .toList();
        } catch (Exception e) {
            log.warn("[JourneyPlanner] OTP fallback (no preferences) failed: {}", e.toString());
            return List.of();
        }
    }

    private URI resolveOtpGraphQlUri() {
        // OTP 2.x exposes GTFS GraphQL at /otp/gtfs/v1 in most setups.
        return UriComponentsBuilder.fromHttpUrl(otpBaseUrl)
                .path("/otp/gtfs/v1")
                .build(true)
                .toUri();
    }

    private String buildModesClause(String requestedModes) {
        LinkedHashSet<String> normalizedModes = Arrays.stream(firstNonBlank(requestedModes, "").split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .map(value -> value.toUpperCase(Locale.ROOT))
                .filter(SUPPORTED_TRANSIT_MODES::contains)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (normalizedModes.isEmpty()) {
            return "";
        }
        if (normalizedModes.size() == SUPPORTED_TRANSIT_MODES.size() && normalizedModes.containsAll(SUPPORTED_TRANSIT_MODES)) {
            return "";
        }

        String modesLiteral = normalizedModes.stream()
                .map(mode -> "{ mode: " + mode + " }")
                .collect(Collectors.joining(", "));
        return "modes: { transitOnly: true, transit: { access: [WALK], egress: [WALK], transfer: WALK, transit: [" + modesLiteral + "] } }";
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

        // Per il calcolo del waitingMinutes: somma dei gap tra fine di una leg e inizio della successiva
        // (l'attesa effettiva alle fermate di interscambio o all'origine se OTP la include).
        int waitingMinutes = 0;
        String previousLegEnd = null;

        for (Map<String, Object> leg : legsRaw) {
            String mode = toStringOrNull(leg.get("mode"));
            Boolean transitLeg = leg.get("transitLeg") instanceof Boolean b ? b : null;
            boolean walk = "foot".equalsIgnoreCase(mode) || "walk".equalsIgnoreCase(mode);
            if (Boolean.TRUE.equals(transitLeg)) {
                walk = false;
            }
            if (!walk) transitLegs++;

            String lineCode = null;
            String routeLongName = null;
            String agencyName = null;
            String agencyId = null;
            if (leg.get("route") instanceof Map<?, ?> route) {
                lineCode = firstNonBlank(
                        toStringOrNull(route.get("shortName")),
                        toStringOrNull(route.get("short_name"))
                );
                routeLongName = firstNonBlank(
                        toStringOrNull(route.get("longName")),
                        toStringOrNull(route.get("long_name"))
                );
                if (route.get("agency") instanceof Map<?, ?> agency) {
                    agencyName = toStringOrNull(agency.get("name"));
                    agencyId = firstNonBlank(
                            toStringOrNull(agency.get("gtfsId")),
                            toStringOrNull(agency.get("id"))
                    );
                }
            }
            String tripShortName = null;
            String tripId = null;
            if (leg.get("trip") instanceof Map<?, ?> trip) {
                tripShortName = firstNonBlank(
                        toStringOrNull(trip.get("tripShortName")),
                        toStringOrNull(trip.get("shortName"))
                );
                tripId = firstNonBlank(
                        toStringOrNull(trip.get("gtfsId")),
                        toStringOrNull(trip.get("id"))
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

            if (previousLegEnd != null && legStart != null) {
                Integer gap = diffMinutes(previousLegEnd, legStart);
                if (gap != null && gap > 0) {
                    waitingMinutes += gap;
                }
            }
            if (legEnd != null) {
                previousLegEnd = legEnd;
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
                    routeLongName,
                    agencyName,
                    agencyId,
                    tripShortName,
                    tripId,
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

        Integer durationFinal = durationMin != null ? durationMin : diffMinutes(startTime, endTime);
        Integer waitingFinal = transfers > 0 && waitingMinutes > 0 ? waitingMinutes : null;

        return new JourneyOptionDTO(
                durationFinal,
                walkMinutes > 0 ? walkMinutes : null,
                waitingFinal,
                transfers,
                startTime,
                endTime,
                legs,
                null
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

    private List<JourneyOptionDTO> dedupeAndEnrich(List<JourneyOptionDTO> rawOptions, int limit) {
        if (rawOptions.isEmpty()) {
            return List.of();
        }
        List<JourneyOptionDTO> ordered = rawOptions.stream()
                .sorted(Comparator
                        .comparing((JourneyOptionDTO option) -> isWalkOnly(option))
                        .thenComparingInt(option -> option.durationMinutes() != null ? option.durationMinutes() : Integer.MAX_VALUE)
                        .thenComparingInt(option -> option.transfers() != null ? option.transfers() : Integer.MAX_VALUE)
                        .thenComparingInt(option -> option.walkMinutes() != null ? option.walkMinutes() : Integer.MAX_VALUE)
                        .thenComparingInt(JourneyPlannerService::realtimeBonus)
                        .thenComparingInt(JourneyPlannerService::journeyTransitPreferenceScore))
                .toList();

        Map<String, List<JourneyOptionDTO>> grouped = new LinkedHashMap<>();
        for (JourneyOptionDTO option : ordered) {
            String key = patternSignature(option);
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(option);
        }

        List<JourneyOptionDTO> unique = new ArrayList<>();
        for (Map.Entry<String, List<JourneyOptionDTO>> entry : grouped.entrySet()) {
            List<JourneyOptionDTO> bucket = entry.getValue();
            JourneyOptionDTO primary = bucket.get(0);
            List<String> alternativeBoardings = collectAlternativeBoardings(bucket);
            if (!alternativeBoardings.isEmpty() || primary.alternativeBoardingTimes() == null) {
                primary = new JourneyOptionDTO(
                        primary.durationMinutes(),
                        primary.walkMinutes(),
                        primary.waitingMinutes(),
                        primary.transfers(),
                        primary.startTime(),
                        primary.endTime(),
                        primary.legs(),
                        alternativeBoardings.isEmpty() ? null : alternativeBoardings
                );
            }
            unique.add(primary);
            if (unique.size() >= limit) {
                break;
            }
        }
        return unique;
    }

    /**
     * Firma "pattern" che identifica itinerari sostanzialmente equivalenti
     * (stessa sequenza di linee/headsign/fermate), indipendentemente dall'orario.
     * Le alternative degli stessi pattern diventano alternativeBoardingTimes.
     */
    private static String patternSignature(JourneyOptionDTO option) {
        StringBuilder out = new StringBuilder();
        out.append(option.transfers() != null ? option.transfers() : -1);
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
                    .append(normalizeKey(leg.toName()));
        }
        return out.toString();
    }

    private static List<String> collectAlternativeBoardings(List<JourneyOptionDTO> bucket) {
        if (bucket.size() <= 1) return List.of();
        JourneyOptionDTO primary = bucket.get(0);
        String primaryFirstTransitStart = firstTransitLegStart(primary);
        TreeSet<String> times = new TreeSet<>();
        for (int i = 1; i < bucket.size(); i++) {
            String t = firstTransitLegStart(bucket.get(i));
            if (t == null) continue;
            if (t.equals(primaryFirstTransitStart)) continue;
            times.add(t);
            if (times.size() >= 5) break;
        }
        return new ArrayList<>(times);
    }

    private static String firstTransitLegStart(JourneyOptionDTO option) {
        if (option.legs() == null) return null;
        for (JourneyLegDTO leg : option.legs()) {
            String mode = leg.mode();
            if (mode != null && !"WALK".equalsIgnoreCase(mode) && leg.startTime() != null) {
                return leg.startTime();
            }
        }
        return option.startTime();
    }

    private static String normalizeKey(String value) {
        return value == null ? "" : value.trim().toLowerCase();
    }

    private static boolean isWalkOnly(JourneyOptionDTO option) {
        return option.legs() != null
                && !option.legs().isEmpty()
                && option.legs().stream().allMatch(leg -> "WALK".equalsIgnoreCase(leg.mode()));
    }

    /**
     * Penalità invertita: itinerari con tutti i transit-leg in realtime ottengono il valore minore.
     * Usato come tie-breaker nel comparator (lower-is-better).
     */
    private static int realtimeBonus(JourneyOptionDTO option) {
        if (option.legs() == null || option.legs().isEmpty()) return 0;
        int transit = 0;
        int rt = 0;
        for (JourneyLegDTO leg : option.legs()) {
            if (leg.mode() == null || "WALK".equalsIgnoreCase(leg.mode())) continue;
            transit++;
            if (Boolean.TRUE.equals(leg.realtime())) rt++;
        }
        if (transit == 0) return 0;
        if (rt == transit) return -2; // tutti realtime → bonus massimo
        if (rt > 0) return -1;        // parziale
        return 0;
    }

    private static int journeyTransitPreferenceScore(JourneyOptionDTO option) {
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

        boolean hasRail = modes.contains("SUBWAY") || modes.contains("RAIL") || modes.contains("TRAM");
        boolean hasBus = modes.contains("BUS");
        int score = 0;
        if (hasRail) score -= 2;
        if (!hasBus && !hasRail) score += 4;
        score += Math.max(0, modes.size() - 1);
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
        return String.format(Locale.ROOT, "%.6f", value);
    }

    private static String formatNumber(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
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
