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

    /**
     * Posizione (0-based) entro cui garantire la visibilità della migliore opzione treno (RAIL).
     * Es. 2 = il treno appare al più come terza opzione, senza scalzare la #0 (la più veloce).
     */
    @Value("${journey.otp.rail-visibility-slot:2}")
    private int railVisibilitySlot;

    /**
     * Quanto può arrivare più tardi (in minuti) la migliore opzione treno rispetto alla
     * migliore opzione in assoluto per essere comunque promossa tra i risultati visibili.
     * Oltre questa soglia il treno non viene forzato in alto (sarebbe poco sensato).
     */
    @Value("${journey.otp.rail-visibility-max-delay-minutes:45}")
    private long railVisibilityMaxDelayMinutes;

    /**
     * Workaround: quando il piano normale NON contiene alcun treno ma partenza e arrivo sono
     * entrambi vicini a una stazione ferroviaria, costruisce un'opzione "treno" sintetica
     * (cammino -> treno stazione->stazione -> cammino), bypassando il caso limite di routing
     * access/egress di OTP sui salti brevi. Disattivabile mettendo il flag a false.
     */
    @Value("${journey.otp.rail-injection-enabled:true}")
    private boolean railInjectionEnabled;

    /** Raggio massimo (metri) entro cui cercare la stazione ferroviaria di salita/discesa. */
    @Value("${journey.otp.rail-injection-max-station-meters:1200}")
    private int railInjectionMaxStationMeters;

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

        // Workaround iniezione treno: solo se non c'è già un treno tra le opzioni.
        if (railInjectionEnabled && options.stream().noneMatch(JourneyPlannerService::hasRailLeg)) {
            JourneyOptionDTO injected = tryBuildInjectedRailOption(
                    fromLat, fromLon, toLat, toLon, preference);
            if (injected != null) {
                List<JourneyOptionDTO> withRail = new ArrayList<>(options);
                int slot = Math.min(Math.max(0, railVisibilitySlot), withRail.size());
                withRail.add(slot, injected);
                while (withRail.size() > requested) {
                    withRail.remove(withRail.size() - 1);
                }
                options = withRail;
                log.debug("[JourneyPlanner] Injected synthetic rail option at slot {}", slot);
            }
        }

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

    /**
     * Epoch millis dell'orario di arrivo (endTime ISO 8601) usato come criterio
     * primario di ordinamento. Le opzioni senza arrivo valido finiscono in fondo.
     */
    private static long arrivalEpochMillis(JourneyOptionDTO option) {
        String end = option.endTime();
        if (end == null || end.isBlank()) return Long.MAX_VALUE;
        try {
            return Instant.parse(end).toEpochMilli();
        } catch (Exception ex) {
            return Long.MAX_VALUE;
        }
    }

    private List<JourneyOptionDTO> dedupeAndEnrich(List<JourneyOptionDTO> rawOptions, int limit) {
        if (rawOptions.isEmpty()) {
            return List.of();
        }
        List<JourneyOptionDTO> ordered = rawOptions.stream()
                .sorted(Comparator
                        .comparing((JourneyOptionDTO option) -> isWalkOnly(option))
                        .thenComparingLong(JourneyPlannerService::arrivalEpochMillis)
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

        // Deduplica TUTTI i pattern (niente break sul limite): così la migliore opzione treno
        // non viene scartata prima ancora di essere costruita e resta candidabile alla promozione.
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
        }

        List<JourneyOptionDTO> result = new ArrayList<>(unique.subList(0, Math.min(limit, unique.size())));
        promoteBestRailOption(unique, result, limit);
        return result;
    }

    /**
     * Garantisce che la migliore opzione "treno" (leg RAIL) sia visibile tra i primi risultati.
     * <p>
     * Il ranking primario è per orario di arrivo, quindi un treno leggermente più lento di un
     * bus/metro finirebbe in fondo (o oltre il limite) e l'utente lo percepisce come "non suggerito".
     * Qui, se la migliore opzione treno non è già entro {@code railVisibilitySlot}, la si promuove a
     * quella posizione — senza toccare la #0 (l'opzione più veloce resta prima) e solo se il treno è
     * competitivo (arrivo entro {@code railVisibilityMaxDelayMinutes} dalla migliore opzione).
     */
    private void promoteBestRailOption(List<JourneyOptionDTO> rankedUnique, List<JourneyOptionDTO> result, int limit) {
        if (result.isEmpty()) {
            return;
        }
        JourneyOptionDTO bestRail = rankedUnique.stream()
                .filter(JourneyPlannerService::hasRailLeg)
                .findFirst()
                .orElse(null);
        if (bestRail == null) {
            return;
        }

        int slot = Math.min(Math.max(0, railVisibilitySlot), Math.max(0, result.size() - 1));
        int currentIdx = result.indexOf(bestRail);
        if (currentIdx >= 0 && currentIdx <= slot) {
            return; // già visibile in alto, niente da fare
        }

        long railArrival = arrivalEpochMillis(bestRail);
        if (railArrival == Long.MAX_VALUE) {
            return; // senza orario di arrivo valido non forziamo la posizione
        }
        long bestArrival = arrivalEpochMillis(result.get(0));
        if (bestArrival != Long.MAX_VALUE) {
            long deltaMinutes = (railArrival - bestArrival) / 60_000L;
            if (deltaMinutes > railVisibilityMaxDelayMinutes) {
                return; // treno troppo più lento: non sensato spingerlo in alto
            }
        }

        if (currentIdx >= 0) {
            result.remove(currentIdx);
        }
        int insertAt = Math.min(slot, result.size());
        result.add(insertAt, bestRail);
        while (result.size() > limit) {
            result.remove(result.size() - 1);
        }
        log.debug("[JourneyPlanner] Rail option promoted to slot {} for visibility", insertAt);
    }

    private static boolean hasRailLeg(JourneyOptionDTO option) {
        if (option.legs() == null) {
            return false;
        }
        for (JourneyLegDTO leg : option.legs()) {
            if ("RAIL".equalsIgnoreCase(leg.mode())) {
                return true;
            }
        }
        return false;
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

    // =====================================================================
    // Workaround iniezione treno (salti brevi tra stazioni vicine)
    // =====================================================================

    private record RailStation(String name, double lat, double lon) {
    }

    private JourneyOptionDTO tryBuildInjectedRailOption(
            double fromLat, double fromLon, double toLat, double toLon, TimePreference preference) {
        try {
            RailStation board = nearestRailStation(fromLat, fromLon);
            RailStation alight = nearestRailStation(toLat, toLon);
            if (board == null || alight == null) {
                return null;
            }
            if (board.name().trim().equalsIgnoreCase(alight.name().trim())) {
                return null; // stessa stazione: niente da iniettare
            }

            int accessMin = walkMinutesBetween(fromLat, fromLon, board.lat(), board.lon());
            int egressMin = walkMinutesBetween(alight.lat(), alight.lon(), toLat, toLon);

            List<JourneyLegDTO> railLegs = railLegsBetween(board, alight, preference);
            if (railLegs.isEmpty()) {
                return null;
            }

            // Scegli il primo treno che si fa davvero in tempo a prendere: deve partire dopo
            // l'orario richiesto + il tempo di cammino fino alla stazione di salita.
            Instant earliestCatch = preference.when().plus(Duration.ofMinutes(accessMin));
            JourneyLegDTO rail = null;
            for (JourneyLegDTO leg : railLegs) {
                Instant dep = parseInstantOrNull(leg.startTime());
                if (dep != null && !dep.isBefore(earliestCatch)) {
                    rail = leg;
                    break;
                }
            }
            if (rail == null) {
                // Nessun treno prendibile entro la finestra dato il cammino d'accesso: non iniettare.
                return null;
            }
            String railStart = rail.startTime();
            String railEnd = rail.endTime();
            if (railStart == null || railEnd == null) {
                return null;
            }

            String accessStart = shiftIso(railStart, -accessMin);
            String egressEnd = shiftIso(railEnd, egressMin);

            JourneyLegDTO accessWalk = new JourneyLegDTO(
                    "WALK", null, null, null, null, null, null, null, null, null,
                    null, board.name(), accessStart, railStart, accessMin, null,
                    fromLat, fromLon, board.lat(), board.lon(), null, List.of());
            JourneyLegDTO egressWalk = new JourneyLegDTO(
                    "WALK", null, null, null, null, null, null, null, null, null,
                    alight.name(), null, railEnd, egressEnd, egressMin, null,
                    alight.lat(), alight.lon(), toLat, toLon, null, List.of());

            Integer railMin = rail.durationMinutes() != null ? rail.durationMinutes() : diffMinutes(railStart, railEnd);
            int total = accessMin + (railMin != null ? railMin : 0) + egressMin;

            Instant chosenDep = parseInstantOrNull(railStart);
            List<String> altBoardings = railLegs.stream()
                    .map(JourneyLegDTO::startTime)
                    .filter(t -> {
                        Instant i = parseInstantOrNull(t);
                        return i != null && chosenDep != null && i.isAfter(chosenDep);
                    })
                    .distinct()
                    .limit(5)
                    .toList();

            return new JourneyOptionDTO(
                    total,
                    accessMin + egressMin,
                    null,
                    0,
                    accessStart,
                    egressEnd,
                    List.of(accessWalk, rail, egressWalk),
                    altBoardings.isEmpty() ? null : altBoardings);
        } catch (Exception e) {
            log.debug("[JourneyPlanner] rail injection skipped: {}", e.toString());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private RailStation nearestRailStation(double lat, double lon) {
        String q = String.format(Locale.ROOT,
                "{ stopsByRadius(lat: %s, lon: %s, radius: %d) { edges { node { distance stop { name lat lon vehicleMode } } } } }",
                trimDecimals(lat), trimDecimals(lon), railInjectionMaxStationMeters);
        Map<String, Object> payload = postGraphQl(q);
        if (payload == null) return null;
        Map<String, Object> data = payload.get("data") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        Map<String, Object> sbr = data.get("stopsByRadius") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        List<Map<String, Object>> edges = sbr.get("edges") instanceof List<?> l
                ? (List<Map<String, Object>>) (List<?>) l : List.of();
        for (Map<String, Object> edge : edges) {
            if (!(edge.get("node") instanceof Map<?, ?> nodeRaw)) continue;
            Map<String, Object> node = (Map<String, Object>) nodeRaw;
            if (!(node.get("stop") instanceof Map<?, ?> stopRaw)) continue;
            Map<String, Object> stop = (Map<String, Object>) stopRaw;
            String mode = toStringOrNull(stop.get("vehicleMode"));
            if (mode == null || !mode.equalsIgnoreCase("RAIL")) continue;
            Double slat = toDoubleOrNull(stop.get("lat"));
            Double slon = toDoubleOrNull(stop.get("lon"));
            if (slat == null || slon == null) continue;
            return new RailStation(firstNonBlank(toStringOrNull(stop.get("name")), "Stazione"), slat, slon);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private List<JourneyLegDTO> railLegsBetween(RailStation a, RailStation b, TimePreference preference) {
        String q = String.format(Locale.ROOT, """
                { planConnection(
                    origin:{location:{coordinate:{latitude:%s longitude:%s}}}
                    destination:{location:{coordinate:{latitude:%s longitude:%s}}}
                    dateTime:{ %s:"%s" } searchWindow:"%s"
                    modes:{ transitOnly:true, transit:{ access:[WALK], egress:[WALK], transfer:WALK, transit:[{mode:RAIL}] } }
                    first:5
                  ){ edges{ node{ legs{ mode duration from{name} to{name} route{shortName longName agency{name gtfsId}} trip{tripShortName gtfsId} legGeometry{points} start{scheduledTime estimated{time}} end{scheduledTime estimated{time}} } } } } }
                """,
                trimDecimals(a.lat()), trimDecimals(a.lon()), trimDecimals(b.lat()), trimDecimals(b.lon()),
                preference.graphQlField(), preference.when().toString(), searchWindow);
        Map<String, Object> payload = postGraphQl(q);
        if (payload == null) return List.of();
        Map<String, Object> data = payload.get("data") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        Map<String, Object> plan = data.get("planConnection") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        List<Map<String, Object>> edges = plan.get("edges") instanceof List<?> l
                ? (List<Map<String, Object>>) (List<?>) l : List.of();
        List<JourneyLegDTO> out = new ArrayList<>();
        for (Map<String, Object> edge : edges) {
            if (!(edge.get("node") instanceof Map<?, ?> nodeRaw)) continue;
            Map<String, Object> node = (Map<String, Object>) nodeRaw;
            List<Map<String, Object>> legs = node.get("legs") instanceof List<?> l
                    ? (List<Map<String, Object>>) (List<?>) l : List.of();
            for (Map<String, Object> leg : legs) {
                if (!"RAIL".equalsIgnoreCase(toStringOrNull(leg.get("mode")))) continue;
                String lineCode = null, routeLong = null, agencyName = null, agencyId = null;
                if (leg.get("route") instanceof Map<?, ?> route) {
                    lineCode = toStringOrNull(route.get("shortName"));
                    routeLong = toStringOrNull(route.get("longName"));
                    if (route.get("agency") instanceof Map<?, ?> agency) {
                        agencyName = toStringOrNull(agency.get("name"));
                        agencyId = toStringOrNull(agency.get("gtfsId"));
                    }
                }
                String tripShort = null, tripId = null;
                if (leg.get("trip") instanceof Map<?, ?> trip) {
                    tripShort = toStringOrNull(trip.get("tripShortName"));
                    tripId = toStringOrNull(trip.get("gtfsId"));
                }
                String geometry = null;
                if (leg.get("legGeometry") instanceof Map<?, ?> g) {
                    geometry = toStringOrNull(g.get("points"));
                }
                String start = otpLegTime(leg.get("start"));
                String end = otpLegTime(leg.get("end"));
                String fromName = leg.get("from") instanceof Map<?, ?> f ? toStringOrNull(((Map<String, Object>) f).get("name")) : a.name();
                String toName = leg.get("to") instanceof Map<?, ?> t ? toStringOrNull(((Map<String, Object>) t).get("name")) : b.name();
                Integer dur = floatMinutes(leg.get("duration"));
                if (dur == null) dur = diffMinutes(start, end);
                out.add(new JourneyLegDTO(
                        "RAIL", lineCode, lineCode, routeLong, agencyName, agencyId, tripShort, tripId,
                        Boolean.TRUE, null, firstNonBlank(fromName, a.name()), firstNonBlank(toName, b.name()),
                        start, end, dur, null,
                        a.lat(), a.lon(), b.lat(), b.lon(), geometry, List.of()));
                break; // una sola leg RAIL per itinerario
            }
        }
        return out;
    }

    private int walkMinutesBetween(double lat1, double lon1, double lat2, double lon2) {
        String q = String.format(Locale.ROOT, """
                { planConnection(
                    origin:{location:{coordinate:{latitude:%s longitude:%s}}}
                    destination:{location:{coordinate:{latitude:%s longitude:%s}}}
                    first:3
                  ){ edges{ node{ duration legs{ mode } } } } }
                """,
                trimDecimals(lat1), trimDecimals(lon1), trimDecimals(lat2), trimDecimals(lon2));
        Map<String, Object> payload = postGraphQl(q);
        Integer best = parseWalkOnlyMinutes(payload);
        if (best != null) return best;
        // fallback: stima da distanza in linea d'aria con fattore detour
        double meters = haversineMeters(lat1, lon1, lat2, lon2) * 1.3;
        return (int) Math.max(1, Math.round(meters / Math.max(0.5, walkSpeed) / 60.0));
    }

    @SuppressWarnings("unchecked")
    private static Integer parseWalkOnlyMinutes(Map<String, Object> payload) {
        if (payload == null) return null;
        Map<String, Object> data = payload.get("data") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        Map<String, Object> plan = data.get("planConnection") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        List<Map<String, Object>> edges = plan.get("edges") instanceof List<?> l
                ? (List<Map<String, Object>>) (List<?>) l : List.of();
        Integer best = null;
        for (Map<String, Object> edge : edges) {
            if (!(edge.get("node") instanceof Map<?, ?> nodeRaw)) continue;
            Map<String, Object> node = (Map<String, Object>) nodeRaw;
            List<Map<String, Object>> legs = node.get("legs") instanceof List<?> l
                    ? (List<Map<String, Object>>) (List<?>) l : List.of();
            boolean walkOnly = !legs.isEmpty() && legs.stream()
                    .allMatch(lg -> {
                        String m = toStringOrNull(lg.get("mode"));
                        return m != null && (m.equalsIgnoreCase("WALK") || m.equalsIgnoreCase("foot"));
                    });
            if (!walkOnly) continue;
            Integer dur = floatMinutes(node.get("duration"));
            if (dur != null && (best == null || dur < best)) best = dur;
        }
        return best;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> postGraphQl(String query) {
        try {
            URI uri = resolveOtpGraphQlUri();
            Map<String, Object> request = new LinkedHashMap<>();
            request.put("query", query);
            return webClient.post()
                    .uri(uri)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("User-Agent", "gtfs-monitor/1.0 (rail-injection)")
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block(Duration.ofSeconds(Math.max(1, otpTimeoutSeconds)));
        } catch (Exception e) {
            log.debug("[JourneyPlanner] rail-injection GraphQL failed: {}", e.toString());
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static String otpLegTime(Object holder) {
        if (!(holder instanceof Map<?, ?> raw)) return null;
        Map<String, Object> m = (Map<String, Object>) raw;
        if (m.get("estimated") instanceof Map<?, ?> est) {
            String t = toStringOrNull(((Map<String, Object>) est).get("time"));
            if (t != null) return t;
        }
        return toStringOrNull(m.get("scheduledTime"));
    }

    private static String shiftIso(String iso, long minutes) {
        if (iso == null) return null;
        try {
            return OffsetDateTime.parse(iso).plusMinutes(minutes).toString();
        } catch (Exception e) {
            return iso;
        }
    }

    private static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double r = 6371000.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double h = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return 2 * r * Math.asin(Math.min(1.0, Math.sqrt(h)));
    }
}
