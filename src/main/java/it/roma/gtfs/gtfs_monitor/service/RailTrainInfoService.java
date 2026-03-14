package it.roma.gtfs.gtfs_monitor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.roma.gtfs.gtfs_monitor.model.dto.RailStationPlatformDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.RailTrainInfoDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.HtmlUtils;
import org.springframework.web.util.UriComponentsBuilder;

import java.text.Normalizer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class RailTrainInfoService {
    private static final Pattern H1_PATTERN = Pattern.compile("(?is)<h1>(.*?)</h1>");
    private static final Pattern BODY_DIV_PATTERN = Pattern.compile("(?is)<div\\s+class=\"corpocentrale\"[^>]*>(.*?)</div>");
    private static final Pattern STATUS_DIV_PATTERN = Pattern.compile("(?is)<div\\s+class=\"evidenziato\"[^>]*>(.*?)</div>");
    private static final Pattern DELAY_PATTERN = Pattern.compile("con\\s+(\\d+)\\s+minut[oi]\\s+di\\s+ritardo", Pattern.CASE_INSENSITIVE);
    private static final Pattern EARLY_PATTERN = Pattern.compile("con\\s+(\\d+)\\s+minut[oi]\\s+di\\s+anticipo", Pattern.CASE_INSENSITIVE);
    private static final Pattern LAST_SEEN_PATTERN = Pattern.compile(
            "(?:ultima\\s+rilevazione|ultimo\\s+rilevamento)\\s+a\\s+(.+?)\\s+alle\\s+(\\d{2}:\\d{2})",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern AUTOCOMPLETE_ENTRY_PATTERN = Pattern.compile("^(.*?)\\|(\\d+)-([A-Z0-9]+)-(\\d+)$");
    private static final Duration CACHE_TTL = Duration.ofSeconds(45);
    private static final ZoneId ROME_ZONE = ZoneId.of("Europe/Rome");

    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final Map<String, CachedRailTrainInfo> cache = new ConcurrentHashMap<>();

    @Value("${rail.viaggiatreno.enabled:true}")
    private boolean viaggiatrenoEnabled;

    @Value("${rail.viaggiatreno.base-url:https://www.viaggiatreno.it}")
    private String viaggiatrenoBaseUrl;

    @Value("${rail.viaggiatreno.api-base-url:http://www.viaggiatreno.it}")
    private String viaggiatrenoApiBaseUrl;

    public Optional<RailTrainInfoDTO> lookupTrainInfo(String trainNumber, String stationName, OffsetDateTime referenceTime) {
        String normalizedTrainNumber = normalizeTrainNumber(trainNumber);
        if (!viaggiatrenoEnabled || normalizedTrainNumber == null) {
            return Optional.empty();
        }

        String cacheKey = normalizedTrainNumber + "|" + normalizeStationKey(stationName) + "|" + normalizeReferenceTimeKey(referenceTime);
        CachedRailTrainInfo cached = cache.get(cacheKey);
        if (cached != null && cached.expiresAt().isAfter(Instant.now())) {
            return Optional.ofNullable(cached.value());
        }

        RailTrainInfoDTO parsed = fetchTrainInfo(normalizedTrainNumber, stationName, referenceTime);
        cache.put(cacheKey, new CachedRailTrainInfo(parsed, Instant.now().plus(CACHE_TTL)));
        return Optional.ofNullable(parsed);
    }

    private RailTrainInfoDTO fetchTrainInfo(String trainNumber, String stationName, OffsetDateTime referenceTime) {
        try {
            RailTrainInfoDTO apiParsed = fetchTrainInfoFromApi(trainNumber, stationName, referenceTime);
            if (apiParsed != null) {
                return apiParsed;
            }
        } catch (Exception ex) {
            log.debug("[RailTrainInfo] Viaggiatreno JSON lookup failed for {}: {}", trainNumber, ex.toString());
        }

        if (!canUseHtmlFallback(referenceTime)) {
            return null;
        }

        try {
            String html = webClient.get()
                    .uri(UriComponentsBuilder.fromHttpUrl(viaggiatrenoBaseUrl)
                            .path("/vt_pax_internet/mobile/scheda")
                            .queryParam("lang", "IT")
                            .queryParam("numeroTreno", trainNumber)
                            .queryParam("tipoRicerca", "numero")
                            .build(true)
                            .toUri())
                    .accept(MediaType.TEXT_HTML, MediaType.APPLICATION_XHTML_XML)
                    .header("User-Agent", "Mozilla/5.0 (compatible; WeMoveRoma/1.0)")
                    .retrieve()
                    .bodyToMono(String.class)
                    .block(Duration.ofSeconds(12));

            if (html == null || html.isBlank()) {
                return null;
            }

            RailTrainInfoDTO parsed = parseTrainInfo(html, trainNumber, stationName);
            if (parsed == null || !matchesReferenceTime(parsed, referenceTime)) {
                return null;
            }
            return parsed;
        } catch (Exception ex) {
            log.debug("[RailTrainInfo] Viaggiatreno lookup failed for {}: {}", trainNumber, ex.toString());
            return null;
        }
    }

    private static boolean canUseHtmlFallback(OffsetDateTime referenceTime) {
        if (referenceTime == null) {
            return true;
        }

        return referenceTime.atZoneSameInstant(ROME_ZONE).toLocalDate()
                .equals(OffsetDateTime.now(ROME_ZONE).toLocalDate());
    }

    private RailTrainInfoDTO fetchTrainInfoFromApi(String trainNumber, String stationName, OffsetDateTime referenceTime) throws Exception {
        List<TrainSearchCandidate> candidates = fetchSearchCandidates(trainNumber);
        if (candidates.isEmpty()) {
            return null;
        }

        RailTrainInfoDTO best = null;
        int bestScore = Integer.MIN_VALUE;
        for (TrainSearchCandidate candidate : candidates) {
            RailTrainInfoDTO parsed = fetchTrainInfoForCandidate(candidate, stationName);
            if (parsed == null) {
                continue;
            }
            int score = scoreCandidate(candidate, parsed, stationName, referenceTime);
            if (score > bestScore) {
                best = parsed;
                bestScore = score;
            }
        }

        return bestScore > Integer.MIN_VALUE ? best : null;
    }

    private List<TrainSearchCandidate> fetchSearchCandidates(String trainNumber) {
        String payload = webClient.get()
                .uri(UriComponentsBuilder.fromUriString(viaggiatrenoApiBaseUrl)
                        .path("/infomobilita/resteasy/viaggiatreno/cercaNumeroTrenoTrenoAutocomplete/")
                        .path(trainNumber)
                        .build(true)
                        .toUri())
                .accept(MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON)
                .header("User-Agent", "Mozilla/5.0 (compatible; WeMoveRoma/1.0)")
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(12));

        if (payload == null || payload.isBlank()) {
            return List.of();
        }

        List<TrainSearchCandidate> candidates = new ArrayList<>();
        for (String rawLine : payload.split("\\R")) {
            String line = rawLine.trim();
            if (line.isEmpty()) {
                continue;
            }
            Matcher matcher = AUTOCOMPLETE_ENTRY_PATTERN.matcher(line);
            if (!matcher.matches()) {
                continue;
            }
            Long departureEpochMillis = safeParseLong(matcher.group(4));
            if (departureEpochMillis == null) {
                continue;
            }
            candidates.add(new TrainSearchCandidate(
                    matcher.group(1).trim(),
                    matcher.group(2).trim(),
                    matcher.group(3).trim(),
                    departureEpochMillis
            ));
        }
        return candidates;
    }

    private RailTrainInfoDTO fetchTrainInfoForCandidate(TrainSearchCandidate candidate, String stationName) throws Exception {
        String payload = webClient.get()
                .uri(UriComponentsBuilder.fromUriString(viaggiatrenoApiBaseUrl)
                        .path("/infomobilita/resteasy/viaggiatreno/andamentoTreno/")
                        .path(candidate.originCode())
                        .path("/")
                        .path(candidate.trainNumber())
                        .path("/")
                        .path(candidate.departureEpochMillis().toString())
                        .build(true)
                        .toUri())
                .accept(MediaType.APPLICATION_JSON)
                .header("User-Agent", "Mozilla/5.0 (compatible; WeMoveRoma/1.0)")
                .retrieve()
                .bodyToMono(String.class)
                .block(Duration.ofSeconds(12));

        if (payload == null || payload.isBlank()) {
            return null;
        }

        JsonNode root = objectMapper.readTree(payload);
        if (root == null || !root.isObject()) {
            return null;
        }

        JsonNode stops = root.path("fermate");
        if (!stops.isArray() || stops.isEmpty()) {
            return null;
        }

        RailStationPlatformDTO departure = stationFromApiStop(stops.get(0), StationContext.DEPARTURE);
        RailStationPlatformDTO arrival = stationFromApiStop(stops.get(stops.size() - 1), StationContext.ARRIVAL);
        RailStationPlatformDTO requested = requestedStationFromStops(stops, stationName);
        List<RailStationPlatformDTO> intermediateStops = intermediateStopsFromStops(stops, stationName);

        return new RailTrainInfoDTO(
                "viaggiatreno-api",
                firstNonBlank(text(root, "numeroTreno"), candidate.trainNumber()),
                firstNonBlank(text(root, "compNumeroTreno"), text(root, "categoria")),
                statusSummaryFromApi(root),
                integerValue(root, "ritardo"),
                text(root, "stazioneUltimoRilevamento"),
                text(root, "compOraUltimoRilevamento"),
                departure,
                arrival,
                requested,
                intermediateStops
        );
    }

    private static int scoreCandidate(
            TrainSearchCandidate searchCandidate,
            RailTrainInfoDTO candidate,
            String stationName,
            OffsetDateTime referenceTime
    ) {
        if (candidate == null) {
            return Integer.MIN_VALUE;
        }

        int score = 0;
        if (referenceTime != null) {
            if (!matchesReferenceDate(searchCandidate, referenceTime)) {
                return Integer.MIN_VALUE;
            }
            score += 80;
            if (matchesReferenceTime(candidate, referenceTime)) {
                score += 40;
            } else {
                score -= 60;
            }
        }

        if (candidate.requestedStation() != null && normalizeStationKey(stationName) != null) {
            score += 100;
        }

        return score;
    }

    private static RailStationPlatformDTO requestedStationFromStops(JsonNode stops, String stationName) {
        String normalizedRequested = normalizeStationKey(stationName);
        if (normalizedRequested == null || stops == null || !stops.isArray()) {
            return null;
        }

        for (JsonNode stop : stops) {
            String normalizedCandidate = normalizeStationKey(text(stop, "stazione"));
            if (normalizedCandidate == null) {
                continue;
            }
            if (Objects.equals(normalizedRequested, normalizedCandidate)
                    || normalizedRequested.contains(normalizedCandidate)
                    || normalizedCandidate.contains(normalizedRequested)) {
                return stationFromApiStop(stop, StationContext.REQUESTED);
            }
        }
        return null;
    }

    private static List<RailStationPlatformDTO> intermediateStopsFromStops(JsonNode stops, String stationName) {
        if (stops == null || !stops.isArray() || stops.size() < 3) {
            return List.of();
        }

        String normalizedRequested = normalizeStationKey(stationName);
        if (normalizedRequested == null) {
            return List.of();
        }

        int startIndex = -1;
        for (int index = 0; index < stops.size(); index += 1) {
            String normalizedCandidate = normalizeStationKey(text(stops.get(index), "stazione"));
            if (normalizedCandidate == null) {
                continue;
            }
            if (Objects.equals(normalizedRequested, normalizedCandidate)
                    || normalizedRequested.contains(normalizedCandidate)
                    || normalizedCandidate.contains(normalizedRequested)) {
                startIndex = index;
                break;
            }
        }

        if (startIndex < 0 || startIndex >= stops.size() - 2) {
            return List.of();
        }

        List<RailStationPlatformDTO> intermediateStops = new ArrayList<>();
        for (int index = startIndex + 1; index < stops.size() - 1; index += 1) {
            RailStationPlatformDTO platform = stationFromApiStop(stops.get(index), StationContext.REQUESTED);
            if (platform != null) {
                intermediateStops.add(platform);
            }
        }
        return intermediateStops;
    }

    private static RailStationPlatformDTO stationFromApiStop(JsonNode stop, StationContext context) {
        if (stop == null || stop.isMissingNode() || stop.isNull()) {
            return null;
        }

        String stationName = text(stop, "stazione");
        if (stationName == null) {
            return null;
        }

        boolean useDepartureFields = switch (context) {
            case DEPARTURE -> true;
            case ARRIVAL -> false;
            case REQUESTED -> preferDepartureForRequestedStop(stop);
        };

        String scheduledLabel = useDepartureFields ? "Partenza programmata" : "Arrivo programmato";
        String actualLabel = useDepartureFields
                ? actualLabelForStop(stop, "Partenza effettiva", "Partenza prevista", "partenzaReale")
                : actualLabelForStop(stop, "Arrivo effettivo", "Arrivo previsto", "arrivoReale");

        String scheduledTime = useDepartureFields
                ? firstNonBlank(formatMillis(longValue(stop, "partenza_teorica")), formatMillis(longValue(stop, "programmata")))
                : firstNonBlank(formatMillis(longValue(stop, "arrivo_teorico")), formatMillis(longValue(stop, "programmata")));

        String actualTime = useDepartureFields
                ? firstNonBlank(formatMillis(longValue(stop, "partenzaReale")), formatMillis(longValue(stop, "effettiva")))
                : firstNonBlank(formatMillis(longValue(stop, "arrivoReale")), formatMillis(longValue(stop, "effettiva")));

        String plannedPlatform = useDepartureFields
                ? sanitizePlatform(text(stop, "binarioProgrammatoPartenzaDescrizione"))
                : sanitizePlatform(text(stop, "binarioProgrammatoArrivoDescrizione"));
        String actualPlatform = useDepartureFields
                ? sanitizePlatform(text(stop, "binarioEffettivoPartenzaDescrizione"))
                : sanitizePlatform(text(stop, "binarioEffettivoArrivoDescrizione"));

        if (actualPlatform != null && plannedPlatform != null && "30".equals(actualPlatform) && !"30".equals(plannedPlatform)) {
            actualPlatform = null;
        }

        return new RailStationPlatformDTO(
                stationName,
                scheduledTime == null ? null : scheduledLabel,
                scheduledTime,
                actualTime == null ? null : actualLabel,
                actualTime,
                plannedPlatform,
                actualPlatform
        );
    }

    private static boolean preferDepartureForRequestedStop(JsonNode stop) {
        String stopType = text(stop, "tipoFermata");
        if ("P".equalsIgnoreCase(stopType)) {
            return true;
        }
        if ("A".equalsIgnoreCase(stopType)) {
            return false;
        }
        boolean hasArrivalInfo = hasAny(
                stop,
                "arrivoReale",
                "arrivo_teorico",
                "binarioProgrammatoArrivoDescrizione",
                "binarioEffettivoArrivoDescrizione"
        );
        if (hasArrivalInfo) {
            return false;
        }
        return hasAny(stop, "partenzaReale", "partenza_teorica", "binarioProgrammatoPartenzaDescrizione", "binarioEffettivoPartenzaDescrizione");
    }

    private static String statusSummaryFromApi(JsonNode root) {
        String explicit = text(root, "statoTreno");
        if (explicit != null) {
            return explicit;
        }
        if (root.path("nonPartito").asBoolean(false)) {
            return "Il treno non è ancora partito";
        }
        if (root.path("arrivato").asBoolean(false)) {
            return "Il treno è arrivato";
        }
        if (root.path("inStazione").asBoolean(false)) {
            String station = text(root, "stazioneUltimoRilevamento");
            return station == null ? "Il treno è in stazione" : "Il treno è in stazione a " + station;
        }
        String andamento = firstArrayText(root, "compRitardoAndamento");
        if (andamento != null) {
            return andamento;
        }
        String ritardo = firstArrayText(root, "compRitardo");
        if (ritardo != null) {
            return ritardo;
        }
        Integer delayMinutes = integerValue(root, "ritardo");
        if (delayMinutes != null) {
            if (delayMinutes == 0) {
                return "In orario";
            }
            if (delayMinutes > 0) {
                return delayMinutes == 1 ? "Ritardo di 1 minuto" : "Ritardo di " + delayMinutes + " minuti";
            }
            int early = Math.abs(delayMinutes);
            return early == 1 ? "Anticipo di 1 minuto" : "Anticipo di " + early + " minuti";
        }
        return null;
    }

    private static String actualLabelForStop(JsonNode stop, String actualLabel, String predictedLabel, String actualField) {
        if (longValue(stop, actualField) != null) {
            return actualLabel;
        }
        if (longValue(stop, "effettiva") != null) {
            return predictedLabel;
        }
        return predictedLabel;
    }

    private static boolean hasAny(JsonNode node, String... fields) {
        for (String field : fields) {
            if (text(node, field) != null || longValue(node, field) != null) {
                return true;
            }
        }
        return false;
    }

    private static String text(JsonNode node, String field) {
        if (node == null || field == null) {
            return null;
        }
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        return blankToNull(value.asText());
    }

    private static String firstArrayText(JsonNode node, String field) {
        if (node == null || field == null) {
            return null;
        }
        JsonNode array = node.path(field);
        if (!array.isArray()) {
            return null;
        }
        for (JsonNode item : array) {
            String value = blankToNull(item.asText());
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private static Integer integerValue(JsonNode node, String field) {
        if (node == null || field == null) {
            return null;
        }
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (value.isInt() || value.isLong()) {
            return value.asInt();
        }
        return safeParseInt(value.asText());
    }

    private static Long longValue(JsonNode node, String field) {
        if (node == null || field == null) {
            return null;
        }
        JsonNode value = node.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (value.isLong() || value.isInt()) {
            long parsed = value.asLong();
            return parsed <= 0 ? null : parsed;
        }
        Long parsed = safeParseLong(value.asText());
        return parsed != null && parsed > 0 ? parsed : null;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            String cleaned = blankToNull(value);
            if (cleaned != null) {
                return cleaned;
            }
        }
        return null;
    }

    private static Long safeParseLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (Exception ex) {
            return null;
        }
    }

    private static String formatMillis(Long value) {
        if (value == null || value <= 0) {
            return null;
        }
        return Instant.ofEpochMilli(value)
                .atZone(ROME_ZONE)
                .toLocalTime()
                .withSecond(0)
                .withNano(0)
                .toString();
    }

    private static String sanitizePlatform(String value) {
        String platform = blankToNull(value);
        if (platform == null) {
            return null;
        }
        return platform.replaceAll("\\s+", " ").trim();
    }

    private static RailTrainInfoDTO parseTrainInfo(String html, String trainNumber, String stationName) {
        String cleanedHtml = stripBom(html);
        String trainLabel = extractFirstText(cleanedHtml, H1_PATTERN);
        List<String> bodyBlocks = extractAll(cleanedHtml, BODY_DIV_PATTERN);
        if (bodyBlocks.isEmpty() && (trainLabel == null || trainLabel.isBlank())) {
            return null;
        }

        RailStationPlatformDTO departure = bodyBlocks.size() > 0 ? parseStationBlock(bodyBlocks.get(0)) : null;
        RailStationPlatformDTO arrival = bodyBlocks.size() > 1 ? parseStationBlock(bodyBlocks.get(1)) : null;
        String statusSummary = extractStatusSummary(cleanedHtml);
        Integer delayMinutes = parseDelayMinutes(statusSummary);
        String[] lastSeen = parseLastSeen(statusSummary);
        RailStationPlatformDTO requested = matchRequestedStation(stationName, departure, arrival);

        if (!isPlausibleHtmlStationBlock(departure) && !isPlausibleHtmlStationBlock(arrival)) {
            return null;
        }

        return new RailTrainInfoDTO(
                "viaggiatreno",
                trainNumber,
                blankToNull(trainLabel),
                blankToNull(statusSummary),
                delayMinutes,
                blankToNull(lastSeen[0]),
                blankToNull(lastSeen[1]),
                departure,
                arrival,
                requested,
                List.of()
        );
    }

    private static RailStationPlatformDTO parseStationBlock(String blockHtml) {
        List<String> lines = blockLines(blockHtml);
        if (lines.isEmpty()) {
            return null;
        }

        String stationName = lines.getFirst();
        LabeledValue scheduled = findLabeledValueAfter(lines, "Partenza programmata", "Arrivo programmato");
        LabeledValue actual = findLabeledValueAfter(lines, "Partenza effettiva", "Arrivo effettivo", "Partenza prevista", "Arrivo previsto");
        return new RailStationPlatformDTO(
                stationName,
                scheduled.label(),
                scheduled.value(),
                actual.label(),
                actual.value(),
                findValueAfter(lines, "Binario Previsto"),
                findValueAfter(lines, "Binario Reale")
        );
    }

    private static boolean isPlausibleHtmlStationBlock(RailStationPlatformDTO station) {
        if (station == null) {
            return false;
        }

        String stationName = blankToNull(station.stationName());
        if (stationName == null || looksLikeStationLabel(stationName)) {
            return false;
        }

        return isPlausibleTime(station.scheduledTime())
                || isPlausibleTime(station.actualTime())
                || blankToNull(station.plannedPlatform()) != null
                || blankToNull(station.actualPlatform()) != null;
    }

    private static boolean looksLikeStationLabel(String value) {
        String simplified = simplifyLabel(value);
        return simplified.startsWith("partenza programmata")
                || simplified.startsWith("partenza effettiva")
                || simplified.startsWith("partenza prevista")
                || simplified.startsWith("arrivo programmato")
                || simplified.startsWith("arrivo effettivo")
                || simplified.startsWith("arrivo previsto")
                || simplified.startsWith("binario previsto")
                || simplified.startsWith("binario reale");
    }

    private static boolean isPlausibleTime(String value) {
        String candidate = blankToNull(value);
        if (candidate == null) {
            return false;
        }
        return candidate.matches("\\d{2}:\\d{2}");
    }

    private static List<String> blockLines(String html) {
        String normalized = html
                .replaceAll("(?i)<br\\s*/?>", "\n")
                .replaceAll("(?i)</p>", "\n")
                .replaceAll("(?i)</h2>", "\n")
                .replaceAll("(?i)</strong>", "\n")
                .replaceAll("(?i)</div>", "\n")
                .replaceAll("(?is)<[^>]+>", " ");
        String unescaped = HtmlUtils.htmlUnescape(stripBom(normalized))
                .replace('\u00A0', ' ')
                .replace("&#039;", "'")
                .replace("’", "'");

        List<String> lines = new ArrayList<>();
        for (String raw : unescaped.split("\\R")) {
            String line = raw.replaceAll("\\s+", " ").trim();
            if (!line.isEmpty()) {
                lines.add(line);
            }
        }
        return mergeSplitLabels(lines);
    }

    private static List<String> mergeSplitLabels(List<String> lines) {
        if (lines.isEmpty()) {
            return lines;
        }

        List<String> merged = new ArrayList<>();
        for (int index = 0; index < lines.size(); index += 1) {
            String current = lines.get(index);
            String currentLabel = simplifyLabel(current);
            if (index + 1 < lines.size()) {
                String next = lines.get(index + 1);
                String nextLabel = simplifyLabel(next);

                if ("binario".equals(currentLabel) && ("previsto".equals(nextLabel) || "previsto:".equals(nextLabel) || "reale".equals(nextLabel) || "reale:".equals(nextLabel))) {
                    merged.add((current + " " + next).replaceAll("\\s+", " ").trim());
                    index += 1;
                    continue;
                }
            }

            merged.add(current);
        }
        return merged;
    }

    private static String extractStatusSummary(String html) {
        for (String block : extractAll(html, STATUS_DIV_PATTERN)) {
            String text = joinLines(blockLines(block));
            if (text.toLowerCase(Locale.ITALIAN).contains("il treno")) {
                return text;
            }
        }
        return null;
    }

    private static Integer parseDelayMinutes(String statusSummary) {
        if (statusSummary == null || statusSummary.isBlank()) {
            return null;
        }
        String normalized = statusSummary.toLowerCase(Locale.ITALIAN);
        if (normalized.contains("in orario")) {
            return 0;
        }

        Matcher delay = DELAY_PATTERN.matcher(normalized);
        if (delay.find()) {
            return safeParseInt(delay.group(1));
        }

        Matcher early = EARLY_PATTERN.matcher(normalized);
        if (early.find()) {
            Integer value = safeParseInt(early.group(1));
            return value == null ? null : -value;
        }
        return null;
    }

    private static String[] parseLastSeen(String statusSummary) {
        if (statusSummary == null || statusSummary.isBlank()) {
            return new String[] {null, null};
        }
        Matcher matcher = LAST_SEEN_PATTERN.matcher(statusSummary);
        if (matcher.find()) {
            return new String[] {
                    blankToNull(matcher.group(1)),
                    blankToNull(matcher.group(2))
            };
        }
        return new String[] {null, null};
    }

    private static RailStationPlatformDTO matchRequestedStation(
            String stationName,
            RailStationPlatformDTO departure,
            RailStationPlatformDTO arrival
    ) {
        String normalizedRequested = normalizeStationKey(stationName);
        if (normalizedRequested == null) {
            return null;
        }
        if (stationMatches(normalizedRequested, departure)) {
            return departure;
        }
        if (stationMatches(normalizedRequested, arrival)) {
            return arrival;
        }
        return null;
    }

    private static boolean stationMatches(String normalizedRequested, RailStationPlatformDTO station) {
        if (station == null) {
            return false;
        }
        String normalizedCandidate = normalizeStationKey(station.stationName());
        if (normalizedCandidate == null) {
            return false;
        }
        return Objects.equals(normalizedRequested, normalizedCandidate)
                || normalizedRequested.contains(normalizedCandidate)
                || normalizedCandidate.contains(normalizedRequested);
    }

    private static String findValueAfter(List<String> lines, String... labels) {
        return findLabeledValueAfter(lines, labels).value();
    }

    private static LabeledValue findLabeledValueAfter(List<String> lines, String... labels) {
        for (int index = 0; index < lines.size(); index += 1) {
            String current = simplifyLabel(lines.get(index));
            for (String label : labels) {
                if (current.startsWith(simplifyLabel(label))) {
                    for (int valueIndex = index + 1; valueIndex < lines.size(); valueIndex += 1) {
                        String candidate = lines.get(valueIndex).trim();
                        if (!candidate.isEmpty() && !isLabel(candidate)) {
                            return new LabeledValue(cleanLabel(lines.get(index)), blankToNull(candidate));
                        }
                    }
                }
            }
        }
        return new LabeledValue(null, null);
    }

    private static boolean isLabel(String value) {
        String simplified = simplifyLabel(value);
        return simplified.startsWith("partenza programmata")
                || simplified.startsWith("partenza effettiva")
                || simplified.startsWith("partenza prevista")
                || simplified.startsWith("arrivo programmato")
                || simplified.startsWith("arrivo effettivo")
                || simplified.startsWith("arrivo previsto")
                || simplified.startsWith("binario previsto")
                || simplified.startsWith("binario reale");
    }

    private static String simplifyLabel(String value) {
        return blankToNull(value) == null
                ? ""
                : value.toLowerCase(Locale.ITALIAN).replace(":", "").replaceAll("\\s+", " ").trim();
    }

    private static String cleanLabel(String value) {
        String raw = blankToNull(value);
        if (raw == null) {
            return null;
        }
        return raw.replaceAll(":\\s*$", "").replaceAll("\\s+", " ").trim();
    }

    private static String extractFirstText(String html, Pattern pattern) {
        Matcher matcher = pattern.matcher(html);
        if (!matcher.find()) {
            return null;
        }
        return blankToNull(HtmlUtils.htmlUnescape(matcher.group(1).replaceAll("(?is)<[^>]+>", " ").replaceAll("\\s+", " ").trim()));
    }

    private static List<String> extractAll(String html, Pattern pattern) {
        Matcher matcher = pattern.matcher(html);
        List<String> values = new ArrayList<>();
        while (matcher.find()) {
            values.add(matcher.group(1));
        }
        return values;
    }

    private static String joinLines(List<String> lines) {
        return blankToNull(String.join(" ", lines).replaceAll("\\s+", " ").trim());
    }

    private static Integer safeParseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (Exception ex) {
            return null;
        }
    }

    private static String normalizeTrainNumber(String trainNumber) {
        String raw = blankToNull(trainNumber);
        if (raw == null) {
            return null;
        }
        Matcher matcher = Pattern.compile("(\\d{3,})").matcher(raw);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return raw.replaceAll("\\s+", "");
    }

    private static String normalizeReferenceTimeKey(OffsetDateTime referenceTime) {
        return referenceTime == null ? "" : referenceTime.toString();
    }

    private static boolean matchesReferenceTime(RailTrainInfoDTO info, OffsetDateTime referenceTime) {
        if (info == null || referenceTime == null) {
            return true;
        }

        LocalTime departureTime = parseStationTime(info.departure());
        LocalTime arrivalTime = parseStationTime(info.arrival());
        if (departureTime == null || arrivalTime == null) {
            return true;
        }

        LocalDateTime departure = referenceTime.toLocalDate().atTime(departureTime);
        LocalDateTime arrival = referenceTime.toLocalDate().atTime(arrivalTime);
        if (arrival.isBefore(departure)) {
            arrival = arrival.plusDays(1);
        }

        LocalDateTime reference = referenceTime.toLocalDateTime();
        return !reference.isBefore(departure.minusMinutes(30)) && !reference.isAfter(arrival.plusMinutes(30));
    }

    private static boolean matchesReferenceDate(TrainSearchCandidate candidate, OffsetDateTime referenceTime) {
        if (candidate == null || candidate.departureEpochMillis() == null || referenceTime == null) {
            return true;
        }

        return Instant.ofEpochMilli(candidate.departureEpochMillis())
                .atZone(ROME_ZONE)
                .toLocalDate()
                .equals(referenceTime.atZoneSameInstant(ROME_ZONE).toLocalDate());
    }

    private static LocalTime parseStationTime(RailStationPlatformDTO station) {
        if (station == null) {
            return null;
        }
        String candidate = blankToNull(station.actualTime());
        if (candidate == null) {
            candidate = blankToNull(station.scheduledTime());
        }
        if (candidate == null) {
            return null;
        }
        try {
            return LocalTime.parse(candidate);
        } catch (Exception ex) {
            return null;
        }
    }

    private static String normalizeStationKey(String value) {
        String raw = blankToNull(value);
        if (raw == null) {
            return null;
        }
        String normalized = Normalizer.normalize(raw, Normalizer.Form.NFD)
                .replaceAll("\\p{M}+", "")
                .replaceAll("[^\\p{Alnum}]+", " ")
                .replaceAll("\\s+", " ")
                .trim()
                .toUpperCase(Locale.ITALIAN);
        return normalized.isBlank() ? null : normalized;
    }

    private static String stripBom(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        return value.charAt(0) == '\uFEFF' ? value.substring(1) : value;
    }

    private static String blankToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isBlank() || "--".equals(trimmed) || "-".equals(trimmed)) {
            return null;
        }
        return trimmed;
    }

    private record CachedRailTrainInfo(RailTrainInfoDTO value, Instant expiresAt) {
    }

    private record LabeledValue(String label, String value) {
    }

    private record TrainSearchCandidate(
            String displayLabel,
            String trainNumber,
            String originCode,
            Long departureEpochMillis
    ) {
    }

    private enum StationContext {
        DEPARTURE,
        ARRIVAL,
        REQUESTED
    }
}
