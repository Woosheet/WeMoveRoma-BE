package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.GeocodeSearchResultDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class GeocodeSearchService {
    // Roma + cintura (municipi e comuni limitrofi utili) per rendere la ricerca più precisa/reattiva.
    private static final double ROME_MIN_LON = 12.20;
    private static final double ROME_MAX_LON = 12.90;
    private static final double ROME_MIN_LAT = 41.65;
    private static final double ROME_MAX_LAT = 42.15;

    private final WebClient webClient;

    @SuppressWarnings("unchecked")
    public List<GeocodeSearchResultDTO> search(String query, Integer limit, Double biasLat, Double biasLon) {
        String q = query == null ? "" : query.trim();
        if (q.isBlank()) return List.of();
        int cappedLimit = limit == null || limit <= 0 ? 5 : Math.min(limit, 10);
        int upstreamLimit = Math.max(cappedLimit * 3, 10);

        try {
            List<String> queries = queryVariants(q);

            Map<String, RankedResult> dedup = new LinkedHashMap<>();
            for (int queryIndex = 0; queryIndex < queries.size(); queryIndex++) {
                String qVariant = queries.get(queryIndex);
                List<Map<String, Object>> payload = fetchNominatim(qVariant, upstreamLimit);
                if (payload == null) continue;

                for (Map<String, Object> row : payload) {
                    Double lat = parseDouble(row.get("lat"));
                    Double lon = parseDouble(row.get("lon"));
                    String label = toStringOrNull(row.get("display_name"));
                    if (lat == null || lon == null || label == null || label.isBlank()) continue;
                    if (!isInsideRomeBounds(lat, lon)) continue;
                    if (!looksLikeRomeResult(row, label)) continue;

                    String normalizedLabel = compactLabel(row, label);
                    String key = normalizeKey(normalizedLabel);
                    int score = rankResult(q, row, normalizedLabel, biasLat, biasLon);
                    score += queryIndex * 35; // query originale sempre preferita rispetto ai fallback

                    RankedResult candidate = new RankedResult(new GeocodeSearchResultDTO(lat, lon, normalizedLabel), score);
                    RankedResult prev = dedup.get(key);
                    if (prev == null || candidate.score < prev.score) {
                        dedup.put(key, candidate);
                    }
                }
            }
            return dedup.values().stream()
                    .sorted(Comparator.comparingInt(v -> v.score))
                    .map(v -> v.result)
                    .limit(cappedLimit)
                    .toList();
        } catch (Exception e) {
            log.debug("[GeocodeSearch] search failed for '{}': {}", q, e.toString());
            return List.of();
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> fetchNominatim(String query, int upstreamLimit) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .scheme("https")
                        .host("nominatim.openstreetmap.org")
                        .path("/search")
                        .queryParam("format", "jsonv2")
                        .queryParam("q", query)
                        .queryParam("limit", upstreamLimit)
                        .queryParam("addressdetails", 1)
                        .queryParam("accept-language", "it")
                        .queryParam("countrycodes", "it")
                        .queryParam("viewbox", "%s,%s,%s,%s".formatted(ROME_MIN_LON, ROME_MAX_LAT, ROME_MAX_LON, ROME_MIN_LAT))
                        .queryParam("bounded", 1)
                        .build())
                .header("User-Agent", "gtfs-monitor/1.0 (geocode-search)")
                .retrieve()
                .bodyToMono(List.class)
                .block(Duration.ofSeconds(8));
    }

    @SuppressWarnings("unchecked")
    private static String compactLabel(Map<String, Object> row, String displayName) {
        if (!(row.get("address") instanceof Map<?, ?> rawAddr)) {
            return displayName;
        }
        Map<String, Object> address = (Map<String, Object>) rawAddr;
        String road = firstNonBlank(
                toStringOrNull(address.get("road")),
                toStringOrNull(address.get("pedestrian")),
                toStringOrNull(address.get("footway")),
                toStringOrNull(address.get("path")),
                toStringOrNull(address.get("residential"))
        );
        String house = toStringOrNull(address.get("house_number"));
        String suburb = firstNonBlank(
                toStringOrNull(address.get("suburb")),
                toStringOrNull(address.get("quarter")),
                toStringOrNull(address.get("city_district")),
                toStringOrNull(address.get("neighbourhood"))
        );
        String city = firstNonBlank(
                toStringOrNull(address.get("city")),
                toStringOrNull(address.get("municipality")),
                toStringOrNull(address.get("town"))
        );

        List<String> parts = new ArrayList<>(4);
        if (road != null) {
            parts.add(house != null ? (road + " " + house) : road);
        }
        if (suburb != null && !equalsIgnoreCase(suburb, city)) {
            parts.add(suburb);
        }
        if (city != null) {
            parts.add(city);
        } else {
            parts.add("Roma");
        }
        if (parts.isEmpty()) return displayName;
        return String.join(", ", parts);
    }

    @SuppressWarnings("unchecked")
    private static boolean looksLikeRomeResult(Map<String, Object> row, String label) {
        Object addressObj = row.get("address");
        if (addressObj instanceof Map<?, ?> rawAddr) {
            Map<String, Object> address = (Map<String, Object>) rawAddr;
            String city = firstNonBlank(
                    toStringOrNull(address.get("city")),
                    toStringOrNull(address.get("municipality")),
                    toStringOrNull(address.get("town")),
                    toStringOrNull(address.get("county"))
            );
            if (city != null) {
                String c = city.toLowerCase();
                if (c.contains("roma")) return true;
            }
        }
        return label.toLowerCase().contains("roma");
    }

    private static boolean isInsideRomeBounds(double lat, double lon) {
        return lat >= ROME_MIN_LAT && lat <= ROME_MAX_LAT && lon >= ROME_MIN_LON && lon <= ROME_MAX_LON;
    }

    @SuppressWarnings("unchecked")
    private static int rankResult(String query, Map<String, Object> row, String label, Double biasLat, Double biasLon) {
        String q = normalizeKey(query);
        String l = normalizeKey(label);
        int score = 1000;

        Map<String, Object> address = row.get("address") instanceof Map<?, ?> raw ? (Map<String, Object>) raw : Map.of();
        String road = firstNonBlank(
                toStringOrNull(address.get("road")),
                toStringOrNull(address.get("pedestrian")),
                toStringOrNull(address.get("footway")),
                toStringOrNull(address.get("path")),
                toStringOrNull(address.get("residential"))
        );
        String houseNumber = toStringOrNull(address.get("house_number"));

        String qHouse = extractHouseNumberToken(query);
        String qStreet = normalizeStreetQuery(query);
        String roadNorm = normalizeKey(road);
        String houseNorm = normalizeHouseNumber(houseNumber);

        if (qStreet != null && !qStreet.isBlank()) {
            if (roadNorm.equals(qStreet)) score -= 350;
            else if (roadNorm.startsWith(qStreet)) score -= 240;
            else if (roadNorm.contains(qStreet)) score -= 120;
        }

        if (qHouse != null) {
            if (houseNorm != null && houseNorm.equals(normalizeHouseNumber(qHouse))) {
                score -= 500;
            } else if (houseNorm != null && normalizeHouseNumber(qHouse).startsWith(houseNorm)) {
                score -= 120;
            } else {
                // Query contiene civico ma il risultato non lo ha: penalizza molto.
                score += 260;
            }
        }

        if (l.startsWith(q)) score -= 300;
        if (l.contains(q)) score -= 120;
        if (l.contains("roma")) score -= 40;
        if (l.contains("municipio")) score -= 10;
        score += Math.abs(l.length() - q.length()) / 4;

        if (biasLat != null && biasLon != null) {
            Double lat = parseDouble(row.get("lat"));
            Double lon = parseDouble(row.get("lon"));
            if (lat != null && lon != null) {
                int meters = haversineMeters(biasLat, biasLon, lat, lon);
                score += Math.min(220, meters / 60); // bias morbido: privilegia risultati vicini senza rompere il civico
            }
        }
        return score;
    }

    private static List<String> queryVariants(String query) {
        LinkedHashSet<String> variants = new LinkedHashSet<>();
        variants.add(query.trim());

        String normalizedSpaces = query.replaceAll("[,;]+", " ").replaceAll("\\s+", " ").trim();
        variants.add(normalizedSpaces);

        String expanded = normalizedSpaces
                .replaceAll("(?i)\\bp\\.za\\b", "piazza")
                .replaceAll("(?i)\\bv\\.le\\b", "viale")
                .replaceAll("(?i)\\bv\\.?\\b", "via")
                .replaceAll("(?i)\\bl\\.go\\b", "largo")
                .replaceAll("(?i)\\bstaz\\.ne\\b", "stazione");
        variants.add(expanded);

        String withRoma = expanded.toLowerCase().contains("roma") ? expanded : (expanded + " Roma");
        variants.add(withRoma);

        String withoutHouse = normalizedStreetQuery(query);
        if (withoutHouse != null && !withoutHouse.isBlank()) {
            variants.add(withoutHouse);
            if (!withoutHouse.toLowerCase().contains("roma")) variants.add(withoutHouse + " Roma");
        }

        return variants.stream().filter(v -> v != null && !v.isBlank()).limit(5).toList();
    }

    private static String extractHouseNumberToken(String query) {
        if (query == null) return null;
        String[] parts = query.trim().split("\\s+");
        for (int i = parts.length - 1; i >= 0; i--) {
            String p = parts[i].replaceAll("[,.;]", "");
            if (p.matches("(?i)\\d+[a-z]{0,3}")) {
                return p;
            }
        }
        return null;
    }

    private static String normalizeStreetQuery(String query) {
        if (query == null) return null;
        String q = query.trim();
        String house = extractHouseNumberToken(q);
        if (house != null) {
            q = q.replaceFirst("(?i)\\b" + java.util.regex.Pattern.quote(house) + "\\b\\s*$", "").trim();
        }
        return normalizeKey(q);
    }

    private static String normalizedStreetQuery(String query) {
        if (query == null) return null;
        String q = query.trim().replaceAll("[,;]+", " ").replaceAll("\\s+", " ");
        String house = extractHouseNumberToken(q);
        if (house != null) {
            q = q.replaceFirst("(?i)\\b" + java.util.regex.Pattern.quote(house) + "\\b\\s*$", "").trim();
        }
        return q;
    }

    private static String normalizeHouseNumber(String value) {
        if (value == null) return null;
        String out = value.toLowerCase().replaceAll("\\s+", "");
        return out.isBlank() ? null : out;
    }

    private static String normalizeKey(String value) {
        return Objects.toString(value, "")
                .toLowerCase()
                .replaceAll("[^\\p{L}\\p{N}\\s]", " ")
                .replaceAll("\\s+", " ")
                .trim();
    }

    private static Double parseDouble(Object value) {
        if (value == null) return null;
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (Exception e) {
            return null;
        }
    }

    private static String toStringOrNull(Object value) {
        if (value == null) return null;
        String s = String.valueOf(value).trim();
        return s.isBlank() ? null : s;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return null;
    }

    private static boolean equalsIgnoreCase(String a, String b) {
        return a != null && b != null && a.equalsIgnoreCase(b);
    }

    private static int haversineMeters(double lat1, double lon1, double lat2, double lon2) {
        double r = 6371000.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return (int) Math.round(r * c);
    }

    private record RankedResult(GeocodeSearchResultDTO result, int score) {}
}
