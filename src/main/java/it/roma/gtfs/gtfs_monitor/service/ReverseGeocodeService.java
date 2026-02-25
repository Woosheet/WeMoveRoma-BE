package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.ReverseGeocodeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReverseGeocodeService {
    private static final String NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse";
    private static final long TTL_MILLIS = Duration.ofHours(6).toMillis();

    private final WebClient webClient;
    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();

    public ReverseGeocodeDTO reverse(double lat, double lon) {
        String key = cacheKey(lat, lon);
        CacheEntry cached = cache.get(key);
        long now = System.currentTimeMillis();
        if (cached != null && (now - cached.loadedAtMillis) < TTL_MILLIS) {
            return cached.value;
        }

        ReverseGeocodeDTO value = fetchReverse(lat, lon);
        if (value != null) {
            cache.put(key, new CacheEntry(value, now));
        }
        return value != null ? value : new ReverseGeocodeDTO(lat, lon, null, null, null, null);
    }

    @SuppressWarnings("unchecked")
    private ReverseGeocodeDTO fetchReverse(double lat, double lon) {
        try {
            Map<String, Object> payload = webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .scheme("https")
                            .host("nominatim.openstreetmap.org")
                            .path("/reverse")
                            .queryParam("format", "jsonv2")
                            .queryParam("lat", lat)
                            .queryParam("lon", lon)
                            .queryParam("addressdetails", 1)
                            .queryParam("accept-language", "it")
                            .build())
                    .header("User-Agent", "gtfs-monitor/1.0 (reverse-geocode)")
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block(Duration.ofSeconds(8));

            if (payload == null) {
                return null;
            }

            String displayName = asString(payload.get("display_name"));
            Map<String, Object> address = payload.get("address") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
            String road = firstNonBlank(
                    asString(address.get("road")),
                    asString(address.get("pedestrian")),
                    asString(address.get("footway")),
                    asString(address.get("path")),
                    asString(address.get("residential"))
            );
            String city = firstNonBlank(
                    asString(address.get("city")),
                    asString(address.get("town")),
                    asString(address.get("village")),
                    asString(address.get("municipality")),
                    asString(address.get("county"))
            );
            String country = asString(address.get("country"));

            String label = firstNonBlank(compactLabel(road, city), displayName);
            return new ReverseGeocodeDTO(lat, lon, label, road, city, country);
        } catch (Exception e) {
            log.debug("[ReverseGeocode] fallback coordinate-only for {}, {}: {}", lat, lon, e.toString());
            return null;
        }
    }

    private static String compactLabel(String street, String city) {
        if (street == null && city == null) return null;
        if (street == null) return city;
        if (city == null) return street;
        return street + ", " + city;
    }

    private static String asString(Object value) {
        if (value == null) return null;
        String s = Objects.toString(value, null);
        return (s == null || s.isBlank()) ? null : s.trim();
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    private static String cacheKey(double lat, double lon) {
        double latR = Math.round(lat * 10000.0d) / 10000.0d;
        double lonR = Math.round(lon * 10000.0d) / 10000.0d;
        return latR + "|" + lonR;
    }

    private record CacheEntry(ReverseGeocodeDTO value, long loadedAtMillis) {}
}
