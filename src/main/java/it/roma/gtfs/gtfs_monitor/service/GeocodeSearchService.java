package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.GeocodeSearchResultDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriBuilder;

import java.text.Normalizer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Slf4j
@Service
@RequiredArgsConstructor
public class GeocodeSearchService {
    // Roma + cintura (municipi e comuni limitrofi utili) per rendere la ricerca più precisa/reattiva.
    private static final double ROME_MIN_LON = 12.20;
    private static final double ROME_MAX_LON = 12.90;
    private static final double ROME_MIN_LAT = 41.65;
    private static final double ROME_MAX_LAT = 42.15;
    private static final long SEARCH_CACHE_TTL_MILLIS = 30_000L;
    private static final long UPSTREAM_CACHE_TTL_MILLIS = 120_000L;

    // Comuni della cintura accettati anche se non contengono "roma" nella label/city.
    private static final Set<String> ROME_METRO_TOWNS = Set.of(
            "fiumicino", "ciampino", "pomezia", "guidonia montecelio", "guidonia",
            "tivoli", "albano laziale", "ardea", "anguillara sabazia", "frascati",
            "grottaferrata", "marino", "mentana", "monterotondo", "monte porzio catone",
            "nettuno", "anzio", "castel gandolfo", "cerveteri", "ladispoli",
            "formello", "campagnano di roma", "sacrofano", "rocca di papa", "genzano di roma",
            "colleferro", "palestrina", "valmontone", "zagarolo", "fonte nuova"
    );

    private final WebClient webClient;
    private final Map<String, TimedValue<List<GeocodeSearchResultDTO>>> searchCache = new ConcurrentHashMap<>();
    private final Map<String, TimedValue<List<Map<String, Object>>>> upstreamCache = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public List<GeocodeSearchResultDTO> search(String query, Integer limit, Double biasLat, Double biasLon) {
        String q = query == null ? "" : query.trim();
        if (q.isBlank()) return List.of();
        int cappedLimit = limit == null || limit <= 0 ? 5 : Math.min(limit, 10);
        int upstreamLimit = Math.max(cappedLimit * 3, 10);
        String cacheKey = buildSearchCacheKey(q, cappedLimit, biasLat, biasLon);
        List<GeocodeSearchResultDTO> cachedResult = getCached(searchCache, cacheKey, SEARCH_CACHE_TTL_MILLIS);
        if (cachedResult != null) {
            return cachedResult;
        }

        try {
            Map<String, RankedResult> dedup = new LinkedHashMap<>();
            boolean rateLimited = false;
            int variantIndex = 0;

            // 0) Photon (komoot): autocomplete prefix-based, ottimo per POI, stazioni e
            //    nomi parziali (es. "Via dei Grac" → "Via dei Gracchi"). Lo interroghiamo
            //    per primo così le sue feature partono col variantIndex più basso (preferite a
            //    parità di score). Gli errori di Photon non bloccano la pipeline Nominatim.
            VariantFetch photonResult = fetchPhoton(q, upstreamLimit, biasLat, biasLon);
            rateLimited = collectResults(photonResult, dedup, q, biasLat, biasLon, variantIndex);
            variantIndex++;

            // 1) Tentativo prioritario: structured search (street/city) per indirizzi.
            //    Nominatim risolve molto meglio civici italiani con i parametri tipizzati.
            ParsedAddress parsed = parseAddress(q);
            if (!rateLimited && parsed != null && parsed.street() != null) {
                List<ParsedAddress> structuredVariants = expandStructuredVariants(parsed);
                for (ParsedAddress variant : structuredVariants) {
                    VariantFetch payloadResult = fetchNominatimStructured(variant, upstreamLimit, true);
                    rateLimited = collectResults(payloadResult, dedup, q, biasLat, biasLon, variantIndex);
                    variantIndex++;
                    if (rateLimited) break;
                }
            }

            // 2) Free-text search con varianti (mantiene la copertura per POI/quartieri/landmark).
            if (!rateLimited) {
                List<String> queries = queryVariants(q);
                for (String qVariant : queries) {
                    VariantFetch payloadResult = fetchNominatim(qVariant, upstreamLimit, true);
                    rateLimited = collectResults(payloadResult, dedup, q, biasLat, biasLon, variantIndex);
                    variantIndex++;
                    if (rateLimited) break;
                }
            }

            // 3) Fallback unbounded se non abbiamo trovato nulla: rilassa il viewbox e ritenta una sola variante.
            if (!rateLimited && dedup.isEmpty()) {
                VariantFetch payloadResult = fetchNominatim(q, upstreamLimit, false);
                collectResults(payloadResult, dedup, q, biasLat, biasLon, variantIndex);
            }

            List<GeocodeSearchResultDTO> results = dedup.values().stream()
                    .sorted(Comparator.comparingInt(v -> v.score))
                    .map(v -> v.result)
                    .limit(cappedLimit)
                    .toList();
            searchCache.put(cacheKey, new TimedValue<>(results, System.currentTimeMillis()));
            return results;
        } catch (Exception e) {
            log.warn("[GeocodeSearch] failed q='{}': {}", q, e.toString(), e);
            List<GeocodeSearchResultDTO> staleResult = getCached(searchCache, cacheKey, Long.MAX_VALUE);
            if (staleResult != null) {
                return staleResult;
            }
            return List.of();
        }
    }

    /**
     * Aggrega i risultati di una singola fetch dentro la mappa dedup; ritorna true se rate-limited.
     */
    private boolean collectResults(
            VariantFetch payloadResult,
            Map<String, RankedResult> dedup,
            String originalQuery,
            Double biasLat,
            Double biasLon,
            int variantIndex
    ) {
        if (payloadResult == null) return false;
        List<Map<String, Object>> payload = payloadResult.rows();
        if (payload == null) return payloadResult.rateLimited();

        for (Map<String, Object> row : payload) {
            Double lat = parseDouble(row.get("lat"));
            Double lon = parseDouble(row.get("lon"));
            String label = toStringOrNull(row.get("display_name"));
            if (lat == null || lon == null || label == null || label.isBlank()) continue;
            if (!isInsideRomeBounds(lat, lon)) continue;
            if (!looksLikeRomeResult(row, label)) continue;

            String normalizedLabel = compactLabel(row, label);
            String key = normalizeKey(normalizedLabel);
            int score = rankResult(originalQuery, row, normalizedLabel, biasLat, biasLon);
            score += variantIndex * 35; // varianti precedenti (più rilevanti) sono preferite

            String poiName = extractPoiDisplayName(row, normalizedLabel);
            RankedResult candidate = new RankedResult(new GeocodeSearchResultDTO(lat, lon, normalizedLabel, poiName), score);
            RankedResult prev = dedup.get(key);
            if (prev == null || candidate.score < prev.score) {
                dedup.put(key, candidate);
            }
        }
        return payloadResult.rateLimited();
    }

    @SuppressWarnings("unchecked")
    private VariantFetch fetchNominatim(String query, int upstreamLimit, boolean bounded) {
        String cacheKey = "ft|" + normalizeKey(query) + "|" + upstreamLimit + "|" + bounded;
        return doFetch(cacheKey, uriBuilder -> uriBuilder
                .scheme("https")
                .host("nominatim.openstreetmap.org")
                .path("/search")
                .queryParam("format", "jsonv2")
                .queryParam("q", query)
                .queryParam("limit", upstreamLimit)
                .queryParam("addressdetails", 1)
                .queryParam("namedetails", 1)
                .queryParam("accept-language", "it")
                .queryParam("countrycodes", "it")
                .queryParam("viewbox", "%s,%s,%s,%s".formatted(ROME_MIN_LON, ROME_MAX_LAT, ROME_MAX_LON, ROME_MIN_LAT))
                .queryParam("bounded", bounded ? 1 : 0)
                .build(), query);
    }

    /**
     * Structured search: usa i parametri tipizzati di Nominatim (street/city).
     * Funziona molto meglio del freetext per indirizzi italiani con civico.
     */
    private VariantFetch fetchNominatimStructured(ParsedAddress address, int upstreamLimit, boolean bounded) {
        String streetParam = buildStreetParam(address);
        String cityParam = address.city() != null ? address.city() : "Roma";
        String cacheKey = "st|" + normalizeKey(streetParam) + "|" + normalizeKey(cityParam) + "|" + upstreamLimit + "|" + bounded;
        return doFetch(cacheKey, uriBuilder -> uriBuilder
                .scheme("https")
                .host("nominatim.openstreetmap.org")
                .path("/search")
                .queryParam("format", "jsonv2")
                .queryParam("street", streetParam)
                .queryParam("city", cityParam)
                .queryParam("country", "Italia")
                .queryParam("limit", upstreamLimit)
                .queryParam("addressdetails", 1)
                .queryParam("namedetails", 1)
                .queryParam("accept-language", "it")
                .queryParam("countrycodes", "it")
                .queryParam("viewbox", "%s,%s,%s,%s".formatted(ROME_MIN_LON, ROME_MAX_LAT, ROME_MAX_LON, ROME_MIN_LAT))
                .queryParam("bounded", bounded ? 1 : 0)
                .build(), "street=" + streetParam + "&city=" + cityParam);
    }

    /**
     * Photon (komoot) free-text search. Punti forti rispetto a Nominatim:
     *   - prefix/autocomplete: "Via dei Grac" trova "Via dei Gracchi";
     *   - POI/stazioni: "Stazione Lepanto" intercetta la fermata metro "Lepanto";
     *   - tokenizzazione più "italiana" sui nomi composti.
     *
     * Le feature GeoJSON vengono adattate alla stessa shape Nominatim così tutto il
     * resto della pipeline (compactLabel, looksLikeRomeResult, rankResult, dedup) funziona
     * senza modifiche.
     */
    @SuppressWarnings("unchecked")
    private VariantFetch fetchPhoton(String query, int upstreamLimit, Double biasLat, Double biasLon) {
        // Photon "lang" supporta solo default/de/en/fr (non it). Lasciamo il default.
        // Appendiamo "Roma" se mancante: empiricamente migliora drasticamente i match per POI
        // come "Stazione Lepanto" (con bbox stretto, la query nuda torna 0 risultati).
        String photonQuery = query;
        if (!photonQuery.toLowerCase(Locale.ROOT).contains("roma")) {
            photonQuery = photonQuery + " Roma";
        }
        final String finalQuery = photonQuery;
        String cacheKey = "ph|" + normalizeKey(finalQuery) + "|" + upstreamLimit
                + "|" + bucketBias(biasLat) + "|" + bucketBias(biasLon);
        try {
            Map<String, Object> payload = webClient.get()
                    .uri(uriBuilder -> {
                        UriBuilder b = uriBuilder
                                .scheme("https")
                                .host("photon.komoot.io")
                                .path("/api/")
                                .queryParam("q", finalQuery)
                                .queryParam("limit", upstreamLimit)
                                // bbox di Photon: minLon,minLat,maxLon,maxLat (diverso dal viewbox Nominatim).
                                .queryParam("bbox", "%s,%s,%s,%s".formatted(
                                        ROME_MIN_LON, ROME_MIN_LAT, ROME_MAX_LON, ROME_MAX_LAT));
                        if (biasLat != null && biasLon != null) {
                            b = b.queryParam("lat", biasLat).queryParam("lon", biasLon);
                        }
                        return b.build();
                    })
                    .header("User-Agent", "gtfs-monitor/1.0 (geocode-search)")
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block(Duration.ofSeconds(6));
            List<Map<String, Object>> rows = photonFeaturesToRows((Map<String, Object>) payload);
            List<Map<String, Object>> safeRows = List.copyOf(rows);
            upstreamCache.put(cacheKey, new TimedValue<>(safeRows, System.currentTimeMillis()));
            return new VariantFetch(safeRows, false);
        } catch (WebClientResponseException.TooManyRequests e) {
            List<Map<String, Object>> stale = getCached(upstreamCache, cacheKey, Long.MAX_VALUE);
            if (stale != null) {
                log.warn("[GeocodeSearch][Photon] 429 for '{}', using stale cache ({} result(s))",
                        query, stale.size());
                return new VariantFetch(stale, true);
            }
            log.warn("[GeocodeSearch][Photon] 429 for '{}', no cached fallback", query);
            return new VariantFetch(List.of(), true);
        } catch (Exception e) {
            // Photon è un servizio terzo: in caso di errore non blocchiamo la pipeline,
            // proseguiamo con Nominatim e usiamo eventualmente la cache stale di Photon.
            log.warn("[GeocodeSearch][Photon] failed for '{}': {}", query, e.toString());
            List<Map<String, Object>> stale = getCached(upstreamCache, cacheKey, Long.MAX_VALUE);
            if (stale != null) {
                return new VariantFetch(stale, false);
            }
            return new VariantFetch(List.of(), false);
        }
    }

    /**
     * Trasforma la GeoJSON FeatureCollection di Photon in righe nello stesso formato
     * che il resto della pipeline (Nominatim-style) usa già.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> photonFeaturesToRows(Map<String, Object> photonResponse) {
        if (photonResponse == null) return List.of();
        Object featuresObj = photonResponse.get("features");
        if (!(featuresObj instanceof List<?> rawFeatures)) return List.of();
        List<Map<String, Object>> rows = new ArrayList<>(rawFeatures.size());
        for (Object feat : rawFeatures) {
            if (!(feat instanceof Map<?, ?> rawFeat)) continue;
            Map<String, Object> row = photonFeatureToRow((Map<String, Object>) rawFeat);
            if (row != null) rows.add(row);
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> photonFeatureToRow(Map<String, Object> feature) {
        Object geomObj = feature.get("geometry");
        Object propsObj = feature.get("properties");
        if (!(geomObj instanceof Map<?, ?> rawGeom)) return null;
        if (!(propsObj instanceof Map<?, ?> rawProps)) return null;
        Map<String, Object> geom = (Map<String, Object>) rawGeom;
        Map<String, Object> props = (Map<String, Object>) rawProps;

        Object coordsObj = geom.get("coordinates");
        if (!(coordsObj instanceof List<?> coords) || coords.size() < 2) return null;
        Double lon = parseDouble(coords.get(0));
        Double lat = parseDouble(coords.get(1));
        if (lat == null || lon == null) return null;

        String type = toStringOrNull(props.get("type"));   // house/street/locality/district/city/...
        String name = toStringOrNull(props.get("name"));
        String street = toStringOrNull(props.get("street"));
        String housenumber = toStringOrNull(props.get("housenumber"));
        String city = firstNonBlank(
                toStringOrNull(props.get("city")),
                toStringOrNull(props.get("town")),
                toStringOrNull(props.get("village")),
                toStringOrNull(props.get("locality"))
        );
        String district = firstNonBlank(
                toStringOrNull(props.get("district")),
                toStringOrNull(props.get("suburb")),
                toStringOrNull(props.get("neighbourhood"))
        );
        String state = toStringOrNull(props.get("state"));
        String country = toStringOrNull(props.get("country"));
        String postcode = toStringOrNull(props.get("postcode"));
        String osmKey = toStringOrNull(props.get("osm_key"));
        String osmValue = toStringOrNull(props.get("osm_value"));

        // Per "house" la strada sta in "street"; per "street" sta in "name". Per POI
        // di solito non c'è strada associata (solo nome).
        String road;
        if (street != null && !street.isBlank()) {
            road = street;
        } else if ("street".equalsIgnoreCase(type)) {
            road = name;
        } else {
            road = null;
        }

        Map<String, Object> address = new LinkedHashMap<>();
        if (road != null) address.put("road", road);
        if (housenumber != null) address.put("house_number", housenumber);
        if (district != null) address.put("suburb", district);
        if (city != null) address.put("city", city);
        if (postcode != null) address.put("postcode", postcode);
        if (state != null) address.put("state", state);
        if (country != null) address.put("country", country);

        // display_name: ricostruito leggibile, in stile italiano.
        List<String> parts = new ArrayList<>(5);
        if ("house".equalsIgnoreCase(type)) {
            if (road != null) parts.add(housenumber != null ? road + " " + housenumber : road);
        } else if ("street".equalsIgnoreCase(type)) {
            if (name != null) parts.add(name);
        } else {
            // POI/locality/city: il nome viene per primo, eventuale strada come dettaglio.
            if (name != null) parts.add(name);
            if (road != null && !equalsIgnoreCase(road, name)) {
                parts.add(housenumber != null ? road + " " + housenumber : road);
            }
        }
        if (district != null && !equalsIgnoreCase(district, city) && !equalsIgnoreCase(district, name)) {
            parts.add(district);
        }
        if (city != null && !equalsIgnoreCase(city, name)) parts.add(city);
        if (state != null && !equalsIgnoreCase(state, city)) parts.add(state);
        if (country != null) parts.add(country);
        String displayName = parts.isEmpty() ? (name != null ? name : "") : String.join(", ", parts);
        if (displayName.isBlank()) return null;

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("lat", lat);
        row.put("lon", lon);
        row.put("display_name", displayName);
        row.put("address", address);
        if (name != null) {
            row.put("name", name);
            Map<String, Object> namedetails = new LinkedHashMap<>();
            namedetails.put("name", name);
            row.put("namedetails", namedetails);
        }
        if (osmKey != null) row.put("class", osmKey);
        if (osmValue != null) row.put("photon_value", osmValue);
        return row;
    }

    @SuppressWarnings("unchecked")
    private VariantFetch doFetch(String cacheKey, Function<UriBuilder, java.net.URI> uri, String description) {
        try {
            List<Map<String, Object>> payload = webClient.get()
                    .uri(uri::apply)
                    .header("User-Agent", "gtfs-monitor/1.0 (geocode-search)")
                    .retrieve()
                    .bodyToMono(List.class)
                    .block(Duration.ofSeconds(8));
            List<Map<String, Object>> safePayload = payload == null ? List.of() : List.copyOf(payload);
            upstreamCache.put(cacheKey, new TimedValue<>(safePayload, System.currentTimeMillis()));
            return new VariantFetch(safePayload, false);
        } catch (WebClientResponseException.TooManyRequests e) {
            List<Map<String, Object>> stalePayload = getCached(upstreamCache, cacheKey, Long.MAX_VALUE);
            if (stalePayload != null) {
                log.warn("[GeocodeSearch] 429 from Nominatim for '{}', using stale upstream cache with {} result(s)",
                        description,
                        stalePayload.size());
                return new VariantFetch(stalePayload, true);
            }
            log.warn("[GeocodeSearch] 429 from Nominatim for '{}', no cached fallback available", description);
            return new VariantFetch(List.of(), true);
        }
    }

    private static String buildStreetParam(ParsedAddress address) {
        String street = address.street();
        String house = address.houseNumber();
        if (house == null || house.isBlank()) return street;
        return street + " " + house;
    }

    /**
     * Espande la coppia street+house in piccole varianti per coprire le scritture
     * più comuni dei civici italiani in OSM ("3n", "3/N", "3 N", "3").
     * La street-only è sempre l'ultima fallback.
     */
    private static List<ParsedAddress> expandStructuredVariants(ParsedAddress base) {
        if (base == null || base.street() == null) return List.of();
        LinkedHashSet<ParsedAddress> out = new LinkedHashSet<>();
        out.add(base);
        String house = base.houseNumber();
        if (house != null && !house.isBlank()) {
            for (String hv : houseNumberVariants(house)) {
                out.add(new ParsedAddress(base.street(), hv, base.city()));
            }
            // Fallback street-only: penalizzato in scoring ma utile se OSM non ha quel civico.
            out.add(new ParsedAddress(base.street(), null, base.city()));
        }
        return List.copyOf(out);
    }

    /**
     * Genera varianti di un civico italiano:
     *  - "3n"   → "3n", "3/n", "3 n", "3"
     *  - "3/N"  → "3/n", "3n", "3 n", "3"
     *  - "12"   → "12"
     *  - "12bis" → "12bis", "12 bis", "12"
     */
    static List<String> houseNumberVariants(String house) {
        if (house == null) return List.of();
        String trimmed = house.trim().toLowerCase(Locale.ROOT);
        if (trimmed.isBlank()) return List.of();
        LinkedHashSet<String> out = new LinkedHashSet<>();
        out.add(trimmed);

        // Estrai parte numerica + suffisso (lettere o "bis"/"ter"/...)
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("^\\s*(\\d+)\\s*[\\-/ ]?\\s*([a-z]{1,4})?\\s*$")
                .matcher(trimmed.replace("/", " "));
        if (m.matches()) {
            String num = m.group(1);
            String suffix = m.group(2);
            if (suffix != null && !suffix.isBlank()) {
                out.add(num + suffix);
                out.add(num + "/" + suffix);
                out.add(num + " " + suffix);
            }
            out.add(num);
        }
        return List.copyOf(out);
    }

    /**
     * Prova a parsare la query come indirizzo strutturato.
     * Riconosce prefissi via/viale/piazza/largo/corso/vicolo/lungotevere/circonvallazione e civico finale.
     */
    static ParsedAddress parseAddress(String query) {
        if (query == null) return null;
        String q = query.trim().replaceAll("[,;]+", " ").replaceAll("\\s+", " ");
        if (q.isBlank()) return null;

        String expanded = q
                .replace('’', '\'')
                .replaceAll("(?i)\\bp\\.zza\\b", "piazza")
                .replaceAll("(?i)\\bp\\.za\\b", "piazza")
                .replaceAll("(?i)\\bv\\.le\\b", "viale")
                .replaceAll("(?i)\\bl\\.go\\b", "largo")
                .replaceAll("(?i)\\bc\\.so\\b", "corso")
                .replaceAll("(?i)\\bv\\.\\b", "via");

        // Civico: numero opzionalmente seguito da /lettera, lettera, "bis"/"ter".
        java.util.regex.Pattern housePattern = java.util.regex.Pattern.compile(
                "(?i)\\s+(\\d+\\s*(?:[\\-/]\\s*[a-z]{1,4}|\\s?[a-z]{1,4}|bis|ter|quater)?)\\s*$");
        java.util.regex.Matcher hm = housePattern.matcher(expanded);
        String house = null;
        String streetPart = expanded;
        if (hm.find()) {
            house = hm.group(1).replaceAll("\\s+", "").toLowerCase(Locale.ROOT);
            streetPart = expanded.substring(0, hm.start()).trim();
        }

        // Estrai eventuale "Roma" / nome comune in coda (dopo virgola era già rimosso).
        String city = null;
        for (String town : ROME_METRO_TOWNS) {
            String regex = "(?i)\\b" + java.util.regex.Pattern.quote(town) + "\\b\\s*$";
            if (streetPart.toLowerCase(Locale.ROOT).matches(".*" + regex)) {
                city = capitalize(town);
                streetPart = streetPart.replaceAll(regex, "").trim();
                break;
            }
        }
        if (city == null) {
            java.util.regex.Matcher rm = java.util.regex.Pattern.compile("(?i)\\bRoma\\b\\s*$").matcher(streetPart);
            if (rm.find()) {
                city = "Roma";
                streetPart = streetPart.substring(0, rm.start()).trim();
            }
        }

        if (streetPart.isBlank()) return null;

        // Considera "indirizzo" solo se inizia con un type-prefix riconosciuto OPPURE se è presente un civico.
        boolean hasTypePrefix = streetPart.toLowerCase(Locale.ROOT).matches(
                "^(via|viale|piazza|largo|corso|vicolo|lungotevere|circonvallazione|salita|discesa|borgo|strada)\\b.*");
        if (!hasTypePrefix && house == null) return null;

        return new ParsedAddress(streetPart, house, city);
    }

    private static String capitalize(String value) {
        if (value == null || value.isBlank()) return value;
        StringBuilder sb = new StringBuilder(value.length());
        boolean upper = true;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isWhitespace(c)) {
                upper = true;
                sb.append(c);
            } else if (upper) {
                sb.append(Character.toUpperCase(c));
                upper = false;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
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
                toStringOrNull(address.get("town")),
                toStringOrNull(address.get("village"))
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
                    toStringOrNull(address.get("village"))
            );
            if (city != null) {
                String c = city.toLowerCase(Locale.ROOT);
                if (c.contains("roma")) return true;
                if (ROME_METRO_TOWNS.contains(c)) return true;
            }
            String county = toStringOrNull(address.get("county"));
            if (county != null && county.toLowerCase(Locale.ROOT).contains("roma")) return true;
            String state = toStringOrNull(address.get("state"));
            // Ultimo paracadute: provincia/regione lazio + bounding box → consideralo valido.
            if (state != null && state.toLowerCase(Locale.ROOT).contains("lazio")) return true;
        }
        String l = label.toLowerCase(Locale.ROOT);
        if (l.contains("roma")) return true;
        for (String town : ROME_METRO_TOWNS) {
            if (l.contains(town)) return true;
        }
        return false;
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
        String qStreetCore = stripStreetTypePrefix(qStreet);
        String roadNorm = normalizeKey(road);
        String roadCore = stripStreetTypePrefix(roadNorm);
        String houseNorm = normalizeHouseNumber(houseNumber);
        String qHouseNorm = normalizeHouseNumber(qHouse);

        if (qStreet != null && !qStreet.isBlank()) {
            if (roadNorm.equals(qStreet)) score -= 350;
            else if (roadNorm.startsWith(qStreet)) score -= 240;
            else if (roadNorm.contains(qStreet)) score -= 120;
            else if (roadCore.equals(qStreetCore)) score -= 210;
            else if (!qStreetCore.isBlank() && roadCore.startsWith(qStreetCore)) score -= 140;
            else if (!qStreetCore.isBlank() && roadCore.contains(qStreetCore)) score -= 80;
        }

        if (qHouseNorm != null) {
            if (houseNorm != null && houseNorm.equals(qHouseNorm)) {
                score -= 500;
            } else if (houseNorm != null && houseNumberLooseMatch(qHouseNorm, houseNorm)) {
                // Match con varianti tipografiche ("3n" vs "3/n" vs "3 n").
                score -= 380;
            } else if (houseNorm != null && qHouseNorm.startsWith(houseNorm)) {
                score -= 120;
            } else {
                // Query contiene civico ma il risultato non lo ha: penalità contenuta
                // così la strada giusta resta visibile come fallback.
                score += 90;
            }
        }

        if (l.startsWith(q)) score -= 300;
        if (l.contains(q)) score -= 120;

        // Boost match on POI/commercial names (e.g. "Carrefour Market", "Conad City")
        // without changing the displayed address label.
        String poiName = extractPoiDisplayName(row, label);
        String poiNorm = normalizeKey(poiName);
        if (!q.isBlank() && poiNorm != null && !poiNorm.isBlank()) {
            if (poiNorm.equals(q)) {
                score -= 320;
            } else if (poiNorm.startsWith(q)) {
                score -= 240;
            } else if (poiNorm.contains(q)) {
                score -= 140;
            }
        }

        if (l.contains("roma")) score -= 40;
        if (l.contains("municipio")) score -= 10;
        score += Math.abs(l.length() - q.length()) / 4;

        if (biasLat != null && biasLon != null) {
            Double lat = parseDouble(row.get("lat"));
            Double lon = parseDouble(row.get("lon"));
            if (lat != null && lon != null) {
                int meters = haversineMeters(biasLat, biasLon, lat, lon);
                // Per POI/ricerche generiche (bar, supermercato, ecc.) la vicinanza deve pesare di più.
                // Per indirizzi manteniamo un bias morbido per non rompere il matching via/civico.
                score += looksLikeAddressQuery(query)
                        ? Math.min(220, meters / 60)
                        : Math.min(420, meters / 35);
            }
        }
        return score;
    }

    /**
     * True se i due civici normalizzati corrispondono a meno di separatori/spazi/slash.
     * Es. "3n" ~ "3/n" ~ "3 n".
     */
    private static boolean houseNumberLooseMatch(String a, String b) {
        if (a == null || b == null) return false;
        String aa = a.replaceAll("[\\s/\\-]", "");
        String bb = b.replaceAll("[\\s/\\-]", "");
        return !aa.isBlank() && aa.equalsIgnoreCase(bb);
    }

    private static List<String> queryVariants(String query) {
        LinkedHashSet<String> variants = new LinkedHashSet<>();
        String trimmed = query.trim();
        variants.add(trimmed);

        String normalizedSpaces = query.replaceAll("[,;]+", " ").replaceAll("\\s+", " ").trim();
        variants.add(normalizedSpaces);

        String expanded = normalizedSpaces
                .replace('’', '\'')
                .replaceAll("(?i)\\bp\\.zza\\b", "piazza")
                .replaceAll("(?i)\\bp\\.za\\b", "piazza")
                .replaceAll("(?i)\\bv\\.le\\b", "viale")
                .replaceAll("(?i)\\bviale\\b", "viale")
                .replaceAll("(?i)\\bv\\.?\\b", "via")
                .replaceAll("(?i)\\bl\\.go\\b", "largo")
                .replaceAll("(?i)\\bc\\.so\\b", "corso")
                .replaceAll("(?i)\\bstaz\\.ne\\b", "stazione");
        variants.add(expanded);

        // POI prefix stripping: "Stazione Lepanto" → tenta anche "Lepanto" e "Lepanto Roma".
        // In OSM la fermata è spesso taggata col solo nome ("Lepanto"), non col prefisso "Stazione".
        String expandedLower = expanded.toLowerCase(Locale.ROOT);
        String[] poiPrefixes = {"stazione ", "fermata ", "metro ", "scalo ", "capolinea "};
        for (String prefix : poiPrefixes) {
            if (expandedLower.startsWith(prefix)) {
                String bare = expanded.substring(prefix.length()).trim();
                if (!bare.isBlank()) {
                    variants.add(bare);
                    if (!bare.toLowerCase(Locale.ROOT).contains("roma")) {
                        variants.add(bare + " Roma");
                    }
                }
                break;
            }
        }

        String baseToken = normalizeKey(expanded);
        if (baseToken.equals("carrefour")) {
            variants.add("carrefour market");
            variants.add("carrefour express");
        } else if (baseToken.equals("conad")) {
            variants.add("conad city");
            variants.add("conad superstore");
        }

        String withRoma = expanded.toLowerCase().contains("roma") ? expanded : (expanded + " Roma");
        variants.add(withRoma);

        String withoutHouse = normalizedStreetQuery(query);
        if (withoutHouse != null && !withoutHouse.isBlank()) {
            variants.add(withoutHouse);
            if (!withoutHouse.toLowerCase().contains("roma")) variants.add(withoutHouse + " Roma");
        }

        String streetCore = stripStreetTypePrefix(normalizeKey(expanded));
        String streetCoreQuery = denormalizeForQuery(streetCore);
        if (streetCoreQuery != null && !streetCoreQuery.isBlank() && !streetCoreQuery.equalsIgnoreCase(expanded)) {
            variants.add(streetCoreQuery);
            variants.add("via " + streetCoreQuery);
            variants.add("viale " + streetCoreQuery);
            variants.add("piazza " + streetCoreQuery);
            if (!streetCoreQuery.toLowerCase(Locale.ROOT).contains("roma")) {
                variants.add(streetCoreQuery + " Roma");
                variants.add("via " + streetCoreQuery + " Roma");
            }
        }

        return variants.stream().filter(v -> v != null && !v.isBlank()).limit(8).toList();
    }

    private static String extractHouseNumberToken(String query) {
        if (query == null) return null;
        String[] parts = query.trim().split("\\s+");
        for (int i = parts.length - 1; i >= 0; i--) {
            // Tollerante a separatori "/" e "-": "3/n", "3-n", "3n", "12bis".
            String p = parts[i].replaceAll("[,.;]", "");
            if (p.matches("(?i)\\d+([\\-/]?[a-z]{1,4}|bis|ter|quater)?")) {
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

    @SuppressWarnings("unchecked")
    private static String extractPoiDisplayName(Map<String, Object> row, String normalizedLabel) {
        String direct = toStringOrNull(row.get("name"));
        Object namedetailsObj = row.get("namedetails");
        String named = null;
        if (namedetailsObj instanceof Map<?, ?> raw) {
            Map<String, Object> namedetails = (Map<String, Object>) raw;
            named = firstNonBlank(
                    toStringOrNull(namedetails.get("name:it")),
                    toStringOrNull(namedetails.get("official_name")),
                    toStringOrNull(namedetails.get("name"))
            );
        }
        String candidate = firstNonBlank(named, direct);
        if (candidate == null) return null;
        String c = candidate.trim();
        if (c.isBlank()) return null;
        // Avoid redundant titles when the "name" is basically the same as the compact address label.
        String cNorm = normalizeKey(c);
        String lNorm = normalizeKey(normalizedLabel);
        if (cNorm.isBlank() || cNorm.equals(lNorm) || lNorm.startsWith(cNorm + " ")) {
            return null;
        }
        return c;
    }

    private static boolean looksLikeAddressQuery(String query) {
        if (query == null) return false;
        String q = normalizeKey(query);
        if (q.isBlank()) return false;
        if (q.matches(".*\\d+[a-z]{0,3}.*")) return true;
        return q.contains("via ")
                || q.contains("viale ")
                || q.contains("piazza ")
                || q.contains("largo ")
                || q.contains("corso ")
                || q.contains("vicolo ")
                || q.contains("lungotevere ")
                || q.contains("circonvallazione ");
    }

    private static String normalizeKey(String value) {
        String normalized = Normalizer.normalize(Objects.toString(value, ""), Normalizer.Form.NFD)
                .replaceAll("\\p{M}+", "");
        return normalized
                .toLowerCase()
                .replaceAll("[^\\p{L}\\p{N}\\s]", " ")
                .replaceAll("\\s+", " ")
                .trim();
    }

    private static String stripStreetTypePrefix(String value) {
        String normalized = normalizeKey(value);
        return normalized.replaceFirst("^(via|viale|piazza|largo|corso|vicolo|lungotevere|circonvallazione|salita|discesa|borgo|strada)\\s+", "").trim();
    }

    private static String denormalizeForQuery(String value) {
        if (value == null) return null;
        String out = value.replaceAll("\\s+", " ").trim();
        return out.isBlank() ? null : out;
    }

    private static String buildSearchCacheKey(String query, int limit, Double biasLat, Double biasLon) {
        return normalizeKey(query)
                + "|" + limit
                + "|" + bucketBias(biasLat)
                + "|" + bucketBias(biasLon);
    }

    private static String bucketBias(Double value) {
        if (value == null) return "-";
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static <T> T getCached(Map<String, TimedValue<T>> cache, String key, long ttlMillis) {
        TimedValue<T> timed = cache.get(key);
        if (timed == null || timed.value() == null) return null;
        long age = System.currentTimeMillis() - timed.loadedAtMillis();
        if (age > ttlMillis) return null;
        return timed.value();
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
    private record TimedValue<T>(T value, long loadedAtMillis) {}
    private record VariantFetch(List<Map<String, Object>> rows, boolean rateLimited) {}
    record ParsedAddress(String street, String houseNumber, String city) {}
}
