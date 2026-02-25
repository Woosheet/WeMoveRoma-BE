// src/main/java/it/roma/gtfs/gtfs_monitor/service/GtfsIndexService.java
package it.roma.gtfs.gtfs_monitor.service;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@RequiredArgsConstructor
public class GtfsIndexService {

    private final GtfsProperties props;
    private Path dataDir;

    private final AtomicReference<Indexes> ref = new AtomicReference<>(Indexes.EMPTY);

    @PostConstruct
    void init() {
        this.dataDir = Path.of(props.staticProps().dataDir());
    }

    public void rebuildIndexes() throws IOException {

        long t0 = System.nanoTime();

        Path stopsPath  = dataDir.resolve("stops.txt");
        Path tripsPath  = dataDir.resolve("trips.txt");
        Path routesPath = dataDir.resolve("routes.txt");
        Path shapesPath = dataDir.resolve("shapes.txt");


        Map<String, Stop> stops   = loadStops(stopsPath);
        Map<String, String> routeShortNames = loadRouteShortNames(routesPath);
        Map<String, Trip> trips   = loadTrips(tripsPath);
        Map<String, List<ShapePoint>> shapesById = loadShapes(shapesPath);
        Map<String, Set<String>> byRoute = buildRouteIndex(trips);
        Map<String, Set<String>> destinationsByRoute = buildDestinationsIndex(trips);
        Map<String, Set<String>> routeIdsByLine = buildRouteIdsByLineIndex(routeShortNames, byRoute.keySet());

        ref.set(new Indexes(stops, trips, byRoute, destinationsByRoute, routeShortNames, routeIdsByLine, shapesById));
        long ms = (System.nanoTime() - t0) / 1_000_000;
        log.info("[GTFS-Index] Indici ricostruiti: stops={} trips={} routes={} in {} ms",
                stops.size(), trips.size(), byRoute.size(), ms);


        if (trips.isEmpty()) {
            log.warn("[GTFS-Index] ⚠ ATTENZIONE: trips è VUOTO. Trip headsign e shape non funzioneranno.");
        }
    }

    // ============================================================
    //                      QUERY API
    // ============================================================

    public Optional<Stop> stopById(String id) {
        return Optional.ofNullable(ref.get().stops().get(id));
    }

    public Stop stopByIdOrNull(String id) {
        if (id == null) return null;
        return ref.get().stops().get(id);
    }

    public Optional<Trip> tripById(String id) {
        return Optional.ofNullable(ref.get().trips().get(id));
    }

    public Trip tripByIdOrNull(String id) {
        if (id == null) return null;
        return ref.get().trips().get(id);
    }

    public Set<String> tripsByRoute(String routeId) {
        return ref.get().tripsByRoute().getOrDefault(routeId, Set.of());
    }

    public Set<String> routeIds() {
        return ref.get().tripsByRoute().keySet();
    }

    public Set<String> destinationsByRoute(String routeId) {
        if (routeId == null) return Set.of();
        return ref.get().destinationsByRoute().getOrDefault(routeId, Set.of());
    }

    public String publicLineByRouteId(String routeId) {
        if (routeId == null) return null;
        return ref.get().routeShortNameByRouteId().getOrDefault(routeId, routeId);
    }

    public Set<String> routeIdsByPublicLine(String line) {
        if (line == null) return Set.of();
        return ref.get().routeIdsByPublicLine().getOrDefault(line, Set.of());
    }

    public Set<String> publicLines() {
        return ref.get().routeIdsByPublicLine().keySet();
    }

    public Collection<Stop> allStops() {
        return ref.get().stops().values();
    }

    public Set<String> destinationsByPublicLine(String line) {
        if (line == null || line.isBlank()) return Set.of();
        Set<String> routeIds = routeIdsByPublicLine(line);
        if (routeIds.isEmpty()) return Set.of();

        Set<String> out = new LinkedHashSet<>();
        for (String routeId : routeIds) {
            out.addAll(destinationsByRoute(routeId));
        }
        return out;
    }

    public boolean matchesLine(String filter, String routeId) {
        if (filter == null || filter.isBlank()) return true;
        if (routeId == null) return false;
        if (filter.equals(routeId)) return true;
        String publicLine = publicLineByRouteId(routeId);
        return filter.equalsIgnoreCase(publicLine);
    }

    public List<ShapePoint> shapeByTripId(String tripId) {
        Trip trip = tripByIdOrNull(tripId);
        if (trip == null || trip.shapeId() == null || trip.shapeId().isBlank()) return List.of();
        return ref.get().shapesById().getOrDefault(trip.shapeId(), List.of());
    }


    // ============================================================
    //                      CONFIG PARSER CSV
    // ============================================================

    private static CsvParser newParser() {
        CsvParserSettings s = new CsvParserSettings();
        s.setHeaderExtractionEnabled(true);
        s.setLineSeparatorDetectionEnabled(true);
        s.setNullValue("");
        s.setEmptyValue("");
        s.getFormat().setDelimiter(',');
        s.setMaxCharsPerColumn(1 << 20);
        return new CsvParser(s);
    }


    // ============================================================
    //                      CARICAMENTO INDICI
    // ============================================================

    private Map<String, Stop> loadStops(Path p) throws IOException {
        Map<String, Stop> out = new HashMap<>(64_000);
        if (!Files.exists(p)) {
            log.warn("[GTFS-Index] stops.txt NON trovato ({})", p);
            return out;
        }

        long t0 = System.nanoTime();
        try (InputStream is = new BufferedInputStream(Files.newInputStream(p))) {
            CsvParser parser = newParser();
            parser.beginParsing(is, StandardCharsets.UTF_8);
            Record r;
            while ((r = parser.parseNextRecord()) != null) {
                String id = r.getString("stop_id");
                if (id == null || id.isEmpty()) continue;

                String code      = r.getString("stop_code");
                String name      = nz(r.getString("stop_name"));
                String desc      = r.getString("stop_desc");
                Float  lat       = parseFloatOrNull(r.getString("stop_lat"));
                Float  lon       = parseFloatOrNull(r.getString("stop_lon"));
                String url       = r.getString("stop_url");
                Integer wheel    = parseIntOrNull(r.getString("wheelchair_boarding"));
                String timezone  = r.getString("stop_timezone");
                Integer locType  = parseIntOrNull(r.getString("location_type"));
                String parent    = r.getString("parent_station");

                out.put(id, new Stop(id, code, name, desc, lat, lon, url, wheel, timezone, locType, parent));
            }
            parser.stopParsing();
            long ms = (System.nanoTime() - t0) / 1_000_000;
            log.debug("[GTFS-Index] Caricate {} stops in {} ms", out.size(), ms);
        }
        return out;
    }

    private Map<String, Trip> loadTrips(Path p) throws IOException {
        Map<String, Trip> out = new HashMap<>(128_000);

        if (!Files.exists(p)) {
            log.warn("[GTFS-Index] trips.txt non trovato in {}", p);
            return out;
        }

        try (InputStream is = new BufferedInputStream(Files.newInputStream(p))) {
            CsvParser parser = newParser();
            parser.beginParsing(is, StandardCharsets.UTF_8);

            Record r;
            while ((r = parser.parseNextRecord()) != null) {

                String tripId = r.getString("trip_id");
                if (tripId == null || tripId.isEmpty()) continue;

                String routeId    = nz(r.getString("route_id"));
                String serviceId  = nz(r.getString("service_id"));
                String headsign   = nz(r.getString("trip_headsign"));
                String shortName  = nz(r.getString("trip_short_name"));
                Integer dirId     = parseIntOrNull(r.getString("direction_id"));
                String blockId    = nz(r.getString("block_id"));
                String shapeId    = nz(r.getString("shape_id"));
                Integer wheelchair  = parseIntOrNull(r.getString("wheelchair_accessible"));
                Integer exceptional = parseIntOrNull(r.getString("exceptional"));

                out.put(tripId, new Trip(
                        tripId,
                        routeId,
                        serviceId,
                        headsign,
                        shortName,
                        dirId,
                        blockId,
                        shapeId,
                        wheelchair,
                        exceptional
                ));
            }
            parser.stopParsing();
        }
        return out;
    }

    private Map<String, String> loadRouteShortNames(Path p) throws IOException {
        Map<String, String> out = new HashMap<>(8_000);
        if (!Files.exists(p)) {
            log.warn("[GTFS-Index] routes.txt non trovato in {}", p);
            return out;
        }

        try (InputStream is = new BufferedInputStream(Files.newInputStream(p))) {
            CsvParser parser = newParser();
            parser.beginParsing(is, StandardCharsets.UTF_8);
            Record r;
            while ((r = parser.parseNextRecord()) != null) {
                String routeId = r.getString("route_id");
                if (routeId == null || routeId.isBlank()) continue;
                String shortName = r.getString("route_short_name");
                out.put(routeId, (shortName == null || shortName.isBlank()) ? routeId : shortName.trim());
            }
            parser.stopParsing();
        }
        return out;
    }

    private Map<String, List<ShapePoint>> loadShapes(Path p) throws IOException {
        Map<String, List<ShapePoint>> out = new HashMap<>(32_000);
        if (!Files.exists(p)) {
            log.warn("[GTFS-Index] shapes.txt non trovato in {}", p);
            return out;
        }
        try (InputStream is = new BufferedInputStream(Files.newInputStream(p))) {
            CsvParser parser = newParser();
            parser.beginParsing(is, StandardCharsets.UTF_8);
            Record r;
            while ((r = parser.parseNextRecord()) != null) {
                String shapeId = r.getString("shape_id");
                if (shapeId == null || shapeId.isBlank()) continue;
                Float lat = parseFloatOrNull(r.getString("shape_pt_lat"));
                Float lon = parseFloatOrNull(r.getString("shape_pt_lon"));
                Integer seq = parseIntOrNull(r.getString("shape_pt_sequence"));
                if (lat == null || lon == null || seq == null) continue;
                out.computeIfAbsent(shapeId, k -> new ArrayList<>(64)).add(new ShapePoint(lat, lon, seq));
            }
            parser.stopParsing();
        }
        for (List<ShapePoint> points : out.values()) {
            points.sort(Comparator.comparingInt(ShapePoint::sequence));
        }
        return out;
    }

    private Map<String, Set<String>> buildRouteIndex(Map<String, Trip> trips) {
        Map<String, Set<String>> byRoute = new HashMap<>(Math.max(16, trips.size() / 8));

        for (Trip t : trips.values()) {
            if (t.routeId == null || t.routeId.isEmpty()) continue;
            byRoute.computeIfAbsent(t.routeId(), k -> new HashSet<>(8)).add(t.tripId());
        }

        return byRoute;
    }

    private Map<String, Set<String>> buildDestinationsIndex(Map<String, Trip> trips) {
        Map<String, Set<String>> out = new HashMap<>(Math.max(16, trips.size() / 8));
        for (Trip t : trips.values()) {
            if (t.routeId == null || t.routeId.isEmpty()) continue;
            if (t.headsign == null || t.headsign.isBlank()) continue;
            out.computeIfAbsent(t.routeId(), k -> new LinkedHashSet<>()).add(t.headsign().trim());
        }
        return out;
    }

    private Map<String, Set<String>> buildRouteIdsByLineIndex(Map<String, String> routeShortNames, Set<String> routeIds) {
        Map<String, Set<String>> out = new HashMap<>(Math.max(16, routeIds.size() / 4));
        for (String routeId : routeIds) {
            String line = routeShortNames.getOrDefault(routeId, routeId);
            out.computeIfAbsent(line, k -> new LinkedHashSet<>()).add(routeId);
        }
        return out;
    }


    // ============================================================
    //                      UTILITY
    // ============================================================

    private static String nz(String s) {
        return (s == null) ? "" : s;
    }

    private static Integer parseIntOrNull(String s) {
        if (s == null || s.isEmpty()) return null;
        try { return Integer.parseInt(s); } catch (NumberFormatException e) { return null; }
    }


    private static Float parseFloatOrNull(String s) {
        if (s == null || s.isEmpty()) return null;
        try { return Float.parseFloat(s); } catch (NumberFormatException e) { return null; }
    }

    // ============================================================
    //                      RECORD TYPES
    // ============================================================
    public record Indexes(
            Map<String, Stop> stops,
            Map<String, Trip> trips,
            Map<String, Set<String>> tripsByRoute,
            Map<String, Set<String>> destinationsByRoute,
            Map<String, String> routeShortNameByRouteId,
            Map<String, Set<String>> routeIdsByPublicLine,
            Map<String, List<ShapePoint>> shapesById
    ) {
        static final Indexes EMPTY = new Indexes(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    public record Stop(
            String id,                 // stop_id
            String code,               // stop_code
            String name,               // stop_name
            String desc,               // stop_desc
            Float  lat,                // stop_lat
            Float  lon,                // stop_lon
            String url,                // stop_url
            Integer wheelchairBoarding,// wheelchair_boarding (0/1/2)
            String timezone,           // stop_timezone
            Integer locationType,      // location_type (0/1/2/3/4)
            String parentStation       // parent_station
    ) {}

    public record Trip(
            String tripId,
            String routeId,
            String serviceId,
            String headsign,
            String shortName,
            Integer directionId,
            String blockId,
            String shapeId,
            Integer wheelchair,
            Integer exceptional
    ) {
    }

    public record ShapePoint(
            Float lat,
            Float lon,
            Integer sequence
    ) {}

}
