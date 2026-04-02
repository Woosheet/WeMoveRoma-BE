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
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
@RequiredArgsConstructor
public class GtfsIndexService {
    private static final ZoneId ROME_ZONE = ZoneId.of("Europe/Rome");

    private final GtfsProperties props;
    private Path dataDir;

    private final AtomicReference<Indexes> ref = new AtomicReference<>(Indexes.EMPTY);
    private final AtomicReference<Map<LocalDate, ScheduledStopIndex>> scheduledByDateRef = new AtomicReference<>(Map.of());
    private final AtomicReference<Map<LocalDate, ActiveTripIndex>> activeTripsByDateRef = new AtomicReference<>(Map.of());

    @PostConstruct
    void init() {
        this.dataDir = Path.of(props.staticProps().dataDir());
    }

    public void rebuildIndexes() throws IOException {
        long t0 = System.nanoTime();

        Path stopsPath = dataDir.resolve("stops.txt");
        Path tripsPath = dataDir.resolve("trips.txt");
        Path routesPath = dataDir.resolve("routes.txt");
        Path shapesPath = dataDir.resolve("shapes.txt");
        Path calendarDatesPath = dataDir.resolve("calendar_dates.txt");

        Map<String, Stop> stops = loadStops(stopsPath);
        Map<String, String> routeShortNames = loadRouteShortNames(routesPath);
        Map<String, Trip> trips = loadTrips(tripsPath);
        Map<String, List<ShapePoint>> shapesById = loadShapes(shapesPath, referencedShapeIds(trips));
        CalendarDatesIndex calendarDates = loadCalendarDates(calendarDatesPath);
        Map<String, Set<String>> byRoute = buildRouteIndex(trips);
        Map<String, Set<String>> destinationsByRoute = buildDestinationsIndex(trips);
        Map<String, Set<String>> routeIdsByLine = buildRouteIdsByLineIndex(routeShortNames, byRoute.keySet());

        ref.set(new Indexes(
                stops,
                trips,
                byRoute,
                destinationsByRoute,
                routeShortNames,
                routeIdsByLine,
                shapesById,
                calendarDates.addedByDate(),
                calendarDates.removedByDate()
        ));
        scheduledByDateRef.set(Map.of());
        activeTripsByDateRef.set(Map.of());

        long ms = (System.nanoTime() - t0) / 1_000_000;
        log.info("[GTFS-Index] Indici ricostruiti: stops={} trips={} routes={} in {} ms",
                stops.size(), trips.size(), byRoute.size(), ms);

        if (trips.isEmpty()) {
            log.warn("[GTFS-Index] ATTENZIONE: trips e' vuoto. Trip headsign e shape non funzioneranno.");
        }
    }

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
        Map<String, Trip> trips = ref.get().trips();
        Trip direct = trips.get(id);
        if (direct != null) {
            return direct;
        }
        String normalized = stripFeedPrefix(id);
        if (normalized == null || normalized.equals(id)) {
            return null;
        }
        return trips.get(normalized);
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

    public List<ScheduledArrival> scheduledArrivalsForStop(String stopId, Instant now, int horizonMinutes, int limit) {
        if (stopId == null || stopId.isBlank() || horizonMinutes <= 0 || limit <= 0) {
            return List.of();
        }

        ZonedDateTime nowRome = ZonedDateTime.ofInstant(now, ROME_ZONE);
        long nowEpochSeconds = now.getEpochSecond();
        long horizonEndEpochSeconds = nowEpochSeconds + (long) horizonMinutes * 60L;

        List<ScheduledArrival> out = new ArrayList<>(limit);
        collectScheduledArrivals(out, stopId, nowRome.toLocalDate(), nowEpochSeconds, horizonEndEpochSeconds, limit);

        LocalDate nextServiceDate = nowRome.toLocalDate().plusDays(1);
        long nextServiceStartEpochSeconds = nextServiceDate.atStartOfDay(ROME_ZONE).toEpochSecond();
        if (out.size() < limit && horizonEndEpochSeconds >= nextServiceStartEpochSeconds) {
            collectScheduledArrivals(out, stopId, nextServiceDate, nowEpochSeconds, horizonEndEpochSeconds, limit);
        }

        out.sort(Comparator.comparingInt(ScheduledArrival::etaMinutes));
        if (out.size() > limit) {
            return List.copyOf(out.subList(0, limit));
        }
        return List.copyOf(out);
    }

    public List<SimulatedTrip> simulatedTrips(String line, String destination, Instant now, int limit) {
        String normalizedLine = normalizeText(line);
        if (normalizedLine == null || limit <= 0) {
            return List.of();
        }

        String normalizedDestination = normalizeText(destination);
        ZonedDateTime nowRome = ZonedDateTime.ofInstant(now, ROME_ZONE);
        List<SimulatedTrip> out = new ArrayList<>(limit);
        collectSimulatedTrips(out, nowRome.toLocalDate(), normalizedLine, normalizedDestination, now.getEpochSecond());
        if (out.size() < limit) {
            collectSimulatedTrips(out, nowRome.toLocalDate().plusDays(1), normalizedLine, normalizedDestination, now.getEpochSecond());
        }
        out.sort(Comparator
                .comparingInt((SimulatedTrip trip) -> trip.activeAt(now.getEpochSecond()) ? 0 : 1)
                .thenComparingLong(trip -> Math.abs(trip.startEpochSeconds() - now.getEpochSecond())));
        if (out.size() > limit) {
            return List.copyOf(out.subList(0, limit));
        }
        return List.copyOf(out);
    }

    public Optional<SimulatedTrip> simulatedTripById(String tripId, Instant now) {
        if (tripId == null || tripId.isBlank()) {
            return Optional.empty();
        }
        String resolvedTripId = resolveKnownTripId(tripId);
        if (resolvedTripId == null) {
            return Optional.empty();
        }
        ZonedDateTime nowRome = ZonedDateTime.ofInstant(now, ROME_ZONE);
        long nowEpochSeconds = now.getEpochSecond();

        Optional<SimulatedTrip> today = simulatedTripByIdOnDate(resolvedTripId, nowRome.toLocalDate(), nowEpochSeconds);
        if (today.isPresent()) {
            return today;
        }

        Optional<SimulatedTrip> yesterday = simulatedTripByIdOnDate(resolvedTripId, nowRome.toLocalDate().minusDays(1), nowEpochSeconds);
        if (yesterday.isPresent()) {
            return yesterday;
        }

        return simulatedTripByIdOnDate(resolvedTripId, nowRome.toLocalDate().plusDays(1), nowEpochSeconds);
    }

    public List<ScheduledTripStop> scheduledNextStopsForTrip(String tripId, Instant now, int limit) {
        if (tripId == null || tripId.isBlank() || limit <= 0) {
            return List.of();
        }
        Trip trip = tripByIdOrNull(tripId);
        if (trip == null || trip.serviceId() == null || trip.serviceId().isBlank()) {
            return List.of();
        }

        ZonedDateTime nowRome = ZonedDateTime.ofInstant(now, ROME_ZONE);
        List<ScheduledTripStop> today = scheduledNextStopsForTripOnDate(trip, nowRome.toLocalDate(), now.getEpochSecond(), limit);
        if (!today.isEmpty()) {
          return today;
        }
        return scheduledNextStopsForTripOnDate(trip, nowRome.toLocalDate().plusDays(1), now.getEpochSecond(), limit);
    }

    public List<ScheduledTripStop> scheduledStopsForTrip(String tripId, Instant when) {
        if (tripId == null || tripId.isBlank()) {
            return List.of();
        }
        Trip trip = tripByIdOrNull(tripId);
        if (trip == null || trip.serviceId() == null || trip.serviceId().isBlank()) {
            return List.of();
        }

        Instant reference = when != null ? when : Instant.now();
        ZonedDateTime whenRome = ZonedDateTime.ofInstant(reference, ROME_ZONE);
        List<ScheduledTripStop> currentDate = scheduledStopsForTripOnDate(trip, whenRome.toLocalDate());
        if (!currentDate.isEmpty()) {
            return currentDate;
        }
        return scheduledStopsForTripOnDate(trip, whenRome.toLocalDate().plusDays(1));
    }

    private String resolveKnownTripId(String tripId) {
        if (tripId == null || tripId.isBlank()) {
            return null;
        }
        if (ref.get().trips().containsKey(tripId)) {
            return tripId;
        }
        String normalized = stripFeedPrefix(tripId);
        if (normalized != null && ref.get().trips().containsKey(normalized)) {
            return normalized;
        }
        return null;
    }

    private static String stripFeedPrefix(String value) {
        if (value == null) {
            return null;
        }
        int separator = value.indexOf(':');
        if (separator <= 0 || separator >= value.length() - 1) {
            return value;
        }
        return value.substring(separator + 1);
    }

    private static CsvParser newParser() {
        CsvParserSettings s = new CsvParserSettings();
        s.setHeaderExtractionEnabled(true);
        s.setLineSeparatorDetectionEnabled(true);
        s.setNullValue("");
        s.setEmptyValue("");
        s.getFormat().setDelimiter(',');
        // GTFS fields should stay small; keep a sane ceiling so a malformed feed row
        // fails fast instead of exhausting heap while univocity builds giant strings.
        s.setMaxCharsPerColumn(1 << 14);
        s.setErrorContentLength(120);
        return new CsvParser(s);
    }

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

                String code = r.getString("stop_code");
                String name = nz(r.getString("stop_name"));
                String desc = r.getString("stop_desc");
                Float lat = parseFloatOrNull(r.getString("stop_lat"));
                Float lon = parseFloatOrNull(r.getString("stop_lon"));
                String url = r.getString("stop_url");
                Integer wheel = parseIntOrNull(r.getString("wheelchair_boarding"));
                String timezone = r.getString("stop_timezone");
                Integer locType = parseIntOrNull(r.getString("location_type"));
                String parent = r.getString("parent_station");

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

                out.put(tripId, new Trip(
                        tripId,
                        nz(r.getString("route_id")),
                        nz(r.getString("service_id")),
                        nz(r.getString("trip_headsign")),
                        nz(r.getString("trip_short_name")),
                        parseIntOrNull(r.getString("direction_id")),
                        nz(r.getString("block_id")),
                        nz(r.getString("shape_id")),
                        parseIntOrNull(r.getString("wheelchair_accessible")),
                        parseIntOrNull(r.getString("exceptional"))
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

    private Map<String, List<ShapePoint>> loadShapes(Path p, Set<String> referencedShapeIds) throws IOException {
        Map<String, List<ShapePoint>> out = new HashMap<>(Math.max(16, referencedShapeIds.size()));
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
                if (!referencedShapeIds.isEmpty() && !referencedShapeIds.contains(shapeId)) continue;
                Float lat = parseFloatOrNull(r.getString("shape_pt_lat"));
                Float lon = parseFloatOrNull(r.getString("shape_pt_lon"));
                Integer seq = parseIntOrNull(r.getString("shape_pt_sequence"));
                if (lat == null || lon == null || seq == null) continue;
                out.computeIfAbsent(shapeId, ignored -> new ArrayList<>(64)).add(new ShapePoint(lat, lon, seq));
            }
            parser.stopParsing();
        }
        for (List<ShapePoint> points : out.values()) {
            points.sort(Comparator.comparingInt(ShapePoint::sequence));
        }
        return out;
    }

    private CalendarDatesIndex loadCalendarDates(Path p) throws IOException {
        Map<LocalDate, Set<String>> addedByDate = new HashMap<>();
        Map<LocalDate, Set<String>> removedByDate = new HashMap<>();
        if (!Files.exists(p)) {
            log.warn("[GTFS-Index] calendar_dates.txt non trovato in {}", p);
            return new CalendarDatesIndex(Map.of(), Map.of());
        }

        try (InputStream is = new BufferedInputStream(Files.newInputStream(p))) {
            CsvParser parser = newParser();
            parser.beginParsing(is, StandardCharsets.UTF_8);
            Record r;
            while ((r = parser.parseNextRecord()) != null) {
                String serviceId = r.getString("service_id");
                LocalDate date = parseLocalDateOrNull(r.getString("date"));
                Integer exceptionType = parseIntOrNull(r.getString("exception_type"));
                if (serviceId == null || serviceId.isBlank() || date == null || exceptionType == null) continue;

                if (exceptionType == 1) {
                    addedByDate.computeIfAbsent(date, ignored -> new HashSet<>()).add(serviceId);
                } else if (exceptionType == 2) {
                    removedByDate.computeIfAbsent(date, ignored -> new HashSet<>()).add(serviceId);
                }
            }
            parser.stopParsing();
        }

        return new CalendarDatesIndex(freezeSetMap(addedByDate), freezeSetMap(removedByDate));
    }

    private Map<String, Set<String>> buildRouteIndex(Map<String, Trip> trips) {
        Map<String, Set<String>> byRoute = new HashMap<>(Math.max(16, trips.size() / 8));
        for (Trip t : trips.values()) {
            if (t.routeId == null || t.routeId.isEmpty()) continue;
            byRoute.computeIfAbsent(t.routeId(), ignored -> new HashSet<>(8)).add(t.tripId());
        }
        return byRoute;
    }

    private Map<String, Set<String>> buildDestinationsIndex(Map<String, Trip> trips) {
        Map<String, Set<String>> out = new HashMap<>(Math.max(16, trips.size() / 8));
        for (Trip t : trips.values()) {
            if (t.routeId == null || t.routeId.isEmpty()) continue;
            if (t.headsign == null || t.headsign.isBlank()) continue;
            out.computeIfAbsent(t.routeId(), ignored -> new LinkedHashSet<>()).add(t.headsign().trim());
        }
        return out;
    }

    private Map<String, Set<String>> buildRouteIdsByLineIndex(Map<String, String> routeShortNames, Set<String> routeIds) {
        Map<String, Set<String>> out = new HashMap<>(Math.max(16, routeIds.size() / 4));
        for (String routeId : routeIds) {
            String line = routeShortNames.getOrDefault(routeId, routeId);
            out.computeIfAbsent(line, ignored -> new LinkedHashSet<>()).add(routeId);
        }
        return out;
    }

    private Set<String> referencedShapeIds(Map<String, Trip> trips) {
        Set<String> shapeIds = new HashSet<>(Math.max(16, trips.size() / 2));
        for (Trip trip : trips.values()) {
            if (trip.shapeId() != null && !trip.shapeId().isBlank()) {
                shapeIds.add(trip.shapeId());
            }
        }
        return shapeIds;
    }

    private void collectScheduledArrivals(
            List<ScheduledArrival> out,
            String stopId,
            LocalDate serviceDate,
            long nowEpochSeconds,
            long horizonEndEpochSeconds,
            int limit
    ) {
        Indexes indexes = ref.get();
        ScheduledStopIndex index = scheduledStopIndexForDate(serviceDate);
        List<ScheduledStopTime> stopTimes = index.byStopId().get(stopId);
        if (stopTimes == null || stopTimes.isEmpty()) {
            return;
        }

        long serviceStartEpochSeconds = serviceDate.atStartOfDay(ROME_ZONE).toEpochSecond();
        for (ScheduledStopTime stopTime : stopTimes) {
            long etaEpochSeconds = serviceStartEpochSeconds + stopTime.bestTimeSeconds();
            if (etaEpochSeconds < nowEpochSeconds) {
                continue;
            }
            if (etaEpochSeconds > horizonEndEpochSeconds) {
                break;
            }
            Trip trip = indexes.trips().get(stopTime.tripId());
            out.add(new ScheduledArrival(
                    trip != null ? publicLineByRouteId(trip.routeId()) : null,
                    trip != null ? trip.headsign() : null,
                    stopTime.tripId(),
                    stopId,
                    toInstantOrNull(serviceStartEpochSeconds, stopTime.arrivalTimeSeconds()),
                    toInstantOrNull(serviceStartEpochSeconds, stopTime.departureTimeSeconds()),
                    (int) Math.max(0L, Math.round((etaEpochSeconds - nowEpochSeconds) / 60.0)),
                    wheelchairAccessible(trip != null ? trip.wheelchair() : null)
            ));
            if (out.size() >= limit) {
                return;
            }
        }
    }

    private ScheduledStopIndex scheduledStopIndexForDate(LocalDate serviceDate) {
        Map<LocalDate, ScheduledStopIndex> current = scheduledByDateRef.get();
        ScheduledStopIndex cached = current.get(serviceDate);
        if (cached != null) {
            return cached;
        }

        synchronized (scheduledByDateRef) {
            current = scheduledByDateRef.get();
            cached = current.get(serviceDate);
            if (cached != null) {
                return cached;
            }

            ScheduledStopIndex built = buildScheduledStopIndex(serviceDate);
            Map<LocalDate, ScheduledStopIndex> next = new HashMap<>();
            for (Map.Entry<LocalDate, ScheduledStopIndex> entry : current.entrySet()) {
                LocalDate date = entry.getKey();
                if (!date.isBefore(serviceDate.minusDays(1)) && !date.isAfter(serviceDate.plusDays(1))) {
                    next.put(date, entry.getValue());
                }
            }
            next.put(serviceDate, built);
            scheduledByDateRef.set(Map.copyOf(next));
            return built;
        }
    }

    private ScheduledStopIndex buildScheduledStopIndex(LocalDate serviceDate) {
        long t0 = System.nanoTime();
        Indexes indexes = ref.get();
        Set<String> activeServiceIds = activeServiceIdsOn(serviceDate, indexes);
        if (activeServiceIds.isEmpty()) {
            return ScheduledStopIndex.EMPTY;
        }

        Set<String> activeTripIds = new HashSet<>();
        for (Trip trip : indexes.trips().values()) {
            if (trip.serviceId() != null && activeServiceIds.contains(trip.serviceId())) {
                activeTripIds.add(trip.tripId());
            }
        }
        if (activeTripIds.isEmpty()) {
            return ScheduledStopIndex.EMPTY;
        }

        Path stopTimesPath = dataDir.resolve("stop_times.txt");
        if (!Files.exists(stopTimesPath)) {
            log.warn("[GTFS-Index] stop_times.txt non trovato in {}", stopTimesPath);
            return ScheduledStopIndex.EMPTY;
        }

        Map<String, List<ScheduledStopTime>> byStopId = new HashMap<>();
        try (InputStream is = new BufferedInputStream(Files.newInputStream(stopTimesPath))) {
            CsvParser parser = newParser();
            parser.beginParsing(is, StandardCharsets.UTF_8);
            Record r;
            while ((r = parser.parseNextRecord()) != null) {
                String tripId = r.getString("trip_id");
                if (tripId == null || !activeTripIds.contains(tripId)) continue;

                String stopId = r.getString("stop_id");
                if (stopId == null || stopId.isBlank()) continue;

                Integer arrivalTimeSeconds = parseGtfsTimeToSeconds(r.getString("arrival_time"));
                Integer departureTimeSeconds = parseGtfsTimeToSeconds(r.getString("departure_time"));
                Integer bestTimeSeconds = arrivalTimeSeconds != null ? arrivalTimeSeconds : departureTimeSeconds;
                if (bestTimeSeconds == null) continue;

                Trip trip = indexes.trips().get(tripId);
                if (trip == null) continue;

                byStopId.computeIfAbsent(stopId, ignored -> new ArrayList<>(8)).add(new ScheduledStopTime(
                        tripId,
                        arrivalTimeSeconds != null ? arrivalTimeSeconds : -1,
                        departureTimeSeconds != null ? departureTimeSeconds : -1,
                        bestTimeSeconds
                ));
            }
            parser.stopParsing();
        } catch (IOException e) {
            log.error("[GTFS-Index] Errore costruendo indice stop_times per {}: {}", serviceDate, e.toString());
            return ScheduledStopIndex.EMPTY;
        }

        for (List<ScheduledStopTime> entries : byStopId.values()) {
            entries.sort(Comparator.comparingInt(ScheduledStopTime::bestTimeSeconds));
        }

        long ms = (System.nanoTime() - t0) / 1_000_000;
        log.info("[GTFS-Index] Indice orari programmati costruito per {}: services={} trips={} stops={} in {} ms",
                serviceDate, activeServiceIds.size(), activeTripIds.size(), byStopId.size(), ms);
        return new ScheduledStopIndex(serviceDate, freezeListMap(byStopId));
    }

    private void collectSimulatedTrips(
            List<SimulatedTrip> out,
            LocalDate serviceDate,
            String normalizedLine,
            String normalizedDestination,
            long nowEpochSeconds
    ) {
        ActiveTripIndex index = activeTripIndexForDate(serviceDate);
        if (index.byTripId().isEmpty()) {
            return;
        }

        for (ActiveTripSchedule schedule : index.byTripId().values()) {
            Trip trip = tripByIdOrNull(schedule.tripId());
            if (trip == null) continue;
            String publicLine = publicLineByRouteId(trip.routeId());
            if (!Objects.equals(normalizeText(publicLine), normalizedLine)) continue;
            if (normalizedDestination != null && !Objects.equals(normalizeText(trip.headsign()), normalizedDestination)) continue;

            List<ShapePoint> shape = shapeByTripId(schedule.tripId());
            if (shape.size() < 2) continue;

            long serviceStartEpochSeconds = serviceDate.atStartOfDay(ROME_ZONE).toEpochSecond();
            long startEpochSeconds = serviceStartEpochSeconds + schedule.startTimeSeconds();
            long endEpochSeconds = serviceStartEpochSeconds + schedule.endTimeSeconds();
            if (endEpochSeconds < nowEpochSeconds - 5 * 60L) continue;
            if (startEpochSeconds > nowEpochSeconds + 45 * 60L) continue;

            out.add(new SimulatedTrip(
                    publicLine,
                    trip.headsign(),
                    schedule.tripId(),
                    startEpochSeconds,
                    endEpochSeconds,
                    shape,
                    wheelchairAccessible(trip.wheelchair())
            ));
        }
    }

    private Optional<SimulatedTrip> simulatedTripByIdOnDate(String tripId, LocalDate serviceDate, long nowEpochSeconds) {
        ActiveTripSchedule schedule = activeTripIndexForDate(serviceDate).byTripId().get(tripId);
        if (schedule == null) {
            return Optional.empty();
        }

        Trip trip = tripByIdOrNull(tripId);
        if (trip == null) {
            return Optional.empty();
        }

        List<ShapePoint> shape = shapeByTripId(tripId);
        if (shape.size() < 2) {
            return Optional.empty();
        }

        long serviceStartEpochSeconds = serviceDate.atStartOfDay(ROME_ZONE).toEpochSecond();
        long startEpochSeconds = serviceStartEpochSeconds + schedule.startTimeSeconds();
        long endEpochSeconds = serviceStartEpochSeconds + schedule.endTimeSeconds();
        if (endEpochSeconds < nowEpochSeconds - 5 * 60L) {
            return Optional.empty();
        }
        if (startEpochSeconds > nowEpochSeconds + 45 * 60L) {
            return Optional.empty();
        }

        return Optional.of(new SimulatedTrip(
                publicLineByRouteId(trip.routeId()),
                trip.headsign(),
                trip.tripId(),
                startEpochSeconds,
                endEpochSeconds,
                shape,
                wheelchairAccessible(trip.wheelchair())
        ));
    }

    private ActiveTripIndex activeTripIndexForDate(LocalDate serviceDate) {
        Map<LocalDate, ActiveTripIndex> current = activeTripsByDateRef.get();
        ActiveTripIndex cached = current.get(serviceDate);
        if (cached != null) {
            return cached;
        }

        synchronized (activeTripsByDateRef) {
            current = activeTripsByDateRef.get();
            cached = current.get(serviceDate);
            if (cached != null) {
                return cached;
            }

            ActiveTripIndex built = buildActiveTripIndex(serviceDate);
            Map<LocalDate, ActiveTripIndex> next = new HashMap<>();
            for (Map.Entry<LocalDate, ActiveTripIndex> entry : current.entrySet()) {
                LocalDate date = entry.getKey();
                if (!date.isBefore(serviceDate.minusDays(1)) && !date.isAfter(serviceDate.plusDays(1))) {
                    next.put(date, entry.getValue());
                }
            }
            next.put(serviceDate, built);
            activeTripsByDateRef.set(Map.copyOf(next));
            return built;
        }
    }

    private ActiveTripIndex buildActiveTripIndex(LocalDate serviceDate) {
        long t0 = System.nanoTime();
        Indexes indexes = ref.get();
        Set<String> activeServiceIds = activeServiceIdsOn(serviceDate, indexes);
        if (activeServiceIds.isEmpty()) {
            return ActiveTripIndex.EMPTY;
        }

        Set<String> activeTripIds = new HashSet<>();
        for (Trip trip : indexes.trips().values()) {
            if (trip.serviceId() != null && activeServiceIds.contains(trip.serviceId())) {
                activeTripIds.add(trip.tripId());
            }
        }
        if (activeTripIds.isEmpty()) {
            return ActiveTripIndex.EMPTY;
        }

        Path stopTimesPath = dataDir.resolve("stop_times.txt");
        if (!Files.exists(stopTimesPath)) {
            return ActiveTripIndex.EMPTY;
        }

        Map<String, MutableTripWindow> windows = new HashMap<>();
        try (InputStream is = new BufferedInputStream(Files.newInputStream(stopTimesPath))) {
            CsvParser parser = newParser();
            parser.beginParsing(is, StandardCharsets.UTF_8);
            Record r;
            while ((r = parser.parseNextRecord()) != null) {
                String tripId = r.getString("trip_id");
                if (tripId == null || !activeTripIds.contains(tripId)) continue;

                Integer arrival = parseGtfsTimeToSeconds(r.getString("arrival_time"));
                Integer departure = parseGtfsTimeToSeconds(r.getString("departure_time"));
                Integer best = arrival != null ? arrival : departure;
                if (best == null) continue;

                MutableTripWindow window = windows.computeIfAbsent(tripId, ignored -> new MutableTripWindow());
                window.min = Math.min(window.min, best);
                window.max = Math.max(window.max, departure != null ? departure : best);
            }
            parser.stopParsing();
        } catch (IOException e) {
            log.error("[GTFS-Index] Errore costruendo indice trip attivi per {}: {}", serviceDate, e.toString());
            return ActiveTripIndex.EMPTY;
        }

        Map<String, ActiveTripSchedule> byTripId = new HashMap<>();
        for (Map.Entry<String, MutableTripWindow> entry : windows.entrySet()) {
            if (entry.getValue().min == Integer.MAX_VALUE || entry.getValue().max == Integer.MIN_VALUE) continue;
            byTripId.put(entry.getKey(), new ActiveTripSchedule(entry.getKey(), entry.getValue().min, entry.getValue().max));
        }

        long ms = (System.nanoTime() - t0) / 1_000_000;
        log.info("[GTFS-Index] Indice trip attivi costruito per {}: trips={} in {} ms", serviceDate, byTripId.size(), ms);
        return new ActiveTripIndex(serviceDate, Map.copyOf(byTripId));
    }

    private List<ScheduledTripStop> scheduledNextStopsForTripOnDate(Trip trip, LocalDate serviceDate, long nowEpochSeconds, int limit) {
        Indexes indexes = ref.get();
        if (!activeServiceIdsOn(serviceDate, indexes).contains(trip.serviceId())) {
            return List.of();
        }

        Path stopTimesPath = dataDir.resolve("stop_times.txt");
        if (!Files.exists(stopTimesPath)) {
            return List.of();
        }

        long serviceStartEpochSeconds = serviceDate.atStartOfDay(ROME_ZONE).toEpochSecond();
        List<ScheduledTripStop> out = new ArrayList<>(limit);
        try (InputStream is = new BufferedInputStream(Files.newInputStream(stopTimesPath))) {
            CsvParser parser = newParser();
            parser.beginParsing(is, StandardCharsets.UTF_8);
            Record r;
            while ((r = parser.parseNextRecord()) != null) {
                if (!trip.tripId().equals(r.getString("trip_id"))) continue;

                Integer arrivalTimeSeconds = parseGtfsTimeToSeconds(r.getString("arrival_time"));
                Integer departureTimeSeconds = parseGtfsTimeToSeconds(r.getString("departure_time"));
                Integer bestTimeSeconds = arrivalTimeSeconds != null ? arrivalTimeSeconds : departureTimeSeconds;
                if (bestTimeSeconds == null) continue;

                long bestEpochSeconds = serviceStartEpochSeconds + bestTimeSeconds;
                if (bestEpochSeconds < nowEpochSeconds - 5 * 60L) continue;

                String stopId = r.getString("stop_id");
                Stop stop = stopByIdOrNull(stopId);
                out.add(new ScheduledTripStop(
                        stopId,
                        stop != null ? stop.name() : null,
                        trip.tripId(),
                        publicLineByRouteId(trip.routeId()),
                        stop != null && stop.lat() != null ? stop.lat().doubleValue() : null,
                        stop != null && stop.lon() != null ? stop.lon().doubleValue() : null,
                        toInstantOrNull(serviceStartEpochSeconds, arrivalTimeSeconds),
                        toInstantOrNull(serviceStartEpochSeconds, departureTimeSeconds)
                ));
                if (out.size() >= limit) {
                    break;
                }
            }
            parser.stopParsing();
        } catch (IOException e) {
            log.error("[GTFS-Index] Errore leggendo stop_times per trip {}: {}", trip.tripId(), e.toString());
            return List.of();
        }

        return List.copyOf(out);
    }

    private List<ScheduledTripStop> scheduledStopsForTripOnDate(Trip trip, LocalDate serviceDate) {
        Indexes indexes = ref.get();
        if (!activeServiceIdsOn(serviceDate, indexes).contains(trip.serviceId())) {
            return List.of();
        }

        Path stopTimesPath = dataDir.resolve("stop_times.txt");
        if (!Files.exists(stopTimesPath)) {
            return List.of();
        }

        long serviceStartEpochSeconds = serviceDate.atStartOfDay(ROME_ZONE).toEpochSecond();
        List<ScheduledTripStop> out = new ArrayList<>();
        try (InputStream is = new BufferedInputStream(Files.newInputStream(stopTimesPath))) {
            CsvParser parser = newParser();
            parser.beginParsing(is, StandardCharsets.UTF_8);
            Record r;
            while ((r = parser.parseNextRecord()) != null) {
                if (!trip.tripId().equals(r.getString("trip_id"))) continue;

                Integer arrivalTimeSeconds = parseGtfsTimeToSeconds(r.getString("arrival_time"));
                Integer departureTimeSeconds = parseGtfsTimeToSeconds(r.getString("departure_time"));
                String stopId = r.getString("stop_id");
                Stop stop = stopByIdOrNull(stopId);

                out.add(new ScheduledTripStop(
                        stopId,
                        stop != null ? stop.name() : null,
                        trip.tripId(),
                        publicLineByRouteId(trip.routeId()),
                        stop != null && stop.lat() != null ? stop.lat().doubleValue() : null,
                        stop != null && stop.lon() != null ? stop.lon().doubleValue() : null,
                        toInstantOrNull(serviceStartEpochSeconds, arrivalTimeSeconds),
                        toInstantOrNull(serviceStartEpochSeconds, departureTimeSeconds)
                ));
            }
            parser.stopParsing();
        } catch (IOException e) {
            log.error("[GTFS-Index] Errore leggendo stop_times completi per trip {}: {}", trip.tripId(), e.toString());
            return List.of();
        }

        return List.copyOf(out);
    }

    private Set<String> activeServiceIdsOn(LocalDate serviceDate, Indexes indexes) {
        Set<String> added = indexes.addedServiceIdsByDate().getOrDefault(serviceDate, Set.of());
        if (added.isEmpty()) {
            return Set.of();
        }
        Set<String> removed = indexes.removedServiceIdsByDate().getOrDefault(serviceDate, Set.of());
        if (removed.isEmpty()) {
            return added;
        }

        Set<String> out = new HashSet<>(added);
        out.removeAll(removed);
        return out.isEmpty() ? Set.of() : Set.copyOf(out);
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    private static Integer parseIntOrNull(String s) {
        if (s == null || s.isEmpty()) return null;
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static LocalDate parseLocalDateOrNull(String value) {
        if (value == null || value.isBlank() || value.length() != 8) return null;
        try {
            return LocalDate.of(
                    Integer.parseInt(value.substring(0, 4)),
                    Integer.parseInt(value.substring(4, 6)),
                    Integer.parseInt(value.substring(6, 8))
            );
        } catch (Exception e) {
            return null;
        }
    }

    private static Integer parseGtfsTimeToSeconds(String value) {
        if (value == null || value.isBlank()) return null;
        String[] parts = value.trim().split(":");
        if (parts.length < 2) return null;
        try {
            int hours = Integer.parseInt(parts[0]);
            int minutes = Integer.parseInt(parts[1]);
            int seconds = parts.length >= 3 ? Integer.parseInt(parts[2]) : 0;
            if (hours < 0 || minutes < 0 || minutes >= 60 || seconds < 0 || seconds >= 60) {
                return null;
            }
            return hours * 3600 + minutes * 60 + seconds;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Float parseFloatOrNull(String s) {
        if (s == null || s.isEmpty()) return null;
        try {
            return Float.parseFloat(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String normalizeText(String value) {
        if (value == null) return null;
        String normalized = java.text.Normalizer.normalize(value, java.text.Normalizer.Form.NFD)
                .replaceAll("\\p{M}+", "")
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^\\p{Alnum}]+", " ")
                .trim()
                .replaceAll("\\s+", " ");
        return normalized.isEmpty() ? null : normalized;
    }

    private static Instant toInstantOrNull(long serviceStartEpochSeconds, Integer timeSeconds) {
        if (timeSeconds == null) return null;
        return toInstantOrNull(serviceStartEpochSeconds, timeSeconds.intValue());
    }

    private static Instant toInstantOrNull(long serviceStartEpochSeconds, int timeSeconds) {
        if (timeSeconds < 0) return null;
        return Instant.ofEpochSecond(serviceStartEpochSeconds + timeSeconds);
    }

    private static Boolean wheelchairAccessible(Integer wheelchair) {
        if (wheelchair == null) return null;
        return switch (wheelchair) {
            case 1 -> Boolean.TRUE;
            case 2 -> Boolean.FALSE;
            default -> null;
        };
    }

    private static Map<LocalDate, Set<String>> freezeSetMap(Map<LocalDate, Set<String>> source) {
        Map<LocalDate, Set<String>> out = new HashMap<>(source.size());
        for (Map.Entry<LocalDate, Set<String>> entry : source.entrySet()) {
            out.put(entry.getKey(), Set.copyOf(entry.getValue()));
        }
        return Map.copyOf(out);
    }

    private static <K, V> Map<K, List<V>> freezeListMap(Map<K, List<V>> source) {
        Map<K, List<V>> out = new HashMap<>(source.size());
        for (Map.Entry<K, List<V>> entry : source.entrySet()) {
            out.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        return Map.copyOf(out);
    }

    public record Indexes(
            Map<String, Stop> stops,
            Map<String, Trip> trips,
            Map<String, Set<String>> tripsByRoute,
            Map<String, Set<String>> destinationsByRoute,
            Map<String, String> routeShortNameByRouteId,
            Map<String, Set<String>> routeIdsByPublicLine,
            Map<String, List<ShapePoint>> shapesById,
            Map<LocalDate, Set<String>> addedServiceIdsByDate,
            Map<LocalDate, Set<String>> removedServiceIdsByDate
    ) {
        static final Indexes EMPTY = new Indexes(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    public record Stop(
            String id,
            String code,
            String name,
            String desc,
            Float lat,
            Float lon,
            String url,
            Integer wheelchairBoarding,
            String timezone,
            Integer locationType,
            String parentStation
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
    ) {}

    public record ShapePoint(
            float lat,
            float lon,
            int sequence
    ) {}

    public record ScheduledArrival(
            String line,
            String destination,
            String tripId,
            String stopId,
            Instant arrivalTime,
            Instant departureTime,
            int etaMinutes,
            Boolean wheelchairAccessible
    ) {}

    private record CalendarDatesIndex(
            Map<LocalDate, Set<String>> addedByDate,
            Map<LocalDate, Set<String>> removedByDate
    ) {}

    private record ScheduledStopTime(
            String tripId,
            int arrivalTimeSeconds,
            int departureTimeSeconds,
            int bestTimeSeconds
    ) {}

    private record ScheduledStopIndex(
            LocalDate serviceDate,
            Map<String, List<ScheduledStopTime>> byStopId
    ) {
        private static final ScheduledStopIndex EMPTY = new ScheduledStopIndex(LocalDate.MIN, Map.of());
    }

    private record ActiveTripIndex(
            LocalDate serviceDate,
            Map<String, ActiveTripSchedule> byTripId
    ) {
        private static final ActiveTripIndex EMPTY = new ActiveTripIndex(LocalDate.MIN, Map.of());
    }

    private record ActiveTripSchedule(
            String tripId,
            int startTimeSeconds,
            int endTimeSeconds
    ) {}

    private static final class MutableTripWindow {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
    }

    public record SimulatedTrip(
            String line,
            String destination,
            String tripId,
            long startEpochSeconds,
            long endEpochSeconds,
            List<ShapePoint> shape,
            Boolean wheelchairAccessible
    ) {
        public boolean activeAt(long nowEpochSeconds) {
            return startEpochSeconds <= nowEpochSeconds && nowEpochSeconds <= endEpochSeconds;
        }
    }

    public record ScheduledTripStop(
            String stopId,
            String stopName,
            String tripId,
            String line,
            Double lat,
            Double lon,
            Instant arrivalTime,
            Instant departureTime
    ) {}
}
