package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.ResourceNotFoundException;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiStopSearchItemDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.NearbyArrivalDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.NearbyResponseDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.NearbyStopDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class NearbyService {

    private static final ZoneId ROME_ZONE = ZoneId.of("Europe/Rome");
    private static final DateTimeFormatter ROME_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");

    private final GtfsIndexService gtfsIndexService;
    private final TripUpdatesService tripUpdatesService;
    private final VehiclePositionsService vehiclePositionsService;

    public List<ApiStopSearchItemDTO> searchStops(String query, Integer limit) {
        String normalizedQuery = normalizeText(query);
        if (normalizedQuery == null) {
            return List.of();
        }
        int max = limit == null || limit <= 0 ? 20 : Math.min(limit, 50);

        return gtfsIndexService.allStops().stream()
                .map(stop -> new ScoredStop(stop, stopScore(stop, normalizedQuery)))
                .filter(scored -> scored.score() > 0)
                .sorted(Comparator
                        .comparingInt(ScoredStop::score).reversed()
                        .thenComparing(scored -> scored.stop().name(), String.CASE_INSENSITIVE_ORDER))
                .limit(max)
                .map(scored -> new ApiStopSearchItemDTO(
                        scored.stop().id(),
                        scored.stop().code(),
                        scored.stop().name(),
                        scored.stop().lat() != null ? scored.stop().lat().doubleValue() : null,
                        scored.stop().lon() != null ? scored.stop().lon().doubleValue() : null
                ))
                .toList();
    }

    /**
     * Elenca le fermate, con diradamento spaziale quando sono piu' di {@code limit}.
     *
     * <p>Prima si ordinava per nome e si troncava: chiedere meno fermate dava una
     * fetta <em>alfabetica</em>, cioe' tutte ammassate dove capitava, non un
     * campione della zona. Con la mappa cittadina (7.215 fermate su Roma, 860 KB)
     * il client non ha modo di chiederne "un po' ovunque".
     *
     * <p>Ora, se il risultato eccede il tetto richiesto, si tiene una fermata per
     * cella di una griglia: il campione resta distribuito su tutto il riquadro.
     */
    public List<ApiStopSearchItemDTO> listStops(
            Double minLat,
            Double maxLat,
            Double minLon,
            Double maxLon,
            Integer limit
    ) {
        int max = limit == null || limit <= 0 ? 10000 : Math.min(limit, 10000);
        boolean bounded = minLat != null && maxLat != null && minLon != null && maxLon != null;

        List<GtfsIndexService.Stop> candidate = gtfsIndexService.allStops().stream()
                .filter(stop -> stop.lat() != null && stop.lon() != null)
                .filter(stop -> !bounded || (
                        stop.lat() >= minLat && stop.lat() <= maxLat &&
                        stop.lon() >= minLon && stop.lon() <= maxLon
                ))
                .sorted(Comparator.comparing(GtfsIndexService.Stop::name, String.CASE_INSENSITIVE_ORDER))
                .toList();

        List<GtfsIndexService.Stop> scelte = candidate.size() <= max
                ? candidate
                : diradaSuGriglia(candidate, max);

        return scelte.stream()
                .map(stop -> new ApiStopSearchItemDTO(
                        stop.id(),
                        stop.code(),
                        stop.name(),
                        stop.lat() != null ? stop.lat().doubleValue() : null,
                        stop.lon() != null ? stop.lon().doubleValue() : null
                ))
                .toList();
    }

    /**
     * Tiene una fermata per cella di griglia, per ottenere {@code max} punti
     * distribuiti invece di una fetta ordinata per nome.
     *
     * <p>Il lato della cella e' <strong>quantizzato su potenze di due</strong> e la
     * griglia e' ancorata all'origine delle coordinate, non al riquadro chiesto.
     * Serve a rendere il campione stabile: senza, ogni piccolo spostamento della
     * mappa cambierebbe leggermente il lato della cella, e le fermate mostrate
     * salterebbero da una all'altra a ogni trascinamento.
     *
     * <p>La fermata scelta dentro una cella e' la prima in ordine di nome: e'
     * arbitraria ma <em>deterministica</em>, quindi la stessa cella restituisce
     * sempre la stessa fermata.
     */
    private List<GtfsIndexService.Stop> diradaSuGriglia(List<GtfsIndexService.Stop> stops, int max) {
        double minLat = Double.MAX_VALUE, maxLat = -Double.MAX_VALUE;
        double minLon = Double.MAX_VALUE, maxLon = -Double.MAX_VALUE;
        for (GtfsIndexService.Stop s : stops) {
            double la = s.lat().doubleValue(), lo = s.lon().doubleValue();
            if (la < minLat) minLat = la;
            if (la > maxLat) maxLat = la;
            if (lo < minLon) minLon = lo;
            if (lo > maxLon) maxLon = lo;
        }
        double span = Math.max(maxLat - minLat, maxLon - minLon);
        if (span <= 0) {
            return stops.subList(0, Math.min(max, stops.size()));
        }

        /*
         * Il lato della cella e' quantizzato su una scala discreta e la griglia
         * e' ancorata all'origine delle coordinate, non al riquadro chiesto.
         * Serve a rendere il campione stabile: senza, ogni piccolo spostamento
         * della mappa cambierebbe di poco il lato, e le fermate mostrate
         * salterebbero da una all'altra a ogni trascinamento.
         *
         * La scala ha passo 2^(1/4) e non 2. Con i raddoppi il lato saltava
         * troppo: dimezzandolo le celle quadruplicano, quindi o si sforava il
         * tetto o si restava larghissimi — con tetto 400 tornavano 136 fermate,
         * due terzi del budget sprecati. Un passo di 2^(1/4) cambia il numero
         * di celle di circa 1,4 volte per scalino, e permette di avvicinarsi.
         */
        final double PASSO = Math.pow(2, 0.25);
        double grezzo = span / Math.sqrt(max);
        double lato = Math.pow(PASSO, Math.round(Math.log(grezzo) / Math.log(PASSO)));

        // Si allarga finche' le celle occupate stanno sotto il tetto.
        List<GtfsIndexService.Stop> migliore = null;
        for (int giro = 0; giro < 40 && migliore == null; giro++) {
            List<GtfsIndexService.Stop> tentativo = unaPerCella(stops, lato);
            if (tentativo.size() <= max) {
                migliore = tentativo;
            } else {
                lato *= PASSO;
            }
        }
        if (migliore == null) {
            return stops.subList(0, Math.min(max, stops.size()));
        }

        // Poi si stringe finche' ci si sta ancora dentro, per non sprecare il
        // budget. Resta deterministico: stesso insieme in ingresso, stesso lato.
        for (int giro = 0; giro < 8; giro++) {
            List<GtfsIndexService.Stop> piuFitto = unaPerCella(stops, lato / PASSO);
            if (piuFitto.size() > max) break;
            lato /= PASSO;
            migliore = piuFitto;
        }
        return migliore;
    }

    /** Una fermata per cella di lato {@code lato}, in ordine stabile. */
    private List<GtfsIndexService.Stop> unaPerCella(List<GtfsIndexService.Stop> stops, double lato) {
        LinkedHashMap<Long, GtfsIndexService.Stop> perCella = new LinkedHashMap<>();
        for (GtfsIndexService.Stop s : stops) {
            long ry = (long) Math.floor(s.lat().doubleValue() / lato);
            long rx = (long) Math.floor(s.lon().doubleValue() / lato);
            perCella.putIfAbsent(ry * 1_000_003L + rx, s);
        }
        return List.copyOf(perCella.values());
    }

    /**
     * Arrivi a una fermata.
     *
     * @throws ResourceNotFoundException se la fermata non esiste nel feed caricato.
     *         Prima si restituiva un 200 con nome fittizio "Fermata non trovata": il
     *         client non poteva distinguere un id sbagliato da una fermata vera senza
     *         corse in arrivo, e finiva per mostrare una fermata inesistente come se
     *         fosse solo momentaneamente vuota. Una fermata che esiste ma non ha
     *         arrivi resta 200 con {@code arrivals} vuoto.
     */
    public NearbyStopDTO stopArrivals(String stopId, Integer limitArrivalsPerStop) {
        if (stopId == null || stopId.isBlank()) {
            throw new IllegalArgumentException("stopId: obbligatorio");
        }
        GtfsIndexService.Stop stop = gtfsIndexService.stopByIdOrNull(stopId);
        if (stop == null) {
            throw ResourceNotFoundException.stop(stopId);
        }

        int arrivalsLimit = limitArrivalsPerStop == null || limitArrivalsPerStop <= 0 ? 8 : Math.min(limitArrivalsPerStop, 12);
        long now = System.currentTimeMillis();
        List<TripUpdateDTO> updates = tripUpdatesService.fetch(null, null);
        List<VehiclePositionDTO> vehicles = vehiclePositionsService.fetch(null, null, null);

        List<TripUpdateDTO> updatesForStop = updates.stream()
                .filter(u -> stopId.equals(u.getFermataId()))
                .toList();

        Map<String, VehiclePositionDTO> vehiclesByTripId = vehicles.stream()
                .filter(v -> v.getCorsa() != null && !v.getCorsa().isBlank())
                .collect(Collectors.toMap(VehiclePositionDTO::getCorsa, v -> v, (a, b) -> a));
        Map<String, VehiclePositionDTO> vehiclesById = vehicles.stream()
                .filter(v -> v.getVeicolo() != null && !v.getVeicolo().isBlank())
                .collect(Collectors.toMap(VehiclePositionDTO::getVeicolo, v -> v, (a, b) -> a));

        return toNearbyStop(new StopDistance(stop, 0), updatesForStop, vehiclesByTripId, vehiclesById, arrivalsLimit, now, false);
    }

    public NearbyResponseDTO nearby(double lat, double lon, Integer radiusMeters, Integer limitStops, Integer limitArrivalsPerStop) {
        int radius = radiusMeters == null || radiusMeters <= 0 ? 500 : Math.min(radiusMeters, 3000);
        int stopsLimit = limitStops == null || limitStops <= 0 ? 8 : Math.min(limitStops, 30);
        int arrivalsLimit = limitArrivalsPerStop == null || limitArrivalsPerStop <= 0 ? 6 : Math.min(limitArrivalsPerStop, 12);

        long now = System.currentTimeMillis();
        List<TripUpdateDTO> updates = tripUpdatesService.fetch(null, null);
        List<VehiclePositionDTO> vehicles = vehiclePositionsService.fetch(null, null, null);

        Map<String, List<TripUpdateDTO>> updatesByStop = updates.stream()
                .filter(u -> u.getFermataId() != null && !u.getFermataId().isBlank())
                .collect(Collectors.groupingBy(TripUpdateDTO::getFermataId));

        Map<String, VehiclePositionDTO> vehiclesByTripId = vehicles.stream()
                .filter(v -> v.getCorsa() != null && !v.getCorsa().isBlank())
                .collect(Collectors.toMap(VehiclePositionDTO::getCorsa, v -> v, (a, b) -> a));
        Map<String, VehiclePositionDTO> vehiclesById = vehicles.stream()
                .filter(v -> v.getVeicolo() != null && !v.getVeicolo().isBlank())
                .collect(Collectors.toMap(VehiclePositionDTO::getVeicolo, v -> v, (a, b) -> a));

        List<NearbyStopDTO> stops = gtfsIndexService.allStops().stream()
                .filter(s -> s.lat() != null && s.lon() != null)
                .map(stop -> new StopDistance(stop, haversineMeters(lat, lon, stop.lat(), stop.lon())))
                .filter(sd -> sd.distanceMeters <= radius)
                .sorted(Comparator.comparingInt(sd -> sd.distanceMeters))
                .limit(stopsLimit)
                .map(sd -> toNearbyStop(sd, updatesByStop.getOrDefault(sd.stop.id(), List.of()), vehiclesByTripId, vehiclesById, arrivalsLimit, now, true))
                .toList();

        return new NearbyResponseDTO(lat, lon, radius, stops, Instant.now());
    }

    private NearbyStopDTO toNearbyStop(
            StopDistance sd,
            List<TripUpdateDTO> updates,
            Map<String, VehiclePositionDTO> vehiclesByTripId,
            Map<String, VehiclePositionDTO> vehiclesById,
            int arrivalsLimit,
            long now,
            boolean includeDistance
    ) {
        List<NearbyArrivalDTO> liveArrivals = updates.stream()
                .map(u -> toArrival(u, vehiclesByTripId, vehiclesById, now))
                .filter(Objects::nonNull)
                .toList();
        List<NearbyArrivalDTO> scheduledArrivals = gtfsIndexService
                .scheduledArrivalsForStop(sd.stop.id(), Instant.ofEpochMilli(now), 180, arrivalsLimit * 3)
                .stream()
                .map(this::toScheduledArrival)
                .toList();
        List<NearbyArrivalDTO> arrivals = mergeArrivals(liveArrivals, scheduledArrivals, arrivalsLimit);

        // Le linee servite non dipendono dagli arrivi del momento: di notte gli
        // arrivi sono vuoti ma la fermata resta servita da quelle linee (e i
        // relativi avvisi di servizio vanno mostrati comunque).
        List<String> servedLines = gtfsIndexService.linesServingStop(
                sd.stop.id(), LocalDate.now(ROME_ZONE));

        return new NearbyStopDTO(
                sd.stop.id(),
                sd.stop.name(),
                sd.stop.lat() != null ? sd.stop.lat().doubleValue() : null,
                sd.stop.lon() != null ? sd.stop.lon().doubleValue() : null,
                includeDistance ? sd.distanceMeters : null,
                includeDistance ? Math.max(1, (int) Math.round(sd.distanceMeters / 80.0)) : null,
                servedLines,
                arrivals
        );
    }

    private static int stopScore(GtfsIndexService.Stop stop, String normalizedQuery) {
        String id = normalizeText(stop.id());
        String code = normalizeText(stop.code());
        String name = normalizeText(stop.name());
        String desc = normalizeText(stop.desc());

        if (normalizedQuery.equals(id) || normalizedQuery.equals(code)) return 120;
        if (name != null && name.equals(normalizedQuery)) return 110;
        if (code != null && code.startsWith(normalizedQuery)) return 100;
        if (id != null && id.startsWith(normalizedQuery)) return 95;
        if (name != null && name.startsWith(normalizedQuery)) return 90;
        if (desc != null && desc.startsWith(normalizedQuery)) return 75;
        if (name != null && name.contains(normalizedQuery)) return 60;
        if (desc != null && desc.contains(normalizedQuery)) return 45;
        return 0;
    }

    private static String normalizeText(String value) {
        if (value == null) return null;
        String normalized = Normalizer.normalize(value, Normalizer.Form.NFD)
                .replaceAll("\\p{M}+", "")
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^\\p{Alnum}]+", " ")
                .trim()
                .replaceAll("\\s+", " ");
        return normalized.isEmpty() ? null : normalized;
    }

    private NearbyArrivalDTO toScheduledArrival(GtfsIndexService.ScheduledArrival arrival) {
        return new NearbyArrivalDTO(
                arrival.line(),
                arrival.destination(),
                arrival.tripId(),
                arrival.stopId(),
                null,
                arrival.arrivalTime() != null ? arrival.arrivalTime().toString() : null,
                arrival.departureTime() != null ? arrival.departureTime().toString() : null,
                arrival.etaMinutes(),
                Boolean.FALSE,
                null,
                arrival.wheelchairAccessible()
        );
    }

    private List<NearbyArrivalDTO> mergeArrivals(
            List<NearbyArrivalDTO> liveArrivals,
            List<NearbyArrivalDTO> scheduledArrivals,
            int arrivalsLimit
    ) {
        Set<String> liveTripIds = liveArrivals.stream()
                .map(NearbyArrivalDTO::tripId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        List<NearbyArrivalDTO> merged = new ArrayList<>(liveArrivals.size() + scheduledArrivals.size());
        merged.addAll(liveArrivals);
        for (NearbyArrivalDTO scheduled : scheduledArrivals) {
            if (scheduled.tripId() != null && liveTripIds.contains(scheduled.tripId())) {
                continue;
            }
            merged.add(scheduled);
        }

        return merged.stream()
                .sorted(Comparator.comparingInt(NearbyArrivalDTO::etaMinutes))
                .limit(arrivalsLimit)
                .toList();
    }

    private NearbyArrivalDTO toArrival(
            TripUpdateDTO dto,
            Map<String, VehiclePositionDTO> vehiclesByTripId,
            Map<String, VehiclePositionDTO> vehiclesById,
            long now
    ) {
        Long etaMs = bestTimeMillis(dto);
        if (etaMs == null) {
            return null;
        }
        int etaMin = (int) Math.max(0, Math.round((etaMs - now) / 60000.0));
        if (etaMin > 180) {
            return null;
        }

        GtfsIndexService.Trip trip = gtfsIndexService.tripByIdOrNull(dto.getCorsa());
        String destination = trip != null ? trip.headsign() : null;
        VehiclePositionDTO vehicle = findVehicle(dto, vehiclesByTripId, vehiclesById);
        Boolean wheelchairAccessible = vehicle != null
                ? vehicle.getWheelchairAccessible()
                : wheelchairAccessible(trip);

        return new NearbyArrivalDTO(
                gtfsIndexService.publicLineByRouteId(dto.getLinea()),
                destination,
                dto.getCorsa(),
                dto.getFermataId(),
                dto.getFermataNome(),
                toIso(dto.getArrivo()),
                toIso(dto.getPartenza()),
                etaMin,
                vehicle != null,
                vehicle != null ? vehicle.getOccupancyStatus() : null,
                wheelchairAccessible
        );
    }

    private static VehiclePositionDTO findVehicle(
            TripUpdateDTO dto,
            Map<String, VehiclePositionDTO> vehiclesByTripId,
            Map<String, VehiclePositionDTO> vehiclesById
    ) {
        if (dto.getCorsa() != null && !dto.getCorsa().isBlank()) {
            VehiclePositionDTO byTrip = vehiclesByTripId.get(dto.getCorsa());
            if (byTrip != null) return byTrip;
        }
        if (dto.getVeicolo() != null && !dto.getVeicolo().isBlank()) {
            return vehiclesById.get(dto.getVeicolo());
        }
        return null;
    }

    private static Boolean wheelchairAccessible(GtfsIndexService.Trip trip) {
        if (trip == null || trip.wheelchair() == null) return null;
        if (trip.wheelchair() == 1) return Boolean.TRUE;
        if (trip.wheelchair() == 2) return Boolean.FALSE;
        return null;
    }

    private static Long bestTimeMillis(TripUpdateDTO dto) {
        Long arr = parseIsoMillis(dto.getArrivo());
        Long dep = parseIsoMillis(dto.getPartenza());
        if (arr == null) return dep;
        if (dep == null) return arr;
        return Math.min(arr, dep);
    }

    private static Long parseIsoMillis(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return ZonedDateTime.parse(value, ROME_TS).toInstant().toEpochMilli();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String toIso(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return ZonedDateTime.parse(value, ROME_TS).toInstant().toString();
        } catch (Exception e) {
            return value;
        }
    }

    private static int haversineMeters(double lat1, double lon1, float lat2, float lon2) {
        double r = 6371000.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return (int) Math.round(r * c);
    }

    private record StopDistance(GtfsIndexService.Stop stop, int distanceMeters) {}
    private record ScoredStop(GtfsIndexService.Stop stop, int score) {}
}
