package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.NearbyArrivalDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.NearbyResponseDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.NearbyStopDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class NearbyService {
    private static final DateTimeFormatter ROME_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");

    private final GtfsIndexService gtfsIndexService;
    private final TripUpdatesService tripUpdatesService;
    private final VehiclePositionsService vehiclePositionsService;

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
                .map(sd -> toNearbyStop(sd, updatesByStop.getOrDefault(sd.stop.id(), List.of()), vehiclesByTripId, vehiclesById, arrivalsLimit, now))
                .toList();

        return new NearbyResponseDTO(lat, lon, radius, stops, Instant.now());
    }

    private NearbyStopDTO toNearbyStop(
            StopDistance sd,
            List<TripUpdateDTO> updates,
            Map<String, VehiclePositionDTO> vehiclesByTripId,
            Map<String, VehiclePositionDTO> vehiclesById,
            int arrivalsLimit,
            long now
    ) {
        List<NearbyArrivalDTO> arrivals = updates.stream()
                .map(u -> toArrival(u, vehiclesByTripId, vehiclesById, now))
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingInt(NearbyArrivalDTO::etaMinutes))
                .limit(arrivalsLimit)
                .toList();

        return new NearbyStopDTO(
                sd.stop.id(),
                sd.stop.name(),
                sd.stop.lat() != null ? sd.stop.lat().doubleValue() : null,
                sd.stop.lon() != null ? sd.stop.lon().doubleValue() : null,
                sd.distanceMeters,
                Math.max(1, (int) Math.round(sd.distanceMeters / 80.0)),
                arrivals
        );
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
}
