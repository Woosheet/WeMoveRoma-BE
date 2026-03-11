package it.roma.gtfs.gtfs_monitor.controller;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiPlannerLiveStopFocusDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiVehicleDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;
import it.roma.gtfs.gtfs_monitor.model.dto.VehiclePositionDTO;
import it.roma.gtfs.gtfs_monitor.service.GtfsIndexService;
import it.roma.gtfs.gtfs_monitor.service.TripUpdatesService;
import it.roma.gtfs.gtfs_monitor.service.VehiclePositionsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/planner")
public class ApiPlannerController {
    private static final DateTimeFormatter ROME_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");

    private final VehiclePositionsService vehiclePositionsService;
    private final TripUpdatesService tripUpdatesService;
    private final GtfsIndexService gtfsIndexService;

    @GetMapping("/live-stop-focus")
    public ApiPlannerLiveStopFocusDTO getLiveStopFocus(
            @RequestParam String linea,
            @RequestParam String stopName,
            @RequestParam(required = false) String headsign,
            @RequestParam(required = false) String tripId,
            @RequestParam(required = false) Double stopLat,
            @RequestParam(required = false) Double stopLon
    ) {
        String line = trimToNull(linea);
        String stop = trimToNull(stopName);
        String selectedTripId = trimToNull(tripId);
        if (line == null || stop == null) {
            return new ApiPlannerLiveStopFocusDTO(null, "ETA non disponibile", null, null, "invalid", false, null, null);
        }

        List<VehiclePositionDTO> vehicles = vehiclePositionsService.fetch(line, null, 500);
        List<TripUpdateDTO> updates = tripUpdatesService.fetch(null, null);

        long now = System.currentTimeMillis();
        String normalizedStop = normalize(stop);
        String normalizedHeadsign = normalize(headsign);

        List<Candidate> candidates = new ArrayList<>();
        for (TripUpdateDTO update : updates) {
            if (!gtfsIndexService.matchesLine(line, update.getLinea())) continue;
            if (selectedTripId != null && !Objects.equals(selectedTripId, trimToNull(update.getCorsa()))) continue;
            if (trimToNull(update.getFermataNome()) == null) continue;
            if (!isLikelySameStopName(normalizedStop, normalize(update.getFermataNome()))) continue;

            Long ts = bestTimeMillis(update);
            if (ts != null && ts < now - 30_000L) continue;

            VehiclePositionDTO vehicle = findVehicleForUpdate(vehicles, update);
            int directionPenalty = directionPenalty(normalizedHeadsign, vehicle != null ? vehicle.getCapolinea() : null);
            double distancePenalty = stopDistancePenalty(stopLat, stopLon, update);
            long etaPenalty = ts != null ? Math.max(0L, ts - now) : Long.MAX_VALUE;

            candidates.add(new Candidate(update, vehicle, ts, directionPenalty, distancePenalty, etaPenalty));
        }

        candidates.sort(Comparator
                .comparingInt(Candidate::directionPenalty)
                .thenComparingDouble(Candidate::stopDistancePenalty)
                .thenComparingLong(Candidate::etaPenalty));

        Candidate best = candidates.isEmpty() ? null : candidates.get(0);
        if (best == null) {
            VehiclePositionDTO fallback = vehicles.stream()
                    .filter(v -> selectedTripId == null || Objects.equals(v.getCorsa(), selectedTripId))
                    .sorted(
                            Comparator.<VehiclePositionDTO>comparingInt(v -> directionPenalty(normalizedHeadsign, v.getCapolinea()))
                                    .thenComparingDouble(v -> stopDistancePenalty(stopLat, stopLon, v))
                    )
                    .findFirst()
                    .orElse(null);
            if (fallback == null) {
                return new ApiPlannerLiveStopFocusDTO(null, "ETA non disponibile", null, null, "none", false, null, null);
            }
            return new ApiPlannerLiveStopFocusDTO(
                    toApiVehicle(fallback),
                    "ETA non disponibile",
                    null,
                    null,
                    "fallback-nearest",
                    true,
                    fallback.getOccupancyStatus(),
                    fallback.getWheelchairAccessible()
            );
        }

        String etaLabel = "ETA non disponibile";
        String etaTime = null;
        if (best.etaTs() != null) {
            long diffMin = Math.round((best.etaTs() - now) / 60000d);
            etaLabel = diffMin <= 0 ? "In arrivo" : "Tra " + diffMin + " min";
            etaTime = Instant.ofEpochMilli(best.etaTs()).toString();
        }

        VehiclePositionDTO vehicle = best.vehicle();
        if (vehicle == null) {
            GtfsIndexService.Trip trip = best.update().getCorsa() != null
                    ? gtfsIndexService.tripByIdOrNull(best.update().getCorsa())
                    : null;
            return new ApiPlannerLiveStopFocusDTO(
                    null,
                    etaLabel,
                    etaTime,
                    best.etaTs(),
                    "scheduled-no-live",
                    false,
                    null,
                    wheelchairAccessible(trip)
            );
        }

        return new ApiPlannerLiveStopFocusDTO(
                toApiVehicle(vehicle),
                etaLabel,
                etaTime,
                best.etaTs(),
                "realtime",
                true,
                vehicle.getOccupancyStatus(),
                vehicle.getWheelchairAccessible()
        );
    }

    private VehiclePositionDTO findVehicleForUpdate(List<VehiclePositionDTO> vehicles, TripUpdateDTO update) {
        String tripId = trimToNull(update.getCorsa());
        String vehicleId = trimToNull(update.getVeicolo());
        for (VehiclePositionDTO v : vehicles) {
            if (tripId != null && Objects.equals(v.getCorsa(), tripId)) return v;
            if (vehicleId != null && Objects.equals(v.getVeicolo(), vehicleId)) return v;
        }
        return null;
    }

    private ApiVehicleDTO toApiVehicle(VehiclePositionDTO dto) {
        return new ApiVehicleDTO(
                dto.getVeicolo(),
                gtfsIndexService.publicLineByRouteId(dto.getLinea()),
                dto.getCapolinea(),
                dto.getCorsa(),
                dto.getLat(),
                dto.getLon(),
                dto.getVelocitaKmh(),
                dto.getTimestamp(),
                dto.getOccupancyStatus(),
                dto.getWheelchairAccessible()
        );
    }

    private static Boolean wheelchairAccessible(GtfsIndexService.Trip trip) {
        if (trip == null || trip.wheelchair() == null) return null;
        return switch (trip.wheelchair()) {
            case 1 -> true;
            case 2 -> false;
            default -> null;
        };
    }

    private static int directionPenalty(String normalizedHeadsign, String destination) {
        if (normalizedHeadsign == null) return 0;
        String normalizedDest = normalize(destination);
        if (normalizedDest == null) return 1;
        return normalizedDest.contains(normalizedHeadsign) ? 0 : 1;
    }

    private double stopDistancePenalty(Double stopLat, Double stopLon, TripUpdateDTO update) {
        if (stopLat == null || stopLon == null || update.getFermataId() == null) return 999_999d;
        GtfsIndexService.Stop stop = gtfsIndexService.stopByIdOrNull(update.getFermataId());
        if (stop == null || stop.lat() == null || stop.lon() == null) return 999_999d;
        return distanceMeters(stopLat, stopLon, stop.lat(), stop.lon());
    }

    private static double stopDistancePenalty(Double stopLat, Double stopLon, VehiclePositionDTO vehicle) {
        if (stopLat == null || stopLon == null || vehicle.getLat() == null || vehicle.getLon() == null) return 999_999d;
        return distanceMeters(stopLat, stopLon, vehicle.getLat(), vehicle.getLon());
    }

    private static Long bestTimeMillis(TripUpdateDTO dto) {
        Long arr = parseRomeMillis(dto.getArrivo());
        Long dep = parseRomeMillis(dto.getPartenza());
        if (arr == null) return dep;
        if (dep == null) return arr;
        return Math.min(arr, dep);
    }

    private static Long parseRomeMillis(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return ZonedDateTime.parse(value, ROME_TS).toInstant().toEpochMilli();
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String trimToNull(String value) {
        if (value == null) return null;
        String out = value.trim();
        return out.isEmpty() ? null : out;
    }

    private static String normalize(String value) {
        String t = trimToNull(value);
        if (t == null) return null;
        return java.text.Normalizer.normalize(t, java.text.Normalizer.Form.NFD)
                .replaceAll("\\p{M}+", "")
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^\\p{Alnum}]+", " ")
                .trim()
                .replaceAll("\\s+", " ");
    }

    private static boolean isLikelySameStopName(String target, String candidate) {
        if (target == null || candidate == null) return false;
        if (target.equals(candidate)) return true;
        if (target.contains(candidate) || candidate.contains(target)) return true;
        String[] targetTokens = target.split(" ");
        String[] candidateTokens = candidate.split(" ");
        int common = 0;
        for (String tt : targetTokens) {
            if (tt.length() <= 1) continue;
            for (String ct : candidateTokens) {
                if (tt.equals(ct)) {
                    common++;
                    break;
                }
            }
        }
        return common >= Math.min(2, targetTokens.length);
    }

    private static double distanceMeters(double lat1, double lon1, double lat2, double lon2) {
        final double r = 6_371_000d;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return r * c;
    }

    private record Candidate(
            TripUpdateDTO update,
            VehiclePositionDTO vehicle,
            Long etaTs,
            int directionPenalty,
            double stopDistancePenalty,
            long etaPenalty
    ) {
    }
}
