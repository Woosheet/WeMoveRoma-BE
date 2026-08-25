package it.roma.gtfs.gtfs_monitor.model.dto;

import java.util.List;

/**
 * Percorso di una linea: il modo di trasporto e una direzione per capolinea,
 * con le fermate in ordine.
 *
 * Pensato per le pagine pubbliche per linea, che hanno bisogno di contenuto
 * leggibile (l'elenco delle fermate servite) e non solo di una mappa, che per un
 * motore di ricerca non dice nulla.
 *
 * E' un <b>percorso canonico</b>, non l'orario di oggi: le fermate non portano
 * orari perche' un orario senza data di servizio non significa niente.
 */
public record ApiLinePatternDTO(
        String line,
        /** "bus", "tram", "metro", "treno": dal route_type del GTFS. */
        String mode,
        List<ApiLineDirectionDTO> directions
) {
    public record ApiLineDirectionDTO(
            Integer directionId,
            /** Capolinea della direzione, come compare sui mezzi. */
            String headsign,
            int stopCount,
            List<ApiLineStopDTO> stops,
            /**
             * Tracciato reale della corsa, per disegnare il percorso sulla mappa.
             * Segue le strade: unire le fermate con segmenti dritti darebbe un
             * disegno che taglia per i campi.
             */
            List<ApiLatLonDTO> shape
    ) {}

    /**
     * Coordinata del tracciato, arrotondata a cinque decimali (~1 metro).
     * Il feed le serializza con quattordici: sono nanometri, e su alcune linee
     * il tracciato ha oltre mille punti.
     */
    public record ApiLatLonDTO(double lat, double lon) {
        public static ApiLatLonDTO of(double lat, double lon) {
            return new ApiLatLonDTO(Math.round(lat * 1e5) / 1e5, Math.round(lon * 1e5) / 1e5);
        }
    }

    public record ApiLineStopDTO(
            String stopId,
            String stopName,
            Double lat,
            Double lon
    ) {}
}
