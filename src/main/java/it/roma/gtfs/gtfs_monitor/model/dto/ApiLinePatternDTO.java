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
            List<ApiLineStopDTO> stops
    ) {}

    public record ApiLineStopDTO(
            String stopId,
            String stopName,
            Double lat,
            Double lon
    ) {}
}
