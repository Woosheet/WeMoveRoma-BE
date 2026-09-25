package it.roma.gtfs.gtfs_monitor.model.dto;

import java.util.List;

/**
 * Orari di tutta la rete fermata per fermata, in una giornata di servizio.
 *
 * Serve a chi genera le pagine statiche delle fermate: una chiamata per
 * giorno-tipo (feriale, sabato, festivo) invece di una per linea, e soprattutto
 * gli orari <b>alla fermata</b> e non quelli del capolinea, che erano l'errore
 * che queste pagine mostravano prima.
 *
 * <p>Gli orari sono in secondi dalla mezzanotte del giorno di servizio e
 * <b>possono superare le 24 ore</b>: nel GTFS una corsa delle 2 di notte
 * appartiene al giorno precedente ed e' scritta 26:00. Chi presenta il dato
 * decide come scriverlo — 92.520 secondi sono "02:02", ma del giorno dopo.
 */
public record ApiStopSchedulesDTO(
        String serviceDate,
        int count,
        List<Row> rows
) {
    /**
     * Una linea a una fermata, in un verso.
     *
     * La stessa palina puo' avere la stessa linea nei due sensi: direzione e
     * capolinea distinguono le due righe, e sono gli stessi che escono da
     * {@code /lines/{line}/pattern}, cosi' le due risposte si incastrano.
     */
    public record Row(
            String stopId,
            String line,
            Integer directionId,
            String headsign,
            int firstSeconds,
            int lastSeconds,
            int trips,
            /** Intervallo tipico fra due passaggi, in minuti. Null con meno di tre corse. */
            Integer headwayMinutes
    ) {}
}
