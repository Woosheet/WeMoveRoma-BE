package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;
import java.util.List;

/**
 * Dove i bus perdono tempo: i tratti fra due fermate in cui le corse accumulano
 * ritardo, per un tipo di giorno e una fascia oraria. Vedi SlowSegmentsService.
 *
 * {@code days} sono i giorni di quel tipo con dati nell'intervallo: i valori "al
 * giorno" sono divisi per loro. Solo i tratti in cui, sommando tutte le corse,
 * si perde tempo; in ordine di minuti persi al giorno, dal peggiore.
 */
public record ApiSlowSegmentsDTO(
        String dayType,
        String band,
        /** La linea richiesta, o null per tutti i tratti. */
        String line,
        String from,
        String to,
        int days,
        List<Segment> segments,
        Instant generatedAt
) {

    /**
     * @param minutesLostPerDay tutti i bus sommati: l'impatto del tratto
     * @param minutesPerRun     quanto perde, in media, ogni corsa
     * @param p90MinutesPerRun  quanto perde almeno una corsa su dieci. Approssimato:
     *                          media dei valori giornalieri pesata sulle corse
     * @param coordinates       [lon, lat] lungo la strada; due punti se il tracciato
     *                          non si e' trovato (linea dritta fra le fermate)
     */
    public record Segment(
            String fromStopId,
            String fromStopName,
            String toStopId,
            String toStopName,
            List<String> lines,
            double runsPerDay,
            double minutesLostPerDay,
            double minutesPerRun,
            double p90MinutesPerRun,
            int lengthMeters,
            List<double[]> coordinates
    ) {}
}
