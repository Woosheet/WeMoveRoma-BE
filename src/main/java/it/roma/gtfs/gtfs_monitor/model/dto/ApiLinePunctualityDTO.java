package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;
import java.util.List;

/**
 * Puntualita' di una linea in un intervallo di date, per ora del giorno.
 *
 * Conteggi e non percentuali: chi legge deve poter vedere su quanti passaggi
 * poggia un numero, e un 80% su dieci campioni non vale un 80% su mille.
 *
 * {@code hours} somma tutti i giorni dell'intervallo; {@code dayTypes} divide gli
 * stessi passaggi per tipo di giorno (feriale, sabato, festivo), sempre tutti e
 * tre anche se vuoti.
 */
public record ApiLinePunctualityDTO(
        String line,
        String from,
        String to,
        int days,
        int samples,
        Thresholds thresholds,
        List<Hour> hours,
        List<DayType> dayTypes,
        Instant generatedAt
) {

    /** Estremi delle categorie, in minuti di ritardo sull'orario programmato. */
    public record Thresholds(double earlyBelowMin, double lateFromMin, double veryLateFromMin) {}

    /** days = giorni con almeno un passaggio. */
    public record DayType(String dayType, int days, int samples, List<Hour> hours) {}

    /** Solo le ore con passaggi. */
    public record Hour(int hour, int samples, int early, int onTime, int late, int veryLate) {}
}
