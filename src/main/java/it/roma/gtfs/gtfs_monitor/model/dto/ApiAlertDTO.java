package it.roma.gtfs.gtfs_monitor.model.dto;

import java.time.Instant;
import java.util.List;

public record ApiAlertDTO(
        String alertId,
        /**
         * Prima linea di {@link #lines()}, mantenuta per non rompere i client
         * esistenti. Da considerare deprecata: un avviso puo' riguardare piu'
         * linee (a Roma capita per 1 avviso su 5) e questo campo ne mostra una sola.
         */
        String line,
        /** Tutte le linee coinvolte dall'avviso. */
        List<String> lines,
        /**
         * Severita' da usare per presentare l'avviso: INFO / WARNING / SEVERE
         * (vocabolario {@code SeverityLevel} di GTFS-RT). Mai {@code null}.
         *
         * <p>Attenzione: non e' necessariamente cio' che dice il feed. Vale
         * {@link #declaredSeverity()} quando il feed la dichiara, altrimenti e'
         * derivata da {@link #effectCode()} — vedi {@code AlertSeverityResolver}
         * per la mappatura. {@link #severitySource()} dice sempre quale dei due casi.
         */
        String severity,
        /** {@code FEED} se {@link #severity()} viene dal feed, {@code DERIVED} se calcolata dal backend. */
        String severitySource,
        /**
         * Severita' dichiarata dal feed, verbatim. {@code null} quando il feed non la
         * popola — che sul feed di Roma Mobilita' e' sempre. Serve a non far credere
         * che il feed dica qualcosa che non dice.
         */
        String declaredSeverity,
        /** Causa dell'avviso in italiano (es. "Lavori"). Null se il feed non la indica. */
        String cause,
        /** Effetto sul servizio in italiano (es. "Deviazione"). Null se il feed non lo indica. */
        String effect,
        /** Codice GTFS-RT {@code Alert.Cause} non tradotto (es. {@code CONSTRUCTION}). */
        String causeCode,
        /**
         * Codice GTFS-RT {@code Alert.Effect} non tradotto (es. {@code DETOUR}).
         * Da preferire alle etichette italiane per qualunque logica: sono stabili
         * e non cambiano se cambia una traduzione.
         */
        String effectCode,
        String title,
        String description,
        Instant startsAt,
        Instant endsAt,
        Instant updatedAt
) {
}
