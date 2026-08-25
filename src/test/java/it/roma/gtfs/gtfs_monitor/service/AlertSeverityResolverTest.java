package it.roma.gtfs.gtfs_monitor.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Fissa la mappatura effetto -> severita' e, soprattutto, la regola che conta:
 * cio' che il feed dichiara vince sempre su cio' che il backend deduce.
 */
class AlertSeverityResolverTest {

    @Test
    void laSeverityDichiaratadalFeedVinceSullaDerivazione() {
        // Il feed dice INFO su una soppressione: strano, ma e' il feed a parlare.
        assertEquals("INFO", AlertSeverityResolver.resolve("INFO", "NO_SERVICE"));
        assertEquals("FEED", AlertSeverityResolver.source("INFO"));
    }

    @Test
    void unknownSeverityNonEUnaDichiarazione() {
        // "non lo so" non e' un'informazione: si deriva, altrimenti la UI resta piatta
        // proprio nei casi in cui la derivazione serve.
        assertEquals("WARNING", AlertSeverityResolver.resolve("UNKNOWN_SEVERITY", "DETOUR"));
        assertEquals("DERIVED", AlertSeverityResolver.source("UNKNOWN_SEVERITY"));
    }

    @Test
    void senzaSeverityLaCorsaCheNonCEDiventaSevere() {
        assertEquals("SEVERE", AlertSeverityResolver.resolve(null, "NO_SERVICE"));
        assertEquals("SEVERE", AlertSeverityResolver.resolve(null, "NO_STOPS"));
    }

    @Test
    void senzaSeverityIlServizioAlteratoDiventaWarning() {
        assertEquals("WARNING", AlertSeverityResolver.resolve(null, "DETOUR"));
        assertEquals("WARNING", AlertSeverityResolver.resolve(null, "MODIFIED_SERVICE"));
        assertEquals("WARNING", AlertSeverityResolver.resolve(null, "REDUCED_SERVICE"));
        assertEquals("WARNING", AlertSeverityResolver.resolve(null, "SIGNIFICANT_DELAYS"));
        assertEquals("WARNING", AlertSeverityResolver.resolve(null, "STOP_MOVED"));
        assertEquals("WARNING", AlertSeverityResolver.resolve(null, "ACCESSIBILITY_ISSUE"));
    }

    @Test
    void nelDubbioNonSiAllarma() {
        assertEquals("INFO", AlertSeverityResolver.resolve(null, "ADDITIONAL_SERVICE"));
        assertEquals("INFO", AlertSeverityResolver.resolve(null, "OTHER_EFFECT"));
        assertEquals("INFO", AlertSeverityResolver.resolve(null, "UNKNOWN_EFFECT"));
        assertEquals("INFO", AlertSeverityResolver.resolve(null, "EFFETTO_MAI_VISTO"));
        assertEquals("INFO", AlertSeverityResolver.resolve(null, null));
        assertEquals("INFO", AlertSeverityResolver.resolve(null, "   "));
    }

    @Test
    void siDerivaDalCodiceNonDallEtichettaItaliana() {
        // "Deviazione" e' cio' che l'utente legge, non un codice: passarla qui non
        // deve produrre un WARNING per caso.
        assertEquals("INFO", AlertSeverityResolver.resolve(null, "Deviazione"));
    }
}
