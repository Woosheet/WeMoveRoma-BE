package it.roma.gtfs.gtfs_monitor.model.dto;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.List;

@Value
@Builder
public class ServiceAlertDTO {
    String id;                 // entity id (se presente)
    String titolo;             // header_text
    String descrizione;        // description_text
    Instant inizio;            // active_period.start (UTC)
    Instant fine;              // active_period.end   (UTC) - può essere null
    String severita;           // opzionale (non sempre popolato dai feed)
    String causa;              // CAUSE enum -> etichetta italiana
    String effetto;            // EFFECT enum -> etichetta italiana
    // Codici GTFS-RT non tradotti. Le etichette italiane servono a chi legge, i codici
    // a chi ragiona: la severita' si deriva da qui (vedi AlertSeverityResolver), cosi'
    // ritoccare una traduzione non sposta in silenzio la gravita' di un avviso.
    String causaCodice;        // CAUSE enum, es. CONSTRUCTION
    String effettoCodice;      // EFFECT enum, es. DETOUR
    List<String> routeIds;
    List<String> tripIds;
    List<String> stopIds;
}