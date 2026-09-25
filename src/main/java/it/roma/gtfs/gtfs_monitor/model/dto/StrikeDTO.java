package it.roma.gtfs.gtfs_monitor.model.dto;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDate;
import java.util.List;

/**
 * Singolo sciopero pubblicato dal MIT (https://scioperi.mit.gov.it).
 * Parser estrae i campi dal feed RSS (titolo + description in CDATA).
 */
@Data
@Builder
public class StrikeDTO {
    /** Id univoco MIT estratto dal <guid> (es. 8356). */
    private String id;
    private LocalDate dataInizio;
    private LocalDate dataFine;
    private String settore;
    private String modalita;
    private String rilevanza;       // Nazionale | Regionale | Locale | ...
    private String regione;
    private String provincia;
    private List<String> sindacati;
    private String categoria;
    private LocalDate dataProclamazione;
    private LocalDate dataRicezione;
    /**
     * Pagina del registro, cosi' come la dichiara il feed.
     *
     * E' la home di scioperi.mit.gov.it, uguale per tutti: una pagina di
     * dettaglio per singolo sciopero non esiste. L'indirizzo che si potrebbe
     * ricavare dal guid (.../8553) risponde 404 — verificato il 18/09/2026.
     */
    private String link;
}
