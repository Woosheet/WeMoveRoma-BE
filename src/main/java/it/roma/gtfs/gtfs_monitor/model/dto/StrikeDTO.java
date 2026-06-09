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
    /** URL canonico al dettaglio sciopero (http://scioperi.mit.gov.it/NNNN). */
    private String link;
}
