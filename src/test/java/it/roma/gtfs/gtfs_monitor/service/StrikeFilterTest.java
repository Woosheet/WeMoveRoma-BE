package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.StrikeDTO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Quali scioperi del registro del Ministero arrivano a chi si muove a Roma.
 *
 * I casi sono presi dal feed del 18/09/2026, compresi i due che il filtro
 * precedente lasciava fuori: lo sciopero plurisettoriale nazionale del 2 ottobre
 * e quello generale del 30, che risultava con Regione = Toscana pur essendo
 * dichiarato nazionale.
 */
class StrikeFilterTest {

    private static final List<String> SETTORI = List.of(
            "trasporto pubblico locale", "trasporto ferroviario", "ferroviario", "generale", "plurisettoriale");
    private static final List<String> REGIONI = List.of("italia", "lazio");

    private static StrikeDTO sciopero(String settore, String rilevanza, String regione, String categoria) {
        return StrikeDTO.builder()
                .id(categoria)
                .settore(settore)
                .rilevanza(rilevanza)
                .regione(regione)
                .categoria(categoria)
                .build();
    }

    private static List<String> passano(StrikeDTO... scioperi) {
        return StrikeService.filtra(List.of(scioperi), SETTORI, REGIONI).stream().map(StrikeDTO::getId).toList();
    }

    @Test
    @DisplayName("il trasporto pubblico del Lazio passa, quello di un'altra regione no")
    void trasportoLocale() {
        assertThat(passano(
                sciopero("Trasporto pubblico locale", "Locale", "Lazio", "ago-uno-castelli"),
                sciopero("Trasporto pubblico locale", "Regionale", "Umbria", "busitalia-umbria"),
                sciopero("Trasporto pubblico locale", "Regionale", "Abruzzo", "tua-abruzzo")))
                .containsExactly("ago-uno-castelli");
    }

    @Test
    @DisplayName("generale e plurisettoriale nazionali: sono quelli che fermano la citta'")
    void scioperiGenerali() {
        assertThat(passano(
                sciopero("Plurisettoriale", "Nazionale", "Italia", "plurisettoriale-2-ottobre"),
                // Dichiarato nazionale, ma il registro ci scrive accanto una regione.
                sciopero("Generale", "Nazionale", "Toscana", "generale-30-ottobre")))
                .containsExactly("plurisettoriale-2-ottobre", "generale-30-ottobre");
    }

    @Test
    @DisplayName("le ferrovie passano per il Lazio e per i nazionali, non per le altre regioni")
    void ferrovie() {
        assertThat(passano(
                sciopero("Ferroviario", "Nazionale", "Italia", "trenitalia-nazionale"),
                sciopero("Ferroviario", "Regionale", "Sicilia", "rfi-sicilia"),
                sciopero("Ferroviario", "Regionale", "Emilia-Romagna", "trenitalia-bologna")))
                .containsExactly("trenitalia-nazionale");
    }

    @Test
    @DisplayName("i settori che non fermano un bus restano fuori, anche se nazionali")
    void settoriNonRilevanti() {
        assertThat(passano(
                sciopero("Aereo", "Nazionale", "Italia", "enav"),
                sciopero("Marittimo", "Nazionale", "Italia", "porti"),
                sciopero("Elicotteri", "Nazionale", "Italia", "avincis"),
                sciopero("Trasporto merci", "Nazionale", "Italia", "sbb-cargo"),
                // Ditte in appalto: pulizie, ristorazione, portierato. Le corse non si fermano.
                sciopero("Appalti ferroviari", "Regionale", "Campania", "bacnet-rfi")))
                .isEmpty();
    }

    @Test
    @DisplayName("senza settore o senza regione non si inventa un criterio")
    void campiMancanti() {
        assertThat(passano(sciopero(null, null, null, "vuoto"))).isEmpty();
        assertThat(StrikeService.filtra(
                List.of(sciopero("Aereo", "Nazionale", "Italia", "enav")), List.of(), List.of()))
                .hasSize(1);
    }
}
