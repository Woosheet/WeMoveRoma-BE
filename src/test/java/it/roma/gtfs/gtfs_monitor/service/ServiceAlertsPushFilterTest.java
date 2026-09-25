package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.ServiceAlertDTO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chi finisce in push sul topic generale.
 *
 * I casi sono quelli veri del feed di Roma: la severita' dichiarata non arriva mai,
 * quindi il filtro deve leggere quella derivata dall'effetto, ma senza trasformare
 * ogni deviazione per lavori in una notifica per tutti.
 */
class ServiceAlertsPushFilterTest {

    private static final Set<String> SOLO_SEVERE = Set.of("SEVERE");

    private static ServiceAlertDTO avviso(String severitaDichiarata, String effettoCodice, String... linee) {
        return ServiceAlertDTO.builder()
                .id("1")
                .severita(severitaDichiarata)
                .effettoCodice(effettoCodice)
                .routeIds(List.of(linee))
                .build();
    }

    @Test
    @DisplayName("servizio sospeso: passa anche senza severita' dichiarata")
    void servizioSospesoPassa() {
        assertThat(ServiceAlertsService.daNotificare(avviso(null, "NO_SERVICE", "64"), SOLO_SEVERE, 5)).isTrue();
        assertThat(ServiceAlertsService.daNotificare(avviso(null, "NO_STOPS", "64"), SOLO_SEVERE, 5)).isTrue();
    }

    @Test
    @DisplayName("deviazione di una linea sola: non arriva a tutti")
    void deviazioneSingolaNonPassa() {
        assertThat(ServiceAlertsService.daNotificare(avviso(null, "DETOUR", "89"), SOLO_SEVERE, 5)).isFalse();
        assertThat(ServiceAlertsService.daNotificare(avviso(null, "MODIFIED_SERVICE", "05B"), SOLO_SEVERE, 5)).isFalse();
    }

    @Test
    @DisplayName("una deviazione che tocca mezza rete passa lo stesso")
    void tanteLineePassano() {
        ServiceAlertDTO esteso = avviso(null, "DETOUR", "23", "30", "62", "64", "83", "280");
        assertThat(ServiceAlertsService.daNotificare(esteso, SOLO_SEVERE, 5)).isTrue();
        // Stesso avviso con la regola disattivata: torna a valere solo la severita'.
        assertThat(ServiceAlertsService.daNotificare(esteso, SOLO_SEVERE, 0)).isFalse();
    }

    @Test
    @DisplayName("la severita' del feed, quando c'e', vince su quella derivata")
    void severitaDichiarataVince() {
        // Effetto da WARNING, ma il feed dichiara SEVERE: passa.
        assertThat(ServiceAlertsService.daNotificare(avviso("SEVERE", "DETOUR", "64"), SOLO_SEVERE, 5)).isTrue();
        // Effetto da SEVERE, ma il feed dichiara INFO: non passa.
        assertThat(ServiceAlertsService.daNotificare(avviso("INFO", "NO_SERVICE", "64"), SOLO_SEVERE, 5)).isFalse();
    }

    @Test
    @DisplayName("filtro vuoto: passa tutto, com'era prima")
    void filtroVuotoPassaTutto() {
        assertThat(ServiceAlertsService.daNotificare(avviso(null, "DETOUR", "89"), Set.of(), 5)).isTrue();
    }
}
