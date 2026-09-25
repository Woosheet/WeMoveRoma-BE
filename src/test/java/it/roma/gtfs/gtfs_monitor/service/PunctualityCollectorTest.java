package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.TripUpdateDTO;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fissa le regole che decidono cosa diventa un passaggio: si registra una
 * fermata quando il mezzo passa alla successiva, mai la prima vista, mai un
 * ritardo implausibile.
 */
class PunctualityCollectorTest {

    private static final Instant T0 = Instant.parse("2026-09-16T06:00:00Z");

    private static TripUpdateDTO riga(String corsa, String fermata, Double ritardoMin) {
        return TripUpdateDTO.builder()
                .linea("23").corsa(corsa).veicolo("736").fermataId(fermata).ritardoArrivoMin(ritardoMin)
                .build();
    }

    @Test
    void laPrimaFermataVistaNonSiRegistra() {
        PunctualityCollector c = new PunctualityCollector();
        assertTrue(c.osserva(List.of(riga("T", "A", 2.0)), T0).isEmpty());
        // Il mezzo passa da A a B: A era la prima vista (capolinea o riavvio), si scarta.
        assertTrue(c.osserva(List.of(riga("T", "B", 3.0)), T0.plusSeconds(60)).isEmpty());
        // Da B a C: B vale.
        List<PunctualityCollector.Passaggio> out = c.osserva(List.of(riga("T", "C", 4.0)), T0.plusSeconds(120));
        assertEquals(1, out.size());
        assertEquals("B", out.getFirst().fermataId());
        assertEquals(3.0, out.getFirst().ritardoMin());
    }

    @Test
    void valeLUltimaPrevisionePrimaDelPassaggio() {
        PunctualityCollector c = new PunctualityCollector();
        c.osserva(List.of(riga("T", "A", 0.0)), T0);
        c.osserva(List.of(riga("T", "B", 1.0)), T0.plusSeconds(60));
        c.osserva(List.of(riga("T", "B", 2.5)), T0.plusSeconds(75));
        List<PunctualityCollector.Passaggio> out = c.osserva(List.of(riga("T", "C", 3.0)), T0.plusSeconds(90));
        assertEquals(2.5, out.getFirst().ritardoMin());
        assertEquals(T0.plusSeconds(75), out.getFirst().quando());
    }

    @Test
    void contaSoloLaPrimaRigaDiOgniCorsa() {
        PunctualityCollector c = new PunctualityCollector();
        c.osserva(List.of(riga("T", "A", 0.0), riga("T", "B", 9.0)), T0);
        c.osserva(List.of(riga("T", "B", 1.0), riga("T", "C", 9.0)), T0.plusSeconds(60));
        List<PunctualityCollector.Passaggio> out =
                c.osserva(List.of(riga("T", "C", 2.0), riga("T", "D", 9.0)), T0.plusSeconds(120));
        assertEquals(1, out.size());
        assertEquals("B", out.getFirst().fermataId());
        assertEquals(1.0, out.getFirst().ritardoMin());
    }

    /** Il caso reale della corsa 0#1035-11: la prossima fermata oscilla fra due valori. */
    @Test
    void ilFeedCheTornaIndietroNonRegistraDueVolte() {
        PunctualityCollector c = new PunctualityCollector();
        c.osserva(List.of(riga("T", "36", 0.0), riga("T", "37", 0.0)), T0);
        c.osserva(List.of(riga("T", "37", 1.0), riga("T", "38", 1.0)), T0.plusSeconds(30));
        List<PunctualityCollector.Passaggio> tutti = new java.util.ArrayList<>();
        tutti.addAll(c.osserva(List.of(riga("T", "38", 2.0), riga("T", "39", 2.0)), T0.plusSeconds(60)));
        // Indietro: 38 torna prima, 39 e' ancora fra le rimanenti. Non e' passata.
        tutti.addAll(c.osserva(List.of(riga("T", "38", 2.5), riga("T", "39", 2.5)), T0.plusSeconds(90)));
        tutti.addAll(c.osserva(List.of(riga("T", "39", 3.0), riga("T", "40", 3.0)), T0.plusSeconds(120)));
        tutti.addAll(c.osserva(List.of(riga("T", "38", 3.0), riga("T", "39", 3.0)), T0.plusSeconds(150)));
        tutti.addAll(c.osserva(List.of(riga("T", "39", 3.5), riga("T", "40", 3.5)), T0.plusSeconds(180)));
        tutti.addAll(c.osserva(List.of(riga("T", "40", 4.0)), T0.plusSeconds(210)));

        assertEquals(List.of("37", "38", "39"), tutti.stream().map(PunctualityCollector.Passaggio::fermataId).toList());
    }

    @Test
    void unaFermataAncoraFraLeRimanentiNonEPassata() {
        PunctualityCollector c = new PunctualityCollector();
        c.osserva(List.of(riga("T", "A", 0.0), riga("T", "B", 0.0), riga("T", "C", 0.0)), T0);
        c.osserva(List.of(riga("T", "B", 1.0), riga("T", "C", 1.0)), T0.plusSeconds(30));
        // Il feed salta avanti a D ma poi torna a C: C non va registrata finche' non sparisce.
        c.osserva(List.of(riga("T", "D", 5.0)), T0.plusSeconds(60)); // B e C spariscono: B vale, C non era la prima
        List<PunctualityCollector.Passaggio> out = c.osserva(List.of(riga("T", "C", 2.0), riga("T", "D", 2.0)), T0.plusSeconds(90));
        assertTrue(out.isEmpty(), "D e' ancora fra le rimanenti: non e' passata");
    }

    @Test
    void unRitardoOltreUnOraEUnAbbinamentoSbagliato() {
        PunctualityCollector c = new PunctualityCollector();
        c.osserva(List.of(riga("T", "A", 0.0)), T0);
        c.osserva(List.of(riga("T", "B", -75.0)), T0.plusSeconds(60));
        assertTrue(c.osserva(List.of(riga("T", "C", 0.0)), T0.plusSeconds(120)).isEmpty());
    }

    @Test
    void unaCorsaSparitaChiudeLUltimaFermataDopoUnMinuto() {
        PunctualityCollector c = new PunctualityCollector();
        c.osserva(List.of(riga("T", "A", 0.0)), T0);
        c.osserva(List.of(riga("T", "B", 4.0)), T0.plusSeconds(60));
        // Un giro senza la corsa non basta: il feed puo' saltare.
        assertTrue(c.osserva(List.of(), T0.plusSeconds(90)).isEmpty());
        List<PunctualityCollector.Passaggio> out = c.osserva(List.of(), T0.plusSeconds(130));
        assertEquals(1, out.size());
        assertEquals("B", out.getFirst().fermataId());
    }

    @Test
    void leCorseOsservateSommanoIPassaggiDallUltimaEstrazione() {
        PunctualityCollector c = new PunctualityCollector();
        c.osserva(List.of(riga("T", "A", 0.0)), T0);
        c.osserva(List.of(riga("T", "B", 1.0)), T0.plusSeconds(60));
        c.osserva(List.of(riga("T", "C", 2.0)), T0.plusSeconds(120));
        c.osserva(List.of(riga("T", "D", 3.0)), T0.plusSeconds(180));

        List<PunctualityCollector.CorsaOsservata> prima = c.estraiCorse();
        assertEquals(1, prima.size());
        assertEquals(2, prima.getFirst().fermateNuove()); // B e C; A scartata, D non ancora passata
        assertEquals(T0, prima.getFirst().primaVista());
        assertEquals("736", prima.getFirst().veicolo());

        assertEquals(0, c.estraiCorse().getFirst().fermateNuove());
    }
}
