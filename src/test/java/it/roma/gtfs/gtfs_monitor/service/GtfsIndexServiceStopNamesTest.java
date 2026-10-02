package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import it.roma.gtfs.gtfs_monitor.model.dto.ApiStopSearchItemDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Paline che nel feed ATAC hanno {@code stop_name = "_"}.
 *
 * Le righe vengono dal feed del 2026-10-01: la 31009 sta a 1 m dalla 82936 e ne
 * prende il nome, la 30994 ha la fermata con nome piu' vicina a 38 m e il nome
 * non si indovina. Solo la 71986 e' spostata: nel feed sta a 39 m dalla 71967,
 * qui le e' sovrapposta per provare che una "NON ATTIVA" non presta il nome.
 */
class GtfsIndexServiceStopNamesTest {

    @TempDir
    Path dataDir;

    private GtfsIndexService service;
    private NearbyService nearby;

    @BeforeEach
    void setUp() throws IOException {
        Files.writeString(dataDir.resolve("stops.txt"), """
                stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,wheelchair_boarding,stop_timezone,location_type,parent_station
                82936,82936,"VALLE AURELIA (MA-FL3)",,41.903400,12.443527,,,,,
                31009,31009,"_",,41.903404,12.443511,,,,,
                77838,77838,"GIUSTINIANO IMPERATORE/TITO",,41.855022,12.487672,,,,,
                30994,30994,"_",,41.855244,12.487323,,,,,
                71986,71986,"TIBURTINA/CASALE CAVALLARI/NON ATTIVA",,41.932564,12.594690,,,,,
                71967,71967,"_",,41.932564,12.594687,,,,,
                70509,70509,"ZAMA",,41.876125,12.510183,,,,,
                """);

        GtfsProperties props = new GtfsProperties(
                new GtfsProperties.StaticProps(null, dataDir.toString(), 0L),
                null
        );
        service = new GtfsIndexService(props);
        service.init();
        service.rebuildIndexes();
        // Elenco e ricerca usano solo l'indice statico: i servizi realtime non servono.
        nearby = new NearbyService(service, null, null);
    }

    @Test
    @DisplayName("nessuna fermata esce dall'indice con un nome senza lettere ne' cifre")
    void noStopKeepsTheUnderscore() {
        for (GtfsIndexService.Stop stop : service.allStops()) {
            assertTrue(stop.name().codePoints().anyMatch(Character::isLetterOrDigit), stop.id());
        }
    }

    @Test
    @DisplayName("una palina sovrapposta a una con nome ne prende il nome")
    void samePoleBorrowsTheName() {
        GtfsIndexService.Stop stop = service.stopByIdOrNull("31009");
        assertEquals("VALLE AURELIA (MA-FL3)", stop.name());
        assertFalse(stop.placeholderName());
    }

    @Test
    @DisplayName("una fermata solo vicina non presta il nome: resta Fermata + codice")
    void nearbyStopDoesNotLendItsName() {
        GtfsIndexService.Stop stop = service.stopByIdOrNull("30994");
        assertEquals("Fermata 30994", stop.name());
        assertTrue(stop.placeholderName());
    }

    @Test
    @DisplayName("una fermata NON ATTIVA non presta il nome nemmeno se sovrapposta")
    void inactiveStopDoesNotLendItsName() {
        assertEquals("Fermata 71967", service.stopByIdOrNull("71967").name());
    }

    @Test
    @DisplayName("nell'elenco le fermate senza nome stanno dopo tutte quelle con nome")
    void placeholdersAreListedLast() {
        List<String> names = nearby.listStops(null, null, null, null, null).stream()
                .map(ApiStopSearchItemDTO::stopName)
                .toList();
        assertEquals(List.of(
                "GIUSTINIANO IMPERATORE/TITO",
                "TIBURTINA/CASALE CAVALLARI/NON ATTIVA",
                "VALLE AURELIA (MA-FL3)",
                "VALLE AURELIA (MA-FL3)",
                "ZAMA",
                "Fermata 30994",
                "Fermata 71967"), names);
    }

    @Test
    @DisplayName("il segnaposto non e' un nome: si trova per codice, non cercando 'fermata'")
    void placeholderIsNotSearchableAsName() {
        assertTrue(nearby.searchStops("fermata", 20).isEmpty());
        assertEquals("Fermata 30994", nearby.searchStops("30994", 20).getFirst().stopName());
    }
}
