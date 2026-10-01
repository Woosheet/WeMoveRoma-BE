package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * La strada fra due fermate per la mappa dei tratti lenti: si prende da una
 * corsa che passa da entrambe, agganciando le fermate al tracciato.
 */
class GtfsIndexServiceShapeBetweenStopsTest {

    private static final LocalDate GIORNO = LocalDate.of(2026, 7, 21);

    @TempDir
    Path dataDir;

    private GtfsIndexService service;

    @BeforeEach
    void setUp() throws IOException {
        // Un tracciato che va dritto verso est e poi piega a nord: le fermate S1,
        // S2, S3 sono a pochi metri dai vertici 0, 2 e 4. S9 e' a 1 km.
        write("stops.txt", """
                stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,stop_url,wheelchair_boarding,stop_timezone,location_type,parent_station
                S1,1,Prima,,41.90000,12.50000,,1,,0,
                S2,2,Seconda,,41.90001,12.50200,,1,,0,
                S3,3,Terza,,41.90200,12.50300,,1,,0,
                S9,9,Lontana,,41.91000,12.50000,,1,,0,
                """);
        write("routes.txt", """
                route_id,route_short_name,route_long_name
                R1,64,Linea 64
                """);
        write("trips.txt", """
                route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible,exceptional
                R1,SVC1,T1,Termini,,0,,SH1,1,0
                R1,SVC1,T2,Termini,,0,,SH1,1,0
                """);
        write("calendar_dates.txt", """
                service_id,date,exception_type
                SVC1,20260721,1
                """);
        write("stop_times.txt", """
                trip_id,arrival_time,departure_time,stop_id,stop_sequence
                T1,08:00:00,08:00:00,S1,1
                T1,08:02:00,08:02:00,S2,2
                T1,08:05:00,08:05:00,S3,3
                T2,09:00:00,09:00:00,S1,1
                T2,09:03:00,09:03:00,S9,2
                """);
        write("shapes.txt", """
                shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence
                SH1,41.90000,12.50000,1
                SH1,41.90000,12.50100,2
                SH1,41.90000,12.50200,3
                SH1,41.90100,12.50300,4
                SH1,41.90200,12.50300,5
                """);

        service = new GtfsIndexService(new GtfsProperties(
                new GtfsProperties.StaticProps(null, dataDir.toString(), 0L), null));
        service.init();
        service.rebuildIndexes();
    }

    @Test
    void seguiIlTracciatoFraLeDueFermate() {
        List<double[]> strada = service.shapeBetweenStops("S2", "S3", GIORNO);

        // S2, i vertici dal 3° al 5° (la curva c'e', non e' una linea dritta), e
        // S3, che coincide con il 5° e non si ripete.
        assertEquals(4, strada.size());
        assertArrayEquals(new double[]{12.502, 41.90001}, strada.getFirst(), 1e-9);
        assertArrayEquals(new double[]{12.502, 41.9}, strada.get(1), 1e-9);
        assertArrayEquals(new double[]{12.503, 41.901}, strada.get(2), 1e-9);
        assertArrayEquals(new double[]{12.503, 41.902}, strada.getLast(), 1e-9);
    }

    @Test
    void nienteStradaSeNessunaCorsaVaInQuellOrdine() {
        assertTrue(service.shapeBetweenStops("S3", "S1", GIORNO).isEmpty());
    }

    @Test
    void nienteStradaSeLaFermataELontanaDalTracciato() {
        // T2 va da S1 a S9, ma S9 e' a un chilometro dal suo tracciato.
        assertTrue(service.shapeBetweenStops("S1", "S9", GIORNO).isEmpty());
    }

    @Test
    void nienteStradaPerFermateCheNonEsistono() {
        assertTrue(service.shapeBetweenStops("S1", "NESSUNA", GIORNO).isEmpty());
    }

    private void write(String nome, String contenuto) throws IOException {
        Files.writeString(dataDir.resolve(nome), contenuto);
    }
}
