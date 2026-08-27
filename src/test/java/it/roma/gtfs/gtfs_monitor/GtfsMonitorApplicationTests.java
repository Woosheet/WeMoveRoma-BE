package it.roma.gtfs.gtfs_monitor;


import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Il profilo "test" (src/test/resources/application-test.properties) porta le due cose
 * che al contesto mancavano: gtfs.static-props.data-dir, definito finora solo nei profili
 * local e prod — senza, GtfsIndexService.init() falliva su Path.of(null) — e gli URL dei
 * feed neutralizzati, cosi' che il refresh di avvio non scarichi il GTFS reale ne'
 * sovrascriva i dati locali.
 */
@SpringBootTest
@ActiveProfiles("test")
class GtfsMonitorApplicationTests {

	@Test
	void contextLoads() {
	}

}
