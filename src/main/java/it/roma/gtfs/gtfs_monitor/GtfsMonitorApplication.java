package it.roma.gtfs.gtfs_monitor;

import it.roma.gtfs.gtfs_monitor.config.GtfsProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(GtfsProperties.class)
public class GtfsMonitorApplication {

	public static void main(String[] args) {
		SpringApplication.run(GtfsMonitorApplication.class, args);
	}

}
