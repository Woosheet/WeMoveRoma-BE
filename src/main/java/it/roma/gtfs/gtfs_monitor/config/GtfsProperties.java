package it.roma.gtfs.gtfs_monitor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gtfs")
public record GtfsProperties(
        StaticProps staticProps,
        RealtimeProps realtime
) {
    public record StaticProps(String url, String dataDir, long refreshMillis) {}
    public record RealtimeProps(String tripUpdatesUrl, String vehiclePositionsUrl, String serviceAlertsUrl, long refreshMillis) {}
}
