package it.roma.gtfs.gtfs_monitor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Value("${app.cors.allowed-origins:http://localhost:4200,http://127.0.0.1:4200,http://localhost:4203,http://127.0.0.1:4203,http://localhost:5173,http://127.0.0.1:5173,http://192.168.1.62:4203}")
    private List<String> allowedOrigins;

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins(allowedOrigins.toArray(String[]::new))
                .allowedMethods("GET", "OPTIONS")
                .allowedHeaders("Content-Type", "Authorization")
                .maxAge(600);
    }
}
