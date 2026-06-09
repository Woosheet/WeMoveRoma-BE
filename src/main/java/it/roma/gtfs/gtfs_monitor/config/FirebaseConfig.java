package it.roma.gtfs.gtfs_monitor.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Inizializza Firebase Admin SDK una sola volta a startup, se le credenziali
 * sono disponibili. Cerca in ordine:
 *   1) property `fcm.credentials-path` (assoluto)
 *   2) env var `GOOGLE_APPLICATION_CREDENTIALS` (gestita dalla libreria stessa)
 *
 * Se nessuna delle due e' presente, NON solleva eccezioni: il dispatcher sara'
 * disattivato a runtime (vedi {@link it.roma.gtfs.gtfs_monitor.service.FcmDispatcherService}).
 * Cosi' il backend gira anche in locale senza service account.
 */
@Slf4j
@Configuration
public class FirebaseConfig {

    @Value("${fcm.credentials-path:}")
    private String credentialsPath;

    @PostConstruct
    public void initFirebase() {
        if (!FirebaseApp.getApps().isEmpty()) {
            log.info("[FCM] FirebaseApp gia' inizializzato, skip.");
            return;
        }
        try {
            GoogleCredentials credentials = loadCredentials();
            if (credentials == null) {
                log.info("[FCM] Nessuna credenziale (fcm.credentials-path / GOOGLE_APPLICATION_CREDENTIALS). FCM disattivato.");
                return;
            }
            FirebaseOptions options = FirebaseOptions.builder()
                    .setCredentials(credentials)
                    .build();
            FirebaseApp.initializeApp(options);
            log.info("[FCM] FirebaseApp inizializzato.");
        } catch (Exception e) {
            log.warn("[FCM] Inizializzazione FirebaseApp fallita: {}. Dispatching disattivato.", e.toString());
        }
    }

    private GoogleCredentials loadCredentials() throws Exception {
        if (credentialsPath != null && !credentialsPath.isBlank()) {
            Path p = Path.of(credentialsPath);
            if (Files.isRegularFile(p)) {
                try (InputStream in = new FileInputStream(p.toFile())) {
                    return GoogleCredentials.fromStream(in);
                }
            }
            log.warn("[FCM] fcm.credentials-path={} non trovato; provo GOOGLE_APPLICATION_CREDENTIALS.", credentialsPath);
        }
        // ApplicationDefault si appoggia a GOOGLE_APPLICATION_CREDENTIALS / metadata server.
        try {
            return GoogleCredentials.getApplicationDefault();
        } catch (Exception e) {
            return null;
        }
    }
}
