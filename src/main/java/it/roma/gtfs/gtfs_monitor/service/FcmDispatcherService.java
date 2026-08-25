package it.roma.gtfs.gtfs_monitor.service;

import com.google.firebase.FirebaseApp;
import com.google.firebase.messaging.AndroidConfig;
import com.google.firebase.messaging.AndroidNotification;
import com.google.firebase.messaging.ApnsConfig;
import com.google.firebase.messaging.Aps;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.Notification;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Dispatcher minimale per push notification FCM. Spedisce a un topic globale.
 *
 * Se Firebase non e' inizializzato (vedi {@link it.roma.gtfs.gtfs_monitor.config.FirebaseConfig})
 * o se {@code fcm.enabled=false}, e' no-op: il backend resta funzionante senza FCM.
 */
@Slf4j
@Service
public class FcmDispatcherService {

    @Value("${fcm.enabled:true}")
    private boolean enabled;

    @Value("${fcm.topic-alerts:wemoveroma-alerts}")
    private String topicAlerts;

    @Value("${fcm.topic-strikes:wemoveroma-strikes}")
    private String topicStrikes;

    public String topicAlerts() {
        return topicAlerts;
    }

    public String topicStrikes() {
        return topicStrikes;
    }

    /**
     * @return true se il messaggio e' partito; false se FCM e' disabilitato o
     *         se la libreria ha sollevato un'eccezione (loggata e inghiottita).
     */
    public boolean sendToTopic(String topic, String title, String body, Map<String, String> data) {
        if (!enabled) {
            log.debug("[FCM] disabled, skip topic={} title='{}'", topic, title);
            return false;
        }
        if (FirebaseApp.getApps().isEmpty()) {
            log.debug("[FCM] FirebaseApp non inizializzato, skip topic={} title='{}'", topic, title);
            return false;
        }
        try {
            Notification notification = Notification.builder()
                    .setTitle(title)
                    .setBody(body)
                    .build();

            Message.Builder builder = Message.builder()
                    .setTopic(topic)
                    .setNotification(notification)
                    // Android: canale "alerts" + priorita' alta per drawer immediato.
                    .setAndroidConfig(AndroidConfig.builder()
                            .setPriority(AndroidConfig.Priority.HIGH)
                            .setNotification(AndroidNotification.builder()
                                    .setChannelId("alerts")
                                    .setColor("#B5121B")
                                    .build())
                            .build())
                    // iOS: alert banner + suono default.
                    .setApnsConfig(ApnsConfig.builder()
                            .setAps(Aps.builder()
                                    .setSound("default")
                                    .build())
                            .build());

            if (data != null) {
                data.forEach(builder::putData);
            }

            String id = FirebaseMessaging.getInstance().send(builder.build());
            log.info("[FCM] sent topic={} id={} title='{}'", topic, id, title);
            return true;
        } catch (Exception e) {
            log.warn("[FCM] send fallito topic={} title='{}': {}", topic, title, e.toString());
            return false;
        }
    }
}
