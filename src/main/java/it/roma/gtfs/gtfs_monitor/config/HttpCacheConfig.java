package it.roma.gtfs.gtfs_monitor.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.CacheControl;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.mvc.WebContentInterceptor;

import java.util.concurrent.TimeUnit;

/**
 * Cache-Control sulle risposte dell'API.
 *
 * Prima non ce n'era nessuno, su nessun endpoint: niente Cache-Control, niente
 * ETag, niente Last-Modified. La conseguenza e' che la proxy_cache di nginx —
 * che pure e' configurata — non memorizzava nulla, perche' e' impostata per
 * onorare il Cache-Control dell'upstream e l'upstream non ne mandava. Ogni
 * richiesta, anche identica alla precedente, arrivava fino alla JVM.
 *
 * Le durate qui sotto non sono scelte a occhio: sono legate a ogni quanto il
 * dato sottostante puo' effettivamente cambiare.
 *
 * Sta in un interceptor e non nei controller perche' i metodi restituiscono i
 * DTO direttamente, non ResponseEntity: aggiungere gli header uno per uno
 * vorrebbe dire riscrivere una trentina di firme e ricordarsene a ogni nuovo
 * endpoint. Qui la regola sta in un posto solo e si legge tutta insieme.
 *
 * Cosa NON e' cachato, di proposito: /nearby, /geocode, /planner, /journey e
 * /stops. I primi quattro dipendono dalla posizione o dalla richiesta di chi
 * chiede — una risposta pubblica condivisa sarebbe sbagliata, non solo
 * inefficiente. Gli arrivi a una fermata sono il dato piu' sensibile al tempo
 * che il prodotto mostra, e li' un secondo di ritardo si vede.
 */
@Configuration
public class HttpCacheConfig implements WebMvcConfigurer {

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        WebContentInterceptor cache = new WebContentInterceptor();

        /*
         * Catalogo: linee, percorsi e capolinea nascono dal GTFS statico, che si
         * aggiorna una volta l'ora (gtfs.static-props.refresh-millis=3600000) e
         * in pratica cambia una volta al giorno. Cinque minuti sono prudenti e
         * tolgono di mezzo la quasi totalita' delle richieste ripetute: la
         * pagina di ogni linea chiede il proprio pattern, e sono 427 pagine.
         */
        cache.addCacheMapping(
                CacheControl.maxAge(5, TimeUnit.MINUTES).cachePublic(),
                "/api/v1/catalog/**");

        /*
         * Avvisi: il feed viene riletto ogni 5 secondi, ma un avviso di servizio
         * resta valido per ore. Trenta secondi vuol dire che un avviso nuovo si
         * vede al massimo mezzo minuto dopo, e nel frattempo le 99 KB della
         * lista non vengono ricalcolate a ogni visita.
         */
        cache.addCacheMapping(
                CacheControl.maxAge(30, TimeUnit.SECONDS).cachePublic(),
                "/api/v1/alerts/**");

        /*
         * Riepilogo della dashboard: il frontend lo chiede ogni 30 secondi, ma i
         * contatori si muovono di continuo. Quindici secondi tengono il numero
         * vivo e dimezzano comunque le richieste che toccano la JVM.
         */
        cache.addCacheMapping(
                CacheControl.maxAge(15, TimeUnit.SECONDS).cachePublic(),
                "/api/v1/dashboard/**");

        /*
         * Posizioni: cinque secondi, cioe' esattamente il periodo con cui il
         * backend rilegge il feed GTFS Realtime. Entro quella finestra la
         * risposta sarebbe comunque identica, quindi non si perde freschezza —
         * si evita solo di ricalcolarla per ogni utente collegato insieme.
         */
        cache.addCacheMapping(
                CacheControl.maxAge(5, TimeUnit.SECONDS).cachePublic(),
                "/api/v1/vehicles");

        registry.addInterceptor(cache);
    }
}
