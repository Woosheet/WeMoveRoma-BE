package it.roma.gtfs.gtfs_monitor.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.PrematureCloseException;
import reactor.util.retry.Retry;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;

@Configuration
public class WebClientConfig {

    @Bean
    @Primary
    public WebClient webClient(WebClient.Builder builder) {
        return builder
                .clientConnector(new ReactorClientHttpConnector(newHttpClient(Duration.ofSeconds(30), 30)))
                .codecs(c -> c.defaultCodecs().maxInMemorySize(20 * 1024 * 1024)) // 20MB
                .filter(retryOn5xxFilter())
                .filter(retryFilter())
                .build();
    }

    @Bean("staticGtfsWebClient")
    public WebClient staticGtfsWebClient(WebClient.Builder builder) {
        return builder
                .clientConnector(new ReactorClientHttpConnector(newHttpClient(Duration.ofSeconds(120), 120)))
                .codecs(c -> c.defaultCodecs().maxInMemorySize(150 * 1024 * 1024)) // 150MB
                .filter(retryOn5xxFilter())
                .filter(retryFilter())
                .build();
    }

    private static HttpClient newHttpClient(Duration responseTimeout, int ioTimeoutSeconds) {
        return HttpClient.create()
                .followRedirect(true)
                .compress(true)
                .responseTimeout(responseTimeout)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10_000)
                .doOnConnected(conn -> conn
                        .addHandlerLast(new ReadTimeoutHandler(ioTimeoutSeconds))
                        .addHandlerLast(new WriteTimeoutHandler(ioTimeoutSeconds)));
    }

    /** Se la risposta è 5xx, crea un errore per attivare il retry. */
    private ExchangeFilterFunction retryOn5xxFilter() {
        return (request, next) -> next.exchange(request)
                .flatMap(response -> {
                    if (response.statusCode().is5xxServerError()) {
                        return response.createException().flatMap(Mono::error);
                    }
                    return Mono.just(response);
                });
    }

    /** Applica retry con backoff (3 tentativi, da 500ms, max 3s, jitter) su errori di rete/5xx. */
    private ExchangeFilterFunction retryFilter() {
        Retry retrySpec = Retry.backoff(3, Duration.ofMillis(500))
                .maxBackoff(Duration.ofSeconds(3))
                .jitter(0.2)
                .filter(t ->
                        t instanceof PrematureCloseException ||
                                t instanceof IOException ||
                                t instanceof WebClientResponseException wcre && wcre.getStatusCode().is5xxServerError()
                );

        return (request, next) -> next.exchange(request).retryWhen(retrySpec);
    }
}
