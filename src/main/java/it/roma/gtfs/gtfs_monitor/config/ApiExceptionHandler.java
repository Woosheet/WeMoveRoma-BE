package it.roma.gtfs.gtfs_monitor.config;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.stream.Collectors;

/**
 * Traduce gli errori di validazione in risposte 400 pulite.
 *
 * Senza questo, una violazione dei vincoli sui parametri (@DecimalMin e simili)
 * arriverebbe al client come 500 Internal Server Error: un parametro sbagliato
 * e' colpa della richiesta, non del server, e il client deve poterlo distinguere.
 */
@Slf4j
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(ConstraintViolationException.class)
    public ProblemDetail onConstraintViolation(ConstraintViolationException ex) {
        String dettaglio = ex.getConstraintViolations().stream()
                .map(ApiExceptionHandler::describe)
                .distinct()
                .collect(Collectors.joining("; "));
        log.debug("Parametri non validi: {}", dettaglio);
        return problem(dettaglio.isBlank() ? "Parametri non validi" : dettaglio);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail onIllegalArgument(IllegalArgumentException ex) {
        log.debug("Richiesta non valida: {}", ex.getMessage());
        return problem(ex.getMessage() == null ? "Richiesta non valida" : ex.getMessage());
    }

    /**
     * Risorsa inesistente -> 404.
     *
     * Prima queste richieste rispondevano 200: una fermata sconosciuta tornava un
     * segnaposto ("Fermata non trovata") e una corsa sconosciuta una lista vuota,
     * indistinguibile da una corsa vera senza fermate programmate adesso. Il client
     * non aveva modo di sapere se ripulire un preferito ormai morto o solo aspettare.
     */
    @ExceptionHandler(ResourceNotFoundException.class)
    public ProblemDetail onResourceNotFound(ResourceNotFoundException ex) {
        log.debug("Risorsa non trovata: {} {}", ex.resourceType(), ex.resourceId());
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.NOT_FOUND);
        pd.setTitle("Risorsa non trovata");
        pd.setDetail(ex.getMessage());
        pd.setProperty("resourceType", ex.resourceType());
        pd.setProperty("resourceId", ex.resourceId());
        return pd;
    }

    private static ProblemDetail problem(String detail) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        pd.setTitle("Parametri non validi");
        pd.setDetail(detail);
        return pd;
    }

    /** "lat: deve essere maggiore o uguale a -90" invece del path completo del metodo. */
    private static String describe(ConstraintViolation<?> violation) {
        String path = violation.getPropertyPath() == null ? "" : violation.getPropertyPath().toString();
        String param = path.contains(".") ? path.substring(path.lastIndexOf('.') + 1) : path;
        return (param.isBlank() ? "parametro" : param) + ": " + violation.getMessage();
    }
}
