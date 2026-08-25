package it.roma.gtfs.gtfs_monitor.config;

/**
 * La risorsa indirizzata dall'URL non esiste nel feed GTFS caricato.
 *
 * <p>Da usare solo per questo: "questa fermata / questa corsa non esiste". Una
 * risorsa che esiste ma non ha dati da mostrare adesso (fermata senza arrivi nel
 * prossimo orizzonte, corsa non programmata oggi) resta un <b>200 con lista
 * vuota</b> — sono due situazioni diverse e il client deve poterle distinguere:
 * la prima e' un id sbagliato o obsoleto, la seconda e' semplicemente notte fonda.
 *
 * <p>Viene tradotta in {@code 404} con corpo {@code ProblemDetail} da
 * {@link ApiExceptionHandler}.
 */
public class ResourceNotFoundException extends RuntimeException {

    private final String resourceType;
    private final String resourceId;

    public ResourceNotFoundException(String resourceType, String resourceId) {
        super(resourceType + " " + resourceId + " non trovata");
        this.resourceType = resourceType;
        this.resourceId = resourceId;
    }

    public String resourceType() {
        return resourceType;
    }

    public String resourceId() {
        return resourceId;
    }

    public static ResourceNotFoundException stop(String stopId) {
        return new ResourceNotFoundException("Fermata", stopId);
    }

    public static ResourceNotFoundException trip(String tripId) {
        return new ResourceNotFoundException("Corsa", tripId);
    }
}
