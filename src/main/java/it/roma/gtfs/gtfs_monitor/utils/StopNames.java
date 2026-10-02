package it.roma.gtfs.gtfs_monitor.utils;

/**
 * Nomi fermata leggibili quando il feed non ne fornisce uno.
 *
 * ATAC pubblica alcune paline vere, con coordinate e corse, il cui
 * {@code stop_name} e' il solo carattere "_". Il formato del segnaposto e' un
 * contratto con i client: l'app lo riconosce per non ripetere il codice della
 * palina sotto il titolo.
 */
public final class StopNames {
    private StopNames() {}

    /** Vero se il nome non contiene ne' lettere ne' cifre: null, "", "_", " - ". */
    public static boolean isMissing(String name) {
        return name == null || name.codePoints().noneMatch(Character::isLetterOrDigit);
    }

    /** "Fermata 30994": dichiara che il nome manca, senza inventarne uno. */
    public static String placeholder(String code, String id) {
        String ref = code != null && !code.isBlank() ? code.trim() : id;
        return ref == null || ref.isBlank() ? "Fermata" : "Fermata " + ref.trim();
    }
}
