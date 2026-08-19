package it.roma.gtfs.gtfs_monitor.service;

import java.util.Locale;
import java.util.Map;

/**
 * Ricava la severita' di un avviso quando il feed non la dichiara.
 *
 * <h3>Perche' serve</h3>
 * Il feed GTFS-RT di Roma Mobilita' non popola mai {@code severity_level}: misurato
 * il 19/08/2026, {@code severity} era {@code null} su tutti i 115 avvisi attivi. Le
 * varianti "warning" e "critical" dell'interfaccia non comparivano quindi mai, e
 * l'utente vedeva un servizio sospeso con lo stesso peso visivo di una nota
 * informativa. Il campo {@code effect}, invece, il feed lo popola sempre: e' da li'
 * che si ricava una severita' sensata.
 *
 * <h3>Onesta' del dato</h3>
 * La severita' derivata non viene spacciata per un dato del feed:
 * {@code ApiAlertDTO.declaredSeverity} riporta (e resta {@code null} se assente)
 * cio' che il feed dichiara davvero, e {@code severitySource} dice sempre se il
 * valore in {@code severity} viene da {@code FEED} o da {@code DERIVED}.
 *
 * <h3>Mappatura effetto -&gt; severita'</h3>
 * Il criterio e' l'effetto sul viaggio di chi legge, non la gravita' dell'evento
 * che l'ha causato:
 * <ul>
 *   <li><b>SEVERE</b> — la corsa non c'e' o non ferma dove serve: {@code NO_SERVICE},
 *       {@code NO_STOPS}. Chi aspetta a quella fermata deve cambiare piano.</li>
 *   <li><b>WARNING</b> — il servizio c'e' ma non come previsto: {@code DETOUR},
 *       {@code MODIFIED_SERVICE}, {@code REDUCED_SERVICE}, {@code SIGNIFICANT_DELAYS},
 *       {@code STOP_MOVED}, {@code ACCESSIBILITY_ISSUE}. Serve leggere l'avviso, ma
 *       il viaggio resta possibile.</li>
 *   <li><b>INFO</b> — nulla da cambiare: {@code ADDITIONAL_SERVICE},
 *       {@code OTHER_EFFECT}, {@code UNKNOWN_EFFECT}, effetto assente o non
 *       riconosciuto. In dubbio non si allarma.</li>
 * </ul>
 *
 * Il vocabolario in uscita e' quello di GTFS-RT ({@code SeverityLevel}: INFO /
 * WARNING / SEVERE), lo stesso che uscirebbe dal feed se lo popolasse: un domani
 * che Roma Mobilita' lo valorizzi, il contratto verso i client non cambia.
 *
 * <p>La derivazione parte dal <b>codice</b> GTFS-RT, non dalla traduzione italiana:
 * cambiare un'etichetta in {@code ServiceAlertsService.EFFECT_IT} non deve poter
 * spostare in silenzio la severita' di un avviso.
 *
 * <p><b>Ambito deliberatamente limitato all'API di lettura.</b> Il filtro delle push
 * ({@code notifications.alerts.severities=SEVERE,WARNING} in
 * {@code ServiceAlertsService}) continua a leggere la severita' <i>dichiarata</i>:
 * applicare li' quella derivata trasformerebbe 112 avvisi su 115 in candidati alla
 * notifica, cioe' spam. Un eventuale allargamento delle push va deciso a parte, con
 * criteri suoi.
 */
public final class AlertSeverityResolver {

    /** Valore emesso quando la severita' arriva davvero dal feed. */
    public static final String SOURCE_FEED = "FEED";
    /** Valore emesso quando la severita' e' calcolata dal backend a partire dall'effetto. */
    public static final String SOURCE_DERIVED = "DERIVED";

    private static final String SEVERE = "SEVERE";
    private static final String WARNING = "WARNING";
    private static final String INFO = "INFO";

    /** Codici {@code Alert.Effect} di GTFS-RT -> severita'. Chi manca ricade su INFO. */
    private static final Map<String, String> SEVERITY_BY_EFFECT_CODE = Map.ofEntries(
            Map.entry("NO_SERVICE", SEVERE),
            Map.entry("NO_STOPS", SEVERE),
            Map.entry("REDUCED_SERVICE", WARNING),
            Map.entry("SIGNIFICANT_DELAYS", WARNING),
            Map.entry("DETOUR", WARNING),
            Map.entry("MODIFIED_SERVICE", WARNING),
            Map.entry("STOP_MOVED", WARNING),
            Map.entry("ACCESSIBILITY_ISSUE", WARNING),
            Map.entry("ADDITIONAL_SERVICE", INFO),
            Map.entry("OTHER_EFFECT", INFO),
            Map.entry("UNKNOWN_EFFECT", INFO)
    );

    private AlertSeverityResolver() {
    }

    /**
     * Severita' da esporre: quella del feed se c'e', altrimenti quella derivata.
     *
     * @param declaredSeverity valore di {@code severity_level} dal feed (spesso {@code null})
     * @param effectCode       codice {@code Alert.Effect} GTFS-RT (es. {@code DETOUR})
     * @return sempre non-null: INFO / WARNING / SEVERE, o il valore del feed cosi' com'e'
     */
    public static String resolve(String declaredSeverity, String effectCode) {
        if (isDeclared(declaredSeverity)) {
            return declaredSeverity;
        }
        return deriveFromEffect(effectCode);
    }

    /** {@link #SOURCE_FEED} o {@link #SOURCE_DERIVED}, coerente con {@link #resolve}. */
    public static String source(String declaredSeverity) {
        return isDeclared(declaredSeverity) ? SOURCE_FEED : SOURCE_DERIVED;
    }

    /**
     * {@code UNKNOWN_SEVERITY} non e' un'informazione: il feed sta dicendo "non lo so".
     * Trattarlo come dichiarato lascerebbe la UI piatta esattamente nei casi in cui
     * la derivazione serve.
     */
    private static boolean isDeclared(String declaredSeverity) {
        return declaredSeverity != null
                && !declaredSeverity.isBlank()
                && !"UNKNOWN_SEVERITY".equalsIgnoreCase(declaredSeverity.trim());
    }

    private static String deriveFromEffect(String effectCode) {
        if (effectCode == null || effectCode.isBlank()) {
            return INFO;
        }
        return SEVERITY_BY_EFFECT_CODE.getOrDefault(effectCode.trim().toUpperCase(Locale.ROOT), INFO);
    }
}
