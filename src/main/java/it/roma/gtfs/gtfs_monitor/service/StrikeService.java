package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.StrikeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.ByteArrayInputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Polling del feed RSS scioperi MIT (https://scioperi.mit.gov.it/mit2/public/scioperi/rss).
 * Filtra per settori che impattano Roma (trasporto pubblico locale + ferroviario)
 * e dispatcha le voci nuove via FCM al topic "wemoveroma-strikes".
 *
 * Cron: due refresh al giorno (06:00 e 14:00 Europe/Rome), piu' un primo refresh
 * 60s dopo l'avvio per popolare il tracker senza spammare push retroattive.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StrikeService {

    private static final DateTimeFormatter D_MM_YYYY = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.ITALIAN);
    /** Per push: "Venerdì 12 giugno" (no anno, leggibile). */
    private static final DateTimeFormatter DAY_NAME = DateTimeFormatter.ofPattern("EEEE d MMMM", Locale.ITALIAN);
    /** Estrae intervallo orario "DALLE 9.30 ALLE 12.30" → ("9.30","12.30"). */
    private static final Pattern TIME_WINDOW = Pattern.compile(
            "DALLE\\s+(\\d{1,2}[\\.,:]?\\d{0,2})\\s+ALLE\\s+(\\d{1,2}[\\.,:]?\\d{0,2})",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern GUID_ID = Pattern.compile(".*?/(\\d+)\\s*$");

    private final WebClient webClient;
    private final FcmDispatcherService fcmDispatcher;
    private final StrikesDispatchTracker dispatchTracker;

    @Value("${strikes.mit.rss-url:https://scioperi.mit.gov.it/mit2/public/scioperi/rss}")
    private String rssUrl;

    /** Settori che ci interessano (matching case-insensitive contains). Tutti gli altri vengono ignorati. */
    @Value("${strikes.sectors:Trasporto Pubblico Locale,Ferroviario,Appalti ferroviari,Trasporto ferroviario}")
    private String sectorsCsv;

    /** Regioni rilevanti per la rete WeMoveRoma. "Italia" = scioperi nazionali. */
    @Value("${strikes.regions:Italia,Lazio}")
    private String regionsCsv;

    @Value("${strikes.dispatch-on-startup:false}")
    private boolean dispatchOnStartup;

    private final AtomicReference<List<StrikeDTO>> lastSnapshot = new AtomicReference<>(List.of());

    /** Espone snapshot a controller / REST endpoint. */
    public List<StrikeDTO> snapshot() {
        return lastSnapshot.get();
    }

    /** Refresh manuale on-demand (utile per testing / admin endpoint). */
    public List<StrikeDTO> refreshNow() {
        return doRefresh(true);
    }

    /** Primo refresh poco dopo il boot: popola il tracker senza dispatchare (anti-spam). */
    @Scheduled(initialDelay = 60_000, fixedDelay = Long.MAX_VALUE)
    public void warmup() {
        log.info("[Strikes] warmup refresh (dispatch={})", dispatchOnStartup);
        doRefresh(dispatchOnStartup);
    }

    /**
     * Refresh schedulato: cron a 06:00 e 14:00 Europe/Rome.
     * Crontab: sec min hour day month dow.
     */
    @Scheduled(cron = "0 0 6,14 * * *", zone = "Europe/Rome")
    public void scheduledRefresh() {
        doRefresh(true);
    }

    private List<StrikeDTO> doRefresh(boolean dispatch) {
        try {
            byte[] body = webClient.get().uri(rssUrl)
                    .header("User-Agent", "wemoveroma-bot/1.0")
                    .retrieve()
                    .bodyToMono(byte[].class)
                    .block();
            if (body == null || body.length == 0) {
                log.warn("[Strikes] body vuoto da {}", rssUrl);
                return lastSnapshot.get();
            }
            List<StrikeDTO> parsed = parseRss(body);
            List<StrikeDTO> filtered = filter(parsed);
            lastSnapshot.set(List.copyOf(filtered));
            log.info("[Strikes] feed {} item totali, {} rilevanti per WeMoveRoma", parsed.size(), filtered.size());

            if (dispatch) {
                dispatchNew(filtered);
            }
            return filtered;
        } catch (Exception e) {
            log.warn("[Strikes] refresh fallito: {}", e.toString());
            return lastSnapshot.get();
        }
    }

    private List<StrikeDTO> parseRss(byte[] xml) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        // Hardening minimo XXE: disabilita external entities / DTDs.
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
        dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        dbf.setNamespaceAware(false);

        Document doc = dbf.newDocumentBuilder().parse(new ByteArrayInputStream(xml));
        NodeList items = doc.getElementsByTagName("item");
        List<StrikeDTO> out = new ArrayList<>(items.getLength());
        for (int i = 0; i < items.getLength(); i++) {
            Element item = (Element) items.item(i);
            String guid = textOf(item, "guid");
            String link = textOf(item, "link");
            String title = textOf(item, "title");
            String desc = textOf(item, "description");
            try {
                out.add(buildStrike(guid, link, title, desc));
            } catch (Exception ignore) {
                // un item malformato non blocca gli altri
            }
        }
        return out;
    }

    private StrikeDTO buildStrike(String guid, String link, String title, String desc) {
        // Description e' HTML semplice con campi separati da <br/>. Normalizziamo.
        Map<String, String> fields = parseFields(desc);
        // Alcuni campi sono anche nel title (riportati): usiamo description come fonte autoritativa.
        LocalDate dataInizio = parseDate(fieldOrTitleScan(fields, "Data inizio", title));
        LocalDate dataFine = parseDate(fields.get("Data fine"));
        LocalDate dataProclamazione = parseDate(fields.get("Data proclamazione"));
        LocalDate dataRicezione = parseDate(fields.get("Data ricezione"));

        String settore = trimToNull(fields.getOrDefault("Settore", fieldFromTitle(title, "Settore:")));
        String rilevanza = trimToNull(fields.getOrDefault("Rilevanza", fieldFromTitle(title, "Rilevanza:")));
        String regione = trimToNull(fields.getOrDefault("Regione", fieldFromTitle(title, "Regione:")));
        String provincia = trimToNull(fields.getOrDefault("Provincia", fieldFromTitle(title, "Provincia:")));
        String modalita = trimToNull(fields.get("modalità"));
        String categoria = trimToNull(fields.get("Categoria interessata"));
        String sindacatiRaw = trimToNull(fields.get("Sindacati"));
        List<String> sindacati = sindacatiRaw == null ? List.of()
                : Arrays.stream(sindacatiRaw.split("/")).map(String::trim).filter(s -> !s.isEmpty()).toList();

        String id = extractGuidId(guid);
        return StrikeDTO.builder()
                .id(id)
                .dataInizio(dataInizio)
                .dataFine(dataFine)
                .settore(settore)
                .modalita(modalita)
                .rilevanza(rilevanza)
                .regione(regione)
                .provincia(provincia)
                .sindacati(sindacati)
                .categoria(categoria)
                .dataProclamazione(dataProclamazione)
                .dataRicezione(dataRicezione)
                .link(link)
                .build();
    }

    private List<StrikeDTO> filter(List<StrikeDTO> all) {
        List<String> sectors = csvLower(sectorsCsv);
        List<String> regions = csvLower(regionsCsv);
        List<StrikeDTO> out = new ArrayList<>();
        for (StrikeDTO s : all) {
            String sec = s.getSettore() == null ? "" : s.getSettore().toLowerCase(Locale.ROOT);
            String reg = s.getRegione() == null ? "" : s.getRegione().toLowerCase(Locale.ROOT).trim();
            boolean sectorMatch = sectors.isEmpty() || sectors.stream().anyMatch(sec::contains);
            boolean regionMatch = regions.isEmpty() || regions.stream().anyMatch(reg::contains);
            if (sectorMatch && regionMatch) {
                out.add(s);
            }
        }
        return out;
    }

    private void dispatchNew(List<StrikeDTO> strikes) {
        int sent = 0;
        for (StrikeDTO s : strikes) {
            if (s.getId() == null) continue;
            if (!dispatchTracker.markIfNew(s.getId())) continue;
            String title = buildTitle(s);
            String body = buildBody(s);
            Map<String, String> data = new HashMap<>();
            data.put("kind", "strike");
            data.put("strikeId", s.getId());
            if (s.getDataInizio() != null) data.put("dataInizio", s.getDataInizio().toString());
            if (s.getDataFine() != null) data.put("dataFine", s.getDataFine().toString());
            if (s.getRilevanza() != null) data.put("rilevanza", s.getRilevanza());
            if (s.getSettore() != null) data.put("settore", s.getSettore());
            data.put("deeplink", "wemoveroma://alerts?tab=strikes");
            if (fcmDispatcher.sendToTopic(fcmDispatcher.topicStrikes(), title, body, data)) {
                sent++;
            }
        }
        if (sent > 0) log.info("[Strikes] dispatched {} push.", sent);
    }

    /**
     * Titolo push: "🛑 [Sciopero bus e metro|treni|trasporti] — [giorno della settimana]".
     * Esempi reali:
     *   🛑 Sciopero bus e metro — Venerdì 12 giugno
     *   🛑 Sciopero treni — Lunedì 24 giugno
     */
    static String buildTitle(StrikeDTO s) {
        String label = shortSectorLabel(s.getSettore());
        String when;
        LocalDate d = s.getDataInizio();
        if (d == null) {
            when = "in arrivo";
        } else {
            // Capitalizza il giorno della settimana (italiano lo dà minuscolo)
            String formatted = d.format(DAY_NAME);
            when = capitalizeFirst(formatted);
            if (s.getDataFine() != null && !s.getDataFine().equals(d)) {
                when += " → " + capitalizeFirst(s.getDataFine().format(DAY_NAME));
            }
        }
        return "🛑 " + label + " — " + when;
    }

    /**
     * Body push: orari estratti dalla modalita' in formato pulito + scope geografico
     * in linguaggio naturale.
     * Esempi reali:
     *   Stop dalle 9:30 alle 12:30 nel Lazio. Tocca per i dettagli.
     *   Stop dalle 9:00 alle 13:00 - Nazionale. Tocca per i dettagli.
     */
    static String buildBody(StrikeDTO s) {
        StringBuilder sb = new StringBuilder();
        String window = extractTimeWindow(s.getModalita());
        if (window != null) {
            sb.append("Stop ").append(window).append(' ');
        } else if (s.getModalita() != null && !s.getModalita().isBlank()) {
            // Fallback: prendi la modalita' grezza in lower-sentence
            String low = s.getModalita().trim().toLowerCase(Locale.ITALIAN);
            sb.append(capitalizeFirst(low)).append(". ");
        }
        // Geographic scope umano
        String scope = humanScope(s);
        if (scope != null) sb.append(scope).append(". ");
        sb.append("Tocca per i dettagli.");
        String r = sb.toString().trim().replaceAll("\\s+", " ");
        return r.length() > 180 ? r.substring(0, 179) + "…" : r;
    }

    /** Mappa "Trasporto pubblico locale" → "Sciopero bus e metro", "Trasporto ferroviario" → "Sciopero treni". */
    private static String shortSectorLabel(String settore) {
        if (settore == null || settore.isBlank()) return "Sciopero trasporti";
        String s = settore.toLowerCase(Locale.ITALIAN);
        if (s.contains("pubblico locale")) return "Sciopero bus e metro";
        if (s.contains("ferroviario") || s.contains("treno")) return "Sciopero treni";
        return "Sciopero " + settore.toLowerCase(Locale.ITALIAN);
    }

    /** "DALLE 9.30 ALLE 12.30" → "dalle 9:30 alle 12:30". Null se non matcha. */
    private static String extractTimeWindow(String modalita) {
        if (modalita == null) return null;
        Matcher m = TIME_WINDOW.matcher(modalita);
        if (!m.find()) return null;
        return "dalle " + normalizeTime(m.group(1)) + " alle " + normalizeTime(m.group(2));
    }

    /** "9.30" / "930" / "9:30" → "9:30". "9" → "9:00". */
    private static String normalizeTime(String raw) {
        if (raw == null) return "";
        String clean = raw.replace(".", ":").replace(",", ":");
        if (!clean.contains(":")) {
            // "9" → "9:00", "930" → "9:30" (improbabile ma copro)
            if (clean.length() <= 2) return clean + ":00";
            return clean.substring(0, clean.length() - 2) + ":" + clean.substring(clean.length() - 2);
        }
        // assicura formato H:MM con minuti a 2 cifre
        String[] parts = clean.split(":", 2);
        String h = parts[0];
        String mm = parts.length > 1 ? parts[1] : "00";
        if (mm.length() == 1) mm = mm + "0";
        if (mm.isEmpty()) mm = "00";
        return h + ":" + mm;
    }

    /**
     * "Nazionale" se regione Italia; "in [Regione]" se Regionale; "Provincia di X"
     * se provincia specifica (non "Tutte"). Null se nessun campo utile.
     */
    private static String humanScope(StrikeDTO s) {
        String prov = s.getProvincia();
        if (prov != null && !prov.isBlank() && !prov.equalsIgnoreCase("Tutte")) {
            return "Provincia di " + prov;
        }
        String reg = s.getRegione();
        if (reg == null) return null;
        reg = reg.trim();
        if (reg.equalsIgnoreCase("Italia")) return "Nazionale";
        return "in " + reg;
    }

    private static String capitalizeFirst(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    // === Helpers ===
    private static Map<String, String> parseFields(String html) {
        Map<String, String> out = new HashMap<>();
        if (html == null) return out;
        // Sostituisce <br/>, <br>, <br /> con newline e splitta in righe key: value.
        String norm = html.replaceAll("(?i)<br\\s*/?>", "\n");
        for (String line : norm.split("\n")) {
            String t = line.trim();
            int colon = t.indexOf(':');
            if (colon <= 0) continue;
            String k = t.substring(0, colon).trim();
            String v = t.substring(colon + 1).trim();
            out.put(k, v);
        }
        return out;
    }

    private static String fieldOrTitleScan(Map<String, String> fields, String key, String title) {
        String v = fields.get(key);
        if (v != null) return v;
        return fieldFromTitle(title, key + ":");
    }

    private static String fieldFromTitle(String title, String label) {
        if (title == null || label == null) return null;
        int idx = title.indexOf(label);
        if (idx < 0) return null;
        int from = idx + label.length();
        int end = title.indexOf(" - ", from);
        return (end < 0 ? title.substring(from) : title.substring(from, end)).trim();
    }

    private static LocalDate parseDate(String s) {
        if (s == null || s.isBlank()) return null;
        try {
            return LocalDate.parse(s.trim(), D_MM_YYYY);
        } catch (Exception e) {
            return null;
        }
    }

    private static String textOf(Element parent, String tag) {
        NodeList nl = parent.getElementsByTagName(tag);
        if (nl.getLength() == 0) return null;
        return nl.item(0).getTextContent();
    }

    private static String extractGuidId(String guid) {
        if (guid == null) return null;
        Matcher m = GUID_ID.matcher(guid);
        return m.matches() ? m.group(1) : guid.trim();
    }

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    private static List<String> csvLower(String csv) {
        if (csv == null || csv.isBlank()) return List.of();
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s.toLowerCase(Locale.ROOT))
                .toList();
    }
}
