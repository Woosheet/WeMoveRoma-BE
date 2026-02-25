package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.SearchSuggestionDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class SearchService {

    private static final int DEFAULT_LIMIT = 10;
    private static final int MAX_LIMIT = 20;

    private final GtfsIndexService gtfsIndexService;

    public List<SearchSuggestionDTO> suggestions(String q, Integer limit) {
        String nq = normalize(q);
        if (nq == null) {
            return List.of();
        }

        int max = normalizeLimit(limit);
        Map<String, SearchSuggestionDTO> dedup = new LinkedHashMap<>();

        Set<String> routeIds = gtfsIndexService.routeIds();
        for (String routeId : routeIds) {
            String publicLine = gtfsIndexService.publicLineByRouteId(routeId);
            int lineScore = scoreLine(publicLine, nq);
            if (lineScore > 0) {
                dedup.putIfAbsent(
                        "line|" + publicLine,
                        new SearchSuggestionDTO("line", publicLine, publicLine, lineScore, publicLine, null)
                );
            }

            for (String destination : gtfsIndexService.destinationsByRoute(routeId)) {
                int rdScore = scoreRouteDestination(publicLine, destination, nq);
                if (rdScore <= 0) continue;

                String value = publicLine + "|" + destination;
                dedup.putIfAbsent(
                        "rd|" + value,
                        new SearchSuggestionDTO(
                                "route_destination",
                                value,
                                publicLine + " " + destination,
                                rdScore,
                                publicLine,
                                destination
                        )
                );
            }
        }

        List<SearchSuggestionDTO> out = new ArrayList<>(dedup.values());
        out.sort(Comparator
                .comparingInt(SearchSuggestionDTO::score).reversed()
                .thenComparing((SearchSuggestionDTO s) -> typeOrder(s.type()))
                .thenComparing(SearchSuggestionDTO::label, String.CASE_INSENSITIVE_ORDER));

        if (out.size() > max) {
            return List.copyOf(out.subList(0, max));
        }
        return List.copyOf(out);
    }

    private static int scoreLine(String routeId, String nq) {
        String nLine = normalize(routeId);
        if (nLine == null) return 0;
        if (nLine.equals(nq)) return 100;
        if (nLine.startsWith(nq)) return 85;
        if (nLine.contains(nq)) return 65;
        if (allQueryTokensMatch(nLine, nq)) return 60;
        return 0;
    }

    private static int scoreRouteDestination(String routeId, String destination, String nq) {
        String nLine = normalize(routeId);
        String nDest = normalize(destination);
        String nLabel = normalize(routeId + " " + destination);
        if (nLabel == null) return 0;

        if (nLabel.equals(nq)) return 95;
        if (nLine != null && nLine.equals(nq)) return 92;
        if (nLabel.startsWith(nq)) return 90;
        if (nDest != null && nDest.startsWith(nq)) return 80;
        if (nLabel.contains(nq)) return 72;
        if (allQueryTokensMatch(nLabel, nq)) return 70;
        if (nDest != null && allQueryTokensMatch((nLine == null ? "" : nLine + " ") + nDest, nq)) return 68;
        return 0;
    }

    private static boolean allQueryTokensMatch(String label, String query) {
        if (label == null || query == null) return false;
        String[] queryTokens = query.split(" ");
        String[] labelTokens = label.split(" ");

        for (String q : queryTokens) {
            if (q.isBlank()) continue;
            boolean matched = false;
            for (String lt : labelTokens) {
                if (lt.equals(q) || lt.startsWith(q)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) return false;
        }
        return true;
    }

    private static int typeOrder(String type) {
        return "line".equals(type) ? 0 : 1;
    }

    private static int normalizeLimit(Integer limit) {
        if (limit == null || limit <= 0) return DEFAULT_LIMIT;
        return Math.min(limit, MAX_LIMIT);
    }

    private static String normalize(String value) {
        if (value == null) return null;
        String out = Normalizer.normalize(value, Normalizer.Form.NFD)
                .replaceAll("\\p{M}+", "")
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^\\p{Alnum}]+", " ")
                .trim()
                .replaceAll("\\s+", " ");
        return out.isEmpty() ? null : out;
    }
}
