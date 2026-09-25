package it.roma.gtfs.gtfs_monitor.service;

import it.roma.gtfs.gtfs_monitor.model.dto.ApiLinePunctualityDTO;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Il riepilogo somma bene: il totale e' la somma dei tipi, ogni giorno finisce nel tipo giusto. */
class PunctualityServiceComponiTest {

    private static final LocalDate MARTEDI = LocalDate.of(2026, 12, 1);
    private static final LocalDate FESTIVO_DI_MARTEDI = LocalDate.of(2026, 12, 8);
    private static final LocalDate SABATO = LocalDate.of(2026, 12, 5);

    @Test
    void totaleEDivisionePerTipo() {
        List<PunctualityService.RigaOraria> righe = List.of(
                new PunctualityService.RigaOraria(MARTEDI, 8, 10, 1, 6, 2, 1),
                new PunctualityService.RigaOraria(FESTIVO_DI_MARTEDI, 8, 4, 2, 2, 0, 0),
                new PunctualityService.RigaOraria(SABATO, 8, 5, 0, 5, 0, 0),
                new PunctualityService.RigaOraria(SABATO, 9, 3, 1, 1, 1, 0));

        ApiLinePunctualityDTO r = PunctualityService.componi("23", MARTEDI, FESTIVO_DI_MARTEDI, righe, Instant.EPOCH);

        assertEquals(3, r.days());
        assertEquals(22, r.samples());
        assertEquals(List.of(
                new ApiLinePunctualityDTO.Hour(8, 19, 3, 13, 2, 1),
                new ApiLinePunctualityDTO.Hour(9, 3, 1, 1, 1, 0)), r.hours());

        assertEquals(List.of("feriale", "sabato", "festivo"),
                r.dayTypes().stream().map(ApiLinePunctualityDTO.DayType::dayType).toList());
        Map<String, ApiLinePunctualityDTO.DayType> tipi = r.dayTypes().stream()
                .collect(Collectors.toMap(ApiLinePunctualityDTO.DayType::dayType, t -> t));
        assertEquals(10, tipi.get("feriale").samples());
        assertEquals(1, tipi.get("feriale").days());
        assertEquals(8, tipi.get("sabato").samples());
        assertEquals(2, tipi.get("sabato").hours().size());
        assertEquals(4, tipi.get("festivo").samples());   // l'8 dicembre, pur essendo martedi'
        assertEquals(r.samples(), r.dayTypes().stream().mapToInt(ApiLinePunctualityDTO.DayType::samples).sum());
    }
}
