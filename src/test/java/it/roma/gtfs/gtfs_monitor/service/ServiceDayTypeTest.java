package it.roma.gtfs.gtfs_monitor.service;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ServiceDayTypeTest {

    @Test
    void pasqua() {
        assertEquals(LocalDate.of(2026, 4, 5), ServiceDayType.pasqua(2026));
        assertEquals(LocalDate.of(2027, 3, 28), ServiceDayType.pasqua(2027));
        assertEquals(LocalDate.of(2025, 4, 20), ServiceDayType.pasqua(2025));
    }

    @Test
    void domenicheFestivitaEPasquetta() {
        assertEquals("festivo", ServiceDayType.di(LocalDate.of(2026, 12, 8)));   // martedi'
        assertEquals("festivo", ServiceDayType.di(LocalDate.of(2026, 4, 6)));    // Pasquetta
        assertEquals("festivo", ServiceDayType.di(LocalDate.of(2026, 4, 25)));   // sabato festivo
        assertEquals("festivo", ServiceDayType.di(LocalDate.of(2026, 9, 20)));   // domenica
    }

    /** I giorni della settimana che la classificazione dal GTFS aveva sbagliato. */
    @Test
    void settimanaNormale() {
        assertEquals("feriale", ServiceDayType.di(LocalDate.of(2026, 9, 17)));   // giovedi'
        assertEquals("feriale", ServiceDayType.di(LocalDate.of(2026, 9, 18)));   // venerdi'
        assertEquals("sabato", ServiceDayType.di(LocalDate.of(2026, 9, 19)));
        assertEquals("feriale", ServiceDayType.di(LocalDate.of(2026, 6, 29)));   // non verificato: vedi la classe
    }
}
