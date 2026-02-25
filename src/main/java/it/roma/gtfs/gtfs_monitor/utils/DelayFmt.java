package it.roma.gtfs.gtfs_monitor.utils;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public final class DelayFmt {
    private DelayFmt() {}
    private static final ZoneId ROME = ZoneId.of("Europe/Rome");
    private static final DateTimeFormatter ISO_ROME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withZone(ROME);


    public static String toRomeIso(Long epochSeconds) {
        return (epochSeconds == null) ? null : ISO_ROME.format(Instant.ofEpochSecond(epochSeconds));
    }

    // arrotonda a 1 decimale usando BigDecimal; ritorna null se input null
    public static Double round1(Double minutes) {
        if (minutes == null) return null;
        return BigDecimal.valueOf(minutes).setScale(1, RoundingMode.HALF_UP).doubleValue();
    }

    // minutes: positivo=ritardo, negativo=anticipo, null=nessuna info
    public static String formatDelay(Double minutes) {
        if (minutes == null) return null;
        if (minutes == 0.0) return "in orario";
        if (minutes > 0)    return "in ritardo di " + minutes + " min";
        return "in anticipo di " + Math.abs(minutes) + " min";
    }

    /** Variante “breve”: "+3.5 min", "-2 min", "0 min", "-" se null. */
    public static String formatDelayShort(BigDecimal minutes) {
        if (minutes == null) return "-";
        BigDecimal v = minutes.setScale(1, RoundingMode.HALF_UP).stripTrailingZeros();
        String s = v.signum() > 0 ? "+" + toStr(v) : v.toPlainString();
        return s + " min";
    }

    private static String toStr(BigDecimal v) {
        // Evita notazioni scientifiche e rimuove zeri inutili
        String s = v.stripTrailingZeros().toPlainString();
        // Se vuoi la virgola italiana:
        // return s.replace('.', ',');
        return s;
    }
}