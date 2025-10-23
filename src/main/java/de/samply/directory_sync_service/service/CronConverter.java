package de.samply.directory_sync_service.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

public final class CronConverter {
    private static final Logger logger = LogManager.getLogger(CronConverter.class);

    /**
     * Converts a 5-field UNIX cron to a 6-field Quartz cron (no year).
     * If both DOM and DOW are restricted (the UNIX "OR" case), it logs a warning
     * and defaults to using the DOW value.
     */
    public static String unixToQuartz(String unix) {
        String[] f = unix.trim().split("\\s+");
        if (f.length != 5) throw new IllegalArgumentException("Expected 5 fields");

        String min   = f[0];
        String hour  = f[1];
        String dom   = f[2];
        String month = f[3];
        String dow   = f[4];

        boolean domRestricted = !"*".equals(dom);
        boolean dowRestricted = !"*".equals(dow);

        // Handle conflict: both restricted
        if (domRestricted && dowRestricted) {
            logger.warn("UNIX cron uses both day-of-month and day-of-week fields. "
                    + "Quartz cannot do OR semantics. Defaulting to use day-of-week (" + dow + ") only.");
            domRestricted = false; // force day-of-month to wildcard
        }

        dow = mapUnixDowToQuartz(dow);

        String quartzDom = domRestricted ? dom : "?";
        String quartzDow = dowRestricted ? dow : "*";

        // Prepend seconds = 0
        return String.join(" ", Arrays.asList(
                "0", min, hour, quartzDom, month, quartzDow
        ));
    }

    private static String mapUnixDowToQuartz(String dow) {
        if ("*".equals(dow)) return "*";

        String[] parts = dow.split(",");
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i].trim();

            // Map numeric 0/7 → 1 (Sunday)
            if (p.matches("\\d")) {
                int n = Integer.parseInt(p);
                if (n == 0 || n == 7) n = 1;
                parts[i] = String.valueOf(n);
            } else if (p.matches("\\d-\\d")) {
                String[] r = p.split("-");
                int a = Integer.parseInt(r[0]);
                int b = Integer.parseInt(r[1]);
                if (a == 0 || a == 7) a = 1;
                if (b == 0 || b == 7) b = 1;
                parts[i] = a + "-" + b;
            }
        }
        return String.join(",", parts);
    }
}
