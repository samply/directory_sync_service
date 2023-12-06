package de.samply.directory_sync_service;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Parameters and functions used throughout the code.
 */
public class Utils {
    /**
     * Get a printable stack trace from an Exception object.
     * @param e
     * @return
     */
    public static String traceFromException(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}
