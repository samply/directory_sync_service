package de.samply.directory_sync_service.converter;

import java.util.Locale;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Utility class for normalizing ICD-10 (WHO) diagnosis codes.
 *
 * <p>This normalizer attempts to convert possibly malformed input
 * into a valid ICD-10 (WHO) code. If this is not possible, the
 * fallback code {@code R69} ("Illness, unspecified") is returned.</p>
 */
public class Icd10WhoNormalizer {
    private static final Logger logger = Logger.getLogger(Icd10WhoNormalizer.class.getName());

    /**
     * Regular expression for valid ICD-10 (WHO) codes.
     */
    private static final Pattern ICD10_WHO_PATTERN = Pattern.compile("^[A-Z]\\d{2}(?:\\.\\d{1,2})?$");

    private static final String UNKNOWN_CODE = "R69";

    private Icd10WhoNormalizer() {
        // Utility class; prevent instantiation
    }

    /**
     * Normalizes a possibly incorrect ICD-10 code into a valid ICD-10 (WHO) code.
     *
     * <p>If no valid ICD-10 (WHO) code can be derived, {@code R69}
     * ("Illness, unspecified") is returned and a warning is logged.</p>
     *
     * @param raw the raw input string (may be null or malformed)
     * @return a normalized ICD-10 (WHO) code, or {@code R69} if normalization fails
     */
    public static String normalize(String raw) {
        if (raw == null) {
            logFallback(null, "input was null");
            return UNKNOWN_CODE;
        }

        // If the code contains no numeric characters, it is definitely not an ICD10 code
        if (!raw.matches(".*\\d.*")) {
            logFallback(null, "input contains no numeric characters");
            return UNKNOWN_CODE;
        }

        // Trim, uppercase, normalize separators
        String s = raw.trim()
                .toUpperCase(Locale.ROOT)
                .replace(',', '.')
                .replace('-', '.')
                .replaceAll("\\s+", "");

        // Remove illegal characters
        s = s.replaceAll("[^A-Z0-9.]", "");

        // Find first letter
        int idx = -1;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                idx = i;
                break;
            }
        }
        if (idx == -1) {
            logFallback(raw, "no leading alphabetic character found");
            return UNKNOWN_CODE;
        }
        s = s.substring(idx);

        // Extract category (letter + 2 digits)
        if (s.length() < 3) {
            logFallback(raw, "too short to contain letter + two digits");
            return UNKNOWN_CODE;
        }

        char letter = s.charAt(0);
        String digits = s.substring(1).replaceAll("\\D", "");
        if (digits.length() < 2) {
            logFallback(raw, "fewer than two digits after leading letter");
            return UNKNOWN_CODE;
        }

        String category = "" + letter + digits.substring(0, 2);

        // Extract numeric decimal part (WHO: digits only, max 2)
        String decimal = "";
        int dot = s.indexOf('.');
        if (dot >= 0) {
            String afterDot = s.substring(dot + 1).replaceAll("\\D", "");
            if (!afterDot.isEmpty()) {
                decimal = afterDot.substring(0, Math.min(2, afterDot.length()));
            }
        }

        String normalized = decimal.isEmpty()
                ? category
                : category + "." + decimal;

        // Final validation
        if (!isValid(normalized)) {
            logFallback(raw, "normalized value '" + normalized + "' failed validation");
            return UNKNOWN_CODE;
        }

        return normalized;
    }

    /**
     * Checks whether a given string is a valid ICD-10 (WHO) code.
     *
     * @param code the ICD-10 code to validate
     * @return {@code true} if the code is valid according to ICD-10 (WHO) rules,
     *         {@code false} otherwise
     */
    public static boolean isValid(String code) {
        if (code == null) {
            return false;
        }
        return ICD10_WHO_PATTERN.matcher(code).matches();
    }

    /**
     * Logs a normalization fallback to {@code R69}.
     *
     * @param rawInput the original input value
     * @param reason a short explanation of why normalization failed
     */
    private static void logFallback(String rawInput, String reason) {
        logger.warning(() ->
                "ICD-10 normalization fallback to R69. "
                        + "Input='" + rawInput + "', reason=" + reason);
    }
}
