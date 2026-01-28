package de.samply.directory_sync_service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import de.samply.directory_sync_service.sync.Sync;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.stream.Collectors;

public class Util {
  private static final Logger logger = LoggerFactory.getLogger(Sync.class);

    /**
     * Converts a list of maps containing string values into a delimited table format.
     *
     * <p>The keys of the maps form the column headers. The order of columns is determined by the
     * provided {@code columnOrder} list, followed by any remaining keys sorted alphabetically.
     *
     * @param data A list of maps, where each map represents a row and contains string key-value pairs.
     * @param delimiter The delimiter used to separate columns in the table output.
     * @param columnOrder A list specifying the desired order of columns; missing keys are added last in alphabetical order.
     * @return A string representing the table, with headers and rows separated by newlines.
     */
    public static String convertListOfStringMapsToTable(List<Map<String, String>> data, String delimiter, List<String> columnOrder) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        List<Map<String, Object>> objectData = new ArrayList<>();
        for (Map<String, String> map : data) {
            if (map == null || map.isEmpty())
                continue;
            Map<String, Object> objectMap = new HashMap<>();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                objectMap.put(entry.getKey(), entry.getValue());
            }
            objectData.add(objectMap);
        }

        return convertListOfMapsToTable(objectData,  delimiter, columnOrder);
    }

    /**
     * Converts a list of maps containing objects into a delimited table format.
     *
     * <p>Each map represents a row, with the keys forming the column headers. Column order is
     * determined by {@code columnOrder}, with any unspecified columns appended in alphabetical order.
     * The method ensures proper formatting of various object types, including strings, integers,
     * and nested lists of maps.
     *
     * @param data A list of maps where each map represents a row, containing string or object values.
     * @param delimiter The delimiter used to separate columns in the output.
     * @param columnOrder A list specifying the desired order of columns; missing keys are added last in alphabetical order.
     * @return A formatted table as a string, with headers and rows separated by newlines.
     */
    public static String convertListOfMapsToTable(List<Map<String, Object>> data, String delimiter, List<String> columnOrder) {
        if (data == null || data.isEmpty()) {
            return "";
        }

        // Collect all possible keys from the data
        Set<String> allKeys = data.stream()
                .flatMap(map -> map.keySet().stream())
                .collect(Collectors.toSet());

        // Separate keys into those specified in the columnOrder and any remaining keys
        List<String> orderedKeys = new ArrayList<>(columnOrder);
        Set<String> orderedKeySet = new HashSet<>(columnOrder);

        // Add remaining keys not in the columnOrder, sorted alphabetically
        allKeys.stream()
                .filter(key -> !orderedKeySet.contains(key))
                .sorted()
                .forEach(orderedKeys::add);

        // Create the header row
        String headerRow = String.join(delimiter, orderedKeys);

        // Create the data rows
        List<String> rows = new ArrayList<>();
        for (Map<String, Object> map : data) {
            List<String> row = new ArrayList<>();
            for (String key : orderedKeys) {
                Object value = map.get(key);
                row.add(formatValue(value));
            }
            rows.add(String.join(delimiter, row));
        }

        // Sort the list alphabetically, to make the return value more predictable.
        Collections.sort(rows);

        // Combine header and rows into the final table
        rows.add(0, headerRow);
        return String.join("\n", rows);
    }

    /**
     * Formats an object for table output.
     *
     * <p>Handles different data types, including:
     * <ul>
     *   <li>Strings: Returned as is.</li>
     *   <li>Integers: Converted to string.</li>
     *   <li>Maps: Extracts the value of the "name" key, or "id" if "name" is absent.</li>
     *   <li>Lists of Maps: Extracts and joins the "name" values from each map.</li>
     *   <li>Unknown types: Logged as a warning and converted to a string.</li>
     * </ul>
     *
     * @param value The object to be formatted.
     * @return A string representation of the value.
     */
    private static String formatValue(Object value) {
        if (value == null) return "";
        if (value instanceof String) return (String) value;
        if (value instanceof Integer) return value.toString();
        if (value instanceof Map map) {
            if (map.containsKey("name"))
                return map.get("name").toString();
            if (map.containsKey("id"))
                return map.get("id").toString();
        }
        if (value instanceof List<?> list) {
            return list.stream()
                    .filter(item -> item instanceof Map)
                    .map(item -> ((Map<?, ?>) item).get("name"))
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
        }
        logger.warn("formatValue: Unknown value type: " + value.getClass().getName());
        return value.toString();
    }

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

    /**
     * Extracts and concatenates error messages from an {@link OperationOutcome}.
     * <p>
     * This method iterates over the issues in the provided {@code OperationOutcome} and
     * extracts the diagnostic messages for issues with a severity level of {@code ERROR}
     * or {@code FATAL}. The messages are concatenated into a single string, with each
     * message separated by a newline.
     *
     * @param operationOutcome The {@code OperationOutcome} from which to extract error messages.
     * @return A concatenated string of error messages, or an empty string if there are no errors.
     */
    public static String getErrorMessageFromOperationOutcome(OperationOutcome operationOutcome) {
        String errorMessage = "";
        List<OperationOutcome.OperationOutcomeIssueComponent> issues = operationOutcome.getIssue();
        for (OperationOutcome.OperationOutcomeIssueComponent issue: issues) {
            OperationOutcome.IssueSeverity severity = issue.getSeverity();
            if (severity == OperationOutcome.IssueSeverity.ERROR || severity == OperationOutcome.IssueSeverity.FATAL)
                errorMessage += issue.getDiagnostics() + "\n";
        }

        return errorMessage;
    }

    /**
     * Converts an object into a JSON string representation using Gson library.
     *
     * @param object The object to convert into a JSON string.
     * @return The JSON string representation of the object.
     */
    public static String jsonStringFomObject(Object object) {
        if (object == null)
            return null;
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(object);
    }

    /**
     * Generates an ordered key list from the provided map.
     *
     * @param map The map from which to generate the ordered key list.
     * @return A comma-separated string of the ordered keys.
     */
    public static String orderedKeylistFromMap(Map<String, String> map) {
        if (map == null)
            return "";
        // generate an ordered key list from the map
        List<String> keys = new ArrayList<>(map.keySet());
        Collections.sort(keys);
        return String.join(",", keys);
    }

    /**
     * Prints a list of maps as a formatted table with semicolon-separated columns.
     *
     * <p>The method dynamically determines the table's headers by computing the union of all keys
     * across the maps. It also calculates the maximum width for each column to ensure proper alignment.
     * If the input list is empty or null, a message is displayed instead of a table.
     *
     * @param data A {@link List} of {@link Map} objects, where each map represents a row of data.
     *             The keys in the maps act as column headers, and the values are the cell contents.
     *             Missing values in a row are represented as empty strings.
     */
    public static void printAsTable(List<Map<String, String>> data) {
        if (data == null || data.isEmpty()) {
            System.out.println("No data available.");
            return;
        }

        // 1. Compute the union of all keys. (LinkedHashSet preserves insertion order.)
        Set<String> keySet = new LinkedHashSet<>();
        for (Map<String, String> record : data) {
            keySet.addAll(record.keySet());
        }
        List<String> headers = new ArrayList<>(keySet);

        // 2. Determine maximum width for each column (based on header and content lengths).
        Map<String, Integer> colWidths = new HashMap<>();
        for (String header : headers) {
            int maxWidth = header.length();
            for (Map<String, String> record : data) {
                String value = record.get(header);
                if (value != null && value.length() > maxWidth) {
                    maxWidth = value.length();
                }
            }
            colWidths.put(header, maxWidth);
        }

        // 3. Print the header row with semicolon as the column separator.
        for (int i = 0; i < headers.size(); i++) {
            String header = headers.get(i);
            int width = colWidths.get(header);
            // Format header left-justified.
            System.out.printf("%-" + width + "s", header);
            if (i < headers.size() - 1) {
                System.out.print(";");
            }
        }
        System.out.println();

        // 5. Print each data row with semicolon as the separator.
        for (Map<String, String> record : data) {
            for (int i = 0; i < headers.size(); i++) {
                String header = headers.get(i);
                int width = colWidths.get(header);
                String value = record.getOrDefault(header, "");
                System.out.printf("%-" + width + "s", value);
                if (i < headers.size() - 1) {
                    System.out.print(";");
                }
            }
            System.out.println();
        }
    }

    /**
     * Serializes the given object to a JSON string and writes it to the specified file.
     * The method converts the provided {@code object} into its JSON representation using
     * {@link Util#jsonStringFomObject(Object)} and writes the resulting string to the file
     * located at {@code filePath}.
     * <p>
     * If the serialization or file writing operation fails, the method logs an error message.
     * <p>
     * Note: The file will be overwritten if it already exists.
     *
     * @param object   the object to serialize to JSON and save to the file. Should not be {@code null}.
     * @param filePath the path to the file where the serialized object will be saved. Should be a valid file path.
     */
    public static void serializeToFile(Object object, String filePath) {
        if (object == null) {
            logger.warn("serializeToFile: object is null");
            return;
        }
        if (filePath == null) {
            logger.warn("serializeToFile: filePath is null");
            return;
        }
        if (filePath.isEmpty()) {
            logger.warn("serializeToFile: filePath is empty");
            return;
        }

        File outputFile = new File(filePath);
        String objectJsonString = jsonStringFomObject(object);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write(objectJsonString);
        } catch (IOException e) {
            logger.error("serializeToFile: Failed to write object to file", Util.traceFromException(e));
        }
    }
}
