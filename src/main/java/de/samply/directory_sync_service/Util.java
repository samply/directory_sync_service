package de.samply.directory_sync_service;

import org.hl7.fhir.r4.model.OperationOutcome;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;

public class Util {
  public static <K, V> Map<K, V> mapOf() {
    return new HashMap<>();
  }

    /**
     * Creates a new map with a single entry consisting of the specified key and value.
     *
     * @param key the key for the entry
     * @param value the value associated with the key
     * @return a new map containing the specified key-value pair
     */
  public static <K, V> Map<K, V> mapOf(K key, V value) {
    HashMap<K, V> map = new HashMap<>();
    map.put(key, value);
    return map;
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
     * Creates a list containing a single {@link OperationOutcome} that represents an error.
     * <p>
     * The generated {@code OperationOutcome} will have an issue with a severity level of
     * {@code ERROR} and will include the provided diagnostic message.
     *
     * @param diagnostics A diagnostic message that provides details about the error.
     * @return A list containing a single {@code OperationOutcome} object indicating an error.
     */
    public static List<OperationOutcome> createErrorOutcome(String diagnostics) {
        OperationOutcome outcome = new OperationOutcome();
        outcome.addIssue().setSeverity(ERROR).setDiagnostics(diagnostics);
        return Collections.singletonList(outcome);
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
}
