package de.samply.directory_sync_service;

import de.samply.directory_sync_service.sync.Sync;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;

public class Util {
  private static final Logger logger = LoggerFactory.getLogger(Sync.class);

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

    /**
     * Reports a list of {@link OperationOutcome} objects by logging any errors found.
     * <p>
     * This method iterates through the provided list of {@code OperationOutcome} objects,
     * extracting and logging any error messages. If any errors are encountered, they are logged
     * at the {@code ERROR} level and the method will return {@code false}.
     * If no errors are found, the method returns {@code true}.
     *
     * @param operationOutcomes A list of {@code OperationOutcome} objects to be checked for errors.
     * @return {@code true} if no errors were found in any of the {@code OperationOutcome} objects;
     *         {@code false} if at least one error was found and logged.
     */
    public static boolean reportOperationOutcomes(List<OperationOutcome> operationOutcomes) {
        boolean failed = false;
        for (OperationOutcome operationOutcome : operationOutcomes) {
            String errorMessage = Util.getErrorMessageFromOperationOutcome(operationOutcome);
            if (errorMessage.length() > 0) {
                logger.error(errorMessage);
                failed = true;
            }
        }
        return !failed;
    }
}
