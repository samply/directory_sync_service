package de.samply.directory_sync_service;

import com.google.gson.Gson;
import de.samply.directory_sync_service.sync.Sync;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
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
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.INFORMATION;

public class Util {
  private static final Logger logger = LoggerFactory.getLogger(Sync.class);

  public static <K, V> Map<K, V> mapOf() {
    return new HashMap<>();
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
        Gson gson = new Gson();
        return gson.toJson(object);
    }
}
