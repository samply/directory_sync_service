package de.samply.directory_sync_service.directory;

import org.hl7.fhir.r4.model.OperationOutcome;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;

public class DirectoryUtils {
  public static OperationOutcome error(String action, String message) {
    OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue().setSeverity(ERROR).setDiagnostics(errorMsg(action, message));
    return outcome;
  }

  public static String errorMsg(String action, String message) {
    return String.format("Error in BBMRI Directory response for %s, cause: %s", action,
        message);
  }
}
