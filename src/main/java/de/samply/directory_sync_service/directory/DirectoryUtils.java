package de.samply.directory_sync_service.directory;

import org.hl7.fhir.r4.model.OperationOutcome;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.INFORMATION;

/**
 * Utility class for handling operations related to the Directory service.
 */
public class DirectoryUtils {
  /**
   * Creates an {@link OperationOutcome} with a specified error message.
   * <p>
   * This method generates an {@code OperationOutcome} object with the severity level set to {@code ERROR}.
   * It includes a detailed diagnostic message that describes the action that failed and the cause of the error.
   * </p>
   *
   * @param message a detailed message describing the cause of the error
   * @return an {@link OperationOutcome} object populated with the error details
   */
  public static OperationOutcome error(String message) {
    OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue().setSeverity(ERROR).setDiagnostics(message);
    return outcome;
  }

  /**
   * Creates an {@link OperationOutcome} instance representing a successful operation.
   * <p>
   * The created {@code OperationOutcome} will have an issue with a severity level of
   * {@code INFORMATION} and will include the provided diagnostic message.
   *
   * @param message A diagnostic message that provides additional details about the success.
   * @return An {@code OperationOutcome} object indicating the success of an operation.
   */
  public static OperationOutcome success(String message) {
    OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue()
            .setSeverity(INFORMATION)
            .setDiagnostics(message);
    return outcome;
  }
}
