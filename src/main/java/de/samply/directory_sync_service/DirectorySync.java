package de.samply.directory_sync_service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import de.samply.directory_sync.Sync;
import de.samply.directory_sync.directory.DirectoryApi;
import de.samply.directory_sync.directory.DirectoryService;
import de.samply.directory_sync.fhir.FhirApi;
import de.samply.directory_sync.fhir.FhirReporting;
import io.vavr.control.Either;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hl7.fhir.r4.model.OperationOutcome;

import java.io.IOException;
import java.util.List;

/**
 * This class sets up connections to the FHIR store and to the Directory and
 * them performs a synchronization.
 */
public class DirectorySync {
    private static Logger logger = LogManager.getLogger(DirectorySync.class);
    private final FhirContext ctx = FhirContext.forR4();

    /**
     * Attempts to perform synchronization with the Directory repeatedly, until it either
     * succeeds, or the number of attempts exceeds a threshold.
     *
     * @param directoryUserName  User name for logging in to Directory
     * @param directoryPassCode  Password for logging in to Directory
     * @param directoryUrl       Base URL of the Directory
     * @param fhirStoreUrl       URL for Bridgehead Blaze store
     * @param retryMax           Max number of times Directory sync will be retried on failure
     * @param retryInterval      Interval (seconds) between retries
     * @throws IOException
     */
    public void syncWithDirectoryFailover(String directoryUserName, String directoryPassCode, String directoryUrl, String fhirStoreUrl, String retryMax, String retryInterval) {
        for (int retryNum = 0; retryNum < Integer.parseInt(retryMax); retryNum++) {
            if (retryNum > 0) {
                try {
                    Thread.sleep(Integer.parseInt(retryInterval) * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("syncWithDirectoryFailover: retrying sync, attempt " + retryNum + " of " + retryMax);
            }
            if (syncWithDirectory(directoryUserName, directoryPassCode, directoryUrl, fhirStoreUrl))
                break;
        }
    }

    /**
     * Performs the synchronization with the Directory.
     *
     * @param directoryUserName  User name for logging in to Directory
     * @param directoryPassCode  Password for logging in to Directory
     * @param directoryUrl       Base URL of the Directory
     * @param fhirStoreUrl       URL for Bridgehead Blaze store
     * @return                   Return true if synchronization successful.
     * @throws IOException
     */
    private boolean syncWithDirectory(String directoryUserName, String directoryPassCode, String directoryUrl, String fhirStoreUrl) {
        DirectoryApi directoryApi = null;
        try {
            Either<OperationOutcome, DirectoryApi> directoryApiContainer = createDirectoryApi(directoryUserName, directoryPassCode, directoryUrl);
            if (directoryApiContainer.isLeft()) {
                logger.warn("syncWithDirectory: problem setting up Directory API: " + directoryApiContainer.getLeft().getIssue());
                return false;
            }
            directoryApi = directoryApiContainer.get();
        } catch (IOException e) {
            logger.warn("syncWithDirectory: createDirectoryApi failed");
            logger.warn(e.toString());
            return false;
        }
        DirectoryService directoryService = new DirectoryService(directoryApi);
        FhirApi fhirApi = createFhirApi(fhirStoreUrl);
        FhirReporting fhirReporting = new FhirReporting(ctx, fhirApi);
        Sync sync = new Sync(fhirApi, fhirReporting, directoryApi, directoryService);
        Either<String, Void> result = sync.initResources();
        List<OperationOutcome> operationOutcomes = sync.syncCollectionSizesToDirectory();
        for (OperationOutcome operationOutcome : operationOutcomes) {
            List<OperationOutcome.OperationOutcomeIssueComponent> issues = operationOutcome.getIssue();
            for (OperationOutcome.OperationOutcomeIssueComponent issue: issues) {
                OperationOutcome.IssueSeverity severity = issue.getSeverity();
                if (severity == OperationOutcome.IssueSeverity.ERROR || severity == OperationOutcome.IssueSeverity.FATAL) {
                    logger.warn("syncWithDirectory: there was a problem during sync to Directory: " + issue.getDiagnostics());
                    return false;
                }
            }
        }
        operationOutcomes = sync.updateAllBiobanksOnFhirServerIfNecessary();
        for (OperationOutcome operationOutcome : operationOutcomes) {
            List<OperationOutcome.OperationOutcomeIssueComponent> issues = operationOutcome.getIssue();
            for (OperationOutcome.OperationOutcomeIssueComponent issue: issues) {
                OperationOutcome.IssueSeverity severity = issue.getSeverity();
                if (severity == OperationOutcome.IssueSeverity.ERROR || severity == OperationOutcome.IssueSeverity.FATAL) {
                    logger.warn("syncWithDirectory: there was a problem during sync from Directory: " + issue.getDiagnostics());
                    return false;
                }
            }
        }

        logger.info("syncWithDirectory: Directory synchronization completed successfully");

        return true;
    }

    /**
     * Opens a connection to the Directory API.
     *
     * This is where the login to the Directory happens.
     *
     * @param directoryUserName  User name for logging in to Directory
     * @param directoryPassCode  Password for logging in to Directory
     * @param directoryUrl       Base URL of the Directory
     * @return
     * @throws IOException
     */
    private Either<OperationOutcome, DirectoryApi> createDirectoryApi(String directoryUserName, String directoryPassCode, String directoryUrl)
            throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        return DirectoryApi.createWithLogin(client, directoryUrl, directoryUserName, directoryPassCode);
    }

    /**
     * Create a connection to the FHIR store (e.g. Blaze).
     *
     * @param fhirStoreUrl       URL for Bridgehead Blaze store
     * @return
     */
    private FhirApi createFhirApi(String fhirStoreUrl) {
        IGenericClient client = ctx.newRestfulGenericClient(fhirStoreUrl);
        client.registerInterceptor(new LoggingInterceptor(true));
        return new FhirApi(client);
    }
}
