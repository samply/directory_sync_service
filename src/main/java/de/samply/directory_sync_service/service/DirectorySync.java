package de.samply.directory_sync_service.service;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import de.samply.directory_sync_service.sync.Sync;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.fhir.FhirReporting;
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
     * @throws IOException
     */
    public void syncWithDirectoryFailover(Configuration configuration) {
        String retryMax = configuration.getRetryMax();
        String retryInterval = configuration.getRetryInterval();
        for (int retryNum = 0; retryNum < Integer.parseInt(retryMax); retryNum++) {
            if (retryNum > 0) {
                try {
                    Thread.sleep(Integer.parseInt(retryInterval) * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("syncWithDirectoryFailover: retrying sync, attempt " + retryNum + " of " + retryMax);
            }
            if (syncWithDirectory(configuration))
                break;
        }
    }

    /**
     * Performs the synchronization with the Directory.
     *
     * @return                   Return true if synchronization successful.
     * @throws IOException
     */
    private boolean syncWithDirectory(Configuration configuration) {
        String directoryUrl = configuration.getDirectoryUrl();
        String fhirStoreUrl = configuration.getFhirStoreUrl();
        String directoryUserName = configuration.getDirectoryUserName();
        String directoryUserPass = configuration.getDirectoryUserPass();
        String directoryDefaultCollectionId = configuration.getDirectoryDefaultCollectionId();
        boolean directoryAllowStarModel = Boolean.parseBoolean(configuration.getDirectoryAllowStarModel());
        int directoryMinDonors = Integer.parseInt(configuration.getDirectoryMinDonors());
        int directoryMaxFacts = Integer.parseInt(configuration.getDirectoryMaxFacts());
        boolean directoryMock = Boolean.parseBoolean(configuration.getDirectoryMock());

        DirectoryApi directoryApi = null;
        try {
            Either<OperationOutcome, DirectoryApi> directoryApiContainer = createDirectoryApi(directoryUserName, directoryUserPass, directoryUrl, directoryMock);
            if (directoryApiContainer.isLeft()) {
                logger.error("__________ syncWithDirectory: problem setting up Directory API: " + getErrorMessageFromOperationOutcome(directoryApiContainer.getLeft()));
                return false;
            }
            directoryApi = directoryApiContainer.get();
        } catch (NullPointerException e) {
            logger.error("__________ syncWithDirectory: createDirectoryApi failed: " + Util.traceFromException(e));
            return false;
        }
        FhirApi fhirApi = createFhirApi(fhirStoreUrl);
        FhirReporting fhirReporting = new FhirReporting(ctx, fhirApi);
        Sync sync = new Sync(fhirApi, fhirReporting, directoryApi);
        Either<String, Void> initResourcesOutcome = sync.initResources();
        if (initResourcesOutcome.isLeft()) {
            logger.error("__________ syncWithDirectory: problem initializing FHIR resources: " + initResourcesOutcome.getLeft());
            logger.error("__________ syncWithDirectory: fhirStoreUrl: " + fhirStoreUrl);
            return false;
        }
        List<OperationOutcome> operationOutcomes;
        operationOutcomes = sync.generateDiagnosisCorrections(directoryDefaultCollectionId);
        for (OperationOutcome operationOutcome : operationOutcomes) {
            String errorMessage = getErrorMessageFromOperationOutcome(operationOutcome);
            if (errorMessage.length() > 0) {
                logger.error("__________ syncWithDirectory: there was a problem during diagnosis corrections: " + errorMessage);
                return false;
            }
        }
        if (directoryAllowStarModel) {
            operationOutcomes = sync.sendStarModelUpdatesToDirectory(directoryDefaultCollectionId, directoryMinDonors, directoryMaxFacts);
            for (OperationOutcome operationOutcome : operationOutcomes) {
                String errorMessage = getErrorMessageFromOperationOutcome(operationOutcome);
                if (errorMessage.length() > 0) {
                    logger.error("__________ syncWithDirectory: there was a problem during star model update to Directory: " + errorMessage);
                    return false;
                }
            }
        }
        operationOutcomes = sync.sendUpdatesToDirectory(directoryDefaultCollectionId);
        boolean failed = false;
        for (OperationOutcome operationOutcome : operationOutcomes) {
            String errorMessage = getErrorMessageFromOperationOutcome(operationOutcome);
            if (errorMessage.length() > 0) {
                logger.error("__________ syncWithDirectory: there was a problem during sync to Directory: " + errorMessage);
                failed = true;
            }
        }
        if (failed)
            return false;
       operationOutcomes = sync.updateAllBiobanksOnFhirServerIfNecessary();
       for (OperationOutcome operationOutcome : operationOutcomes) {
            String errorMessage = getErrorMessageFromOperationOutcome(operationOutcome);
            if (errorMessage.length() > 0) {
                logger.error("__________ syncWithDirectory: there was a problem during sync from Directory: " + errorMessage);
//                return false;
            }
       }

       logger.info("__________ syncWithDirectory: all synchronization tasks finished");
       return true;
    }

    private String getErrorMessageFromOperationOutcome(OperationOutcome operationOutcome) {
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
     * Opens a connection to the Directory API.
     * <p>
     * This is where the login to the Directory happens.
     *
     * @param directoryUserName User name for logging in to Directory
     * @param directoryPassCode Password for logging in to Directory
     * @param directoryUrl      Base URL of the Directory
     * @param directoryMock
     * @return
     */
    private Either<OperationOutcome, DirectoryApi> createDirectoryApi(String directoryUserName, String directoryPassCode, String directoryUrl, boolean directoryMock) {
        CloseableHttpClient client = HttpClients.createDefault();
        return Either.right(new DirectoryApi(client, directoryUrl, directoryMock, directoryUserName, directoryPassCode));
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
