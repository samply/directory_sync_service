package de.samply.directory_sync_service.service;

import ca.uhn.fhir.context.FhirContext;
import de.samply.directory_sync_service.sync.Sync;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.fhir.FhirReporting;
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

        DirectoryApi directoryApi = new DirectoryApi(directoryUrl, directoryMock, directoryUserName, directoryUserPass);
        FhirApi fhirApi = new FhirApi(fhirStoreUrl);
        FhirReporting fhirReporting = new FhirReporting(fhirApi);
        Sync sync = new Sync(fhirApi, fhirReporting, directoryApi);
        List<OperationOutcome> operationOutcomes;
        operationOutcomes = sync.generateDiagnosisCorrections(directoryDefaultCollectionId);
        for (OperationOutcome operationOutcome : operationOutcomes) {
            String errorMessage = Util.getErrorMessageFromOperationOutcome(operationOutcome);
            if (errorMessage.length() > 0) {
                logger.error("__________ syncWithDirectory: there was a problem during diagnosis corrections: " + errorMessage);
                return false;
            }
        }
        if (directoryAllowStarModel) {
            operationOutcomes = sync.sendStarModelUpdatesToDirectory(directoryDefaultCollectionId, directoryMinDonors, directoryMaxFacts);
            for (OperationOutcome operationOutcome : operationOutcomes) {
                String errorMessage = Util.getErrorMessageFromOperationOutcome(operationOutcome);
                if (errorMessage.length() > 0) {
                    logger.error("__________ syncWithDirectory: there was a problem during star model update to Directory: " + errorMessage);
                    return false;
                }
            }
        }
        operationOutcomes = sync.sendUpdatesToDirectory(directoryDefaultCollectionId);
        boolean failed = false;
        for (OperationOutcome operationOutcome : operationOutcomes) {
            String errorMessage = Util.getErrorMessageFromOperationOutcome(operationOutcome);
            if (errorMessage.length() > 0) {
                logger.error("__________ syncWithDirectory: there was a problem during sync to Directory: " + errorMessage);
                failed = true;
            }
        }
        if (failed)
            return false;
       operationOutcomes = sync.updateAllBiobanksOnFhirServerIfNecessary();
       for (OperationOutcome operationOutcome : operationOutcomes) {
            String errorMessage = Util.getErrorMessageFromOperationOutcome(operationOutcome);
            if (errorMessage.length() > 0) {
                logger.error("__________ syncWithDirectory: there was a problem during sync from Directory: " + errorMessage);
//                return false;
            }
       }

       logger.info("__________ syncWithDirectory: all synchronization tasks finished");
       return true;
    }
}
