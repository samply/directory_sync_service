package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides functionality to synchronize between a BBMRI Directory instance and a FHIR store in both directions.
 * This class provides methods to update biobanks, synchronize collection sizes, generate diagnosis corrections,
 * send star model updates, and perform aggregated updates to the Directory service based on information from
 * the FHIR store.
 * <p>
 * Usage:
 * <p>
 * You will need to supply a set of parameters to control exactly how synchronization should operate. These
 * are set in the constructor. Your code might look something like
 * this:
 * <p>
 * Sync sync = new Sync(<Parameters>);
 * <p>
 * Now you can start to do some synchronization, e.g.:
 * <p>
 * sync.syncWithDirectoryFailover();
 * <p>
 */
public class Sync {
    private static final Logger logger = LoggerFactory.getLogger(Sync.class);

    /**
     * Attempts to perform synchronization with the Directory repeatedly, until it either
     * succeeds, or the number of attempts exceeds a threshold.
     *
     * @throws IOException
     */
    public static void syncWithDirectoryFailover(String retryMax, String retryInterval, String fhirStoreUrl, String directoryUrl, String directoryUserName, String directoryUserPass, String directoryDefaultCollectionId, boolean directoryAllowStarModel, int directoryMinDonors, int directoryMaxFacts, boolean directoryMock) {
        for (int retryNum = 0; retryNum < Integer.parseInt(retryMax); retryNum++) {
            if (retryNum > 0) {
                try {
                    Thread.sleep(Integer.parseInt(retryInterval) * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("syncWithDirectoryFailover: retrying sync, attempt " + retryNum + " of " + retryMax);
            }
            if (syncWithDirectory(retryMax, retryInterval, fhirStoreUrl, directoryUrl, directoryUserName, directoryUserPass, directoryDefaultCollectionId, directoryAllowStarModel, directoryMinDonors, directoryMaxFacts, directoryMock))
                break;
        }
    }

    private static boolean syncWithDirectory(String retryMax, String retryInterval, String fhirStoreUrl, String directoryUrl, String directoryUserName, String directoryUserPass, String directoryDefaultCollectionId, boolean directoryAllowStarModel, int directoryMinDonors, int directoryMaxFacts, boolean directoryMock) {
        Map<String, String> correctedDiagnoses = null;
        // Re-initialize helper classes every time this method gets called
        FhirApi fhirApi = new FhirApi(fhirStoreUrl);
        DirectoryApi directoryApi = new DirectoryApi(directoryUrl, directoryMock, directoryUserName, directoryUserPass);

        correctedDiagnoses = DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApi, directoryDefaultCollectionId);
        if (correctedDiagnoses == null) {
            logger.warn("syncWithDirectory: there was a problem during diagnosis corrections");
            return false;
        }
        if (directoryAllowStarModel)
            if (!StarModelUpdater.sendStarModelUpdatesToDirectory(fhirApi, directoryApi, correctedDiagnoses, directoryDefaultCollectionId, directoryMinDonors, directoryMaxFacts)) {
                logger.warn("syncWithDirectory: there was a problem during star model update to Directory");
                return false;
            }
        if (!CollectionUpdater.sendUpdatesToDirectory(fhirApi, directoryApi, correctedDiagnoses, directoryDefaultCollectionId)) {
            logger.warn("syncWithDirectory: there was a problem during sync to Directory");
            return false;
        }

        if (!BiobanksUpdater.updateBiobanksInFhirStore(fhirApi, directoryApi)) {
            logger.warn("syncWithDirectory: there was a problem during sync from Directory");
            return false;
        }

        logger.info("__________ syncWithDirectory: all synchronization tasks finished");
        return true;
    }
}
