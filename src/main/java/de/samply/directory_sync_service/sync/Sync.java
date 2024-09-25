package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.directory.DirectoryApiRest;
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
    public static void syncWithDirectoryFailover(String retryMax, String retryInterval, String fhirStoreUrl, String directoryUrl, String directoryUserName, String directoryUserPass, String directoryDefaultCollectionId, boolean directoryAllowStarModel, int directoryMinDonors, int directoryMaxFacts, boolean directoryMock, boolean directoryOnlyLogin) {
        for (int retryNum = 0; retryNum < Integer.parseInt(retryMax); retryNum++) {
            if (retryNum > 0) {
                try {
                    Thread.sleep(Integer.parseInt(retryInterval) * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("syncWithDirectoryFailover: retrying sync, attempt " + retryNum + " of " + retryMax);
            }
            if (syncWithDirectory(fhirStoreUrl, directoryUrl, directoryUserName, directoryUserPass, directoryDefaultCollectionId, directoryAllowStarModel, directoryMinDonors, directoryMaxFacts, directoryMock, directoryOnlyLogin))
                break;
        }
    }

    private static boolean syncWithDirectory(String fhirStoreUrl, String directoryUrl, String directoryUserName, String directoryUserPass, String directoryDefaultCollectionId, boolean directoryAllowStarModel, int directoryMinDonors, int directoryMaxFacts, boolean directoryMock, boolean directoryOnlyLogin) {
        Map<String, String> correctedDiagnoses = null;
        // Re-initialize helper classes every time this method gets called
        FhirApi fhirApi = new FhirApi(fhirStoreUrl);
        DirectoryApiRest directoryApiRest = new DirectoryApiRest(directoryUrl, directoryMock, directoryUserName, directoryUserPass);

        if (!directoryApiRest.isAvailable()) {
            logger.warn("syncWithDirectory: Directory REST API is not available");
            return false;
        }

        if (!directoryApiRest.login()) {
            logger.warn("syncWithDirectory: there was a problem during login to Directory");
            return false;
        }

        // Login test. Don't perform any further actions on the Directory.
        if (directoryOnlyLogin) {
            logger.info(">>>>>>>>>>>>>>> syncWithDirectory: login was successful, now quitting because onlyLogin was set to true");
            return true;
        }

        correctedDiagnoses = DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApiRest, directoryDefaultCollectionId);
        if (correctedDiagnoses == null) {
            logger.warn("syncWithDirectory: there was a problem during diagnosis corrections");
            return false;
        }
        if (directoryAllowStarModel)
            if (!StarModelUpdater.sendStarModelUpdatesToDirectory(fhirApi, directoryApiRest, correctedDiagnoses, directoryDefaultCollectionId, directoryMinDonors, directoryMaxFacts)) {
                logger.warn("syncWithDirectory: there was a problem during star model update to Directory");
                return false;
            }
        if (!CollectionUpdater.sendUpdatesToDirectory(fhirApi, directoryApiRest, correctedDiagnoses, directoryDefaultCollectionId)) {
            logger.warn("syncWithDirectory: there was a problem during sync to Directory");
            return false;
        }

        if (!BiobanksUpdater.updateBiobanksInFhirStore(fhirApi, directoryApiRest)) {
            logger.warn("syncWithDirectory: there was a problem during sync from Directory");
            return false;
        }

        logger.info(">>>>>>>>>>>>>>> syncWithDirectory: all synchronization tasks finished");
        return true;
    }
}
