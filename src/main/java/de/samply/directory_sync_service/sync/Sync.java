package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.DirectoryApiWriteToFile;
import de.samply.directory_sync_service.directory.graphql.DirectoryApiGraphql;
import de.samply.directory_sync_service.directory.rest.DirectoryApiRest;
import de.samply.directory_sync_service.fhir.FhirApi;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import de.samply.directory_sync_service.fhir.model.FhirCollection;
import de.samply.directory_sync_service.model.BbmriEricId;
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
    public static boolean syncWithDirectoryFailover(String retryMax, String retryInterval, String fhirStoreUrl, String directoryUrl, String directoryUserName, String directoryUserPass, String directoryDefaultCollectionId, boolean directoryAllowStarModel, int directoryMinDonors, int directoryMaxFacts, boolean directoryMock, boolean directoryOnlyLogin, boolean directoryWriteToFile, String directoryOutputDirectory) {
        logger.info("+++++++++++++++++++ syncWithDirectoryFailover: starting");
        boolean success = false;
        int retryNum;
        for (retryNum = 0; retryNum < Integer.parseInt(retryMax); retryNum++) {
            logger.info("syncWithDirectoryFailover: +++++++++++++++++++ trying sync, attempt " + retryNum + " of " + retryMax);
            if (syncWithDirectory(fhirStoreUrl, directoryUrl, directoryUserName, directoryUserPass, directoryDefaultCollectionId, directoryAllowStarModel, directoryMinDonors, directoryMaxFacts, directoryMock, directoryOnlyLogin, directoryWriteToFile, directoryOutputDirectory)) {
                success = true;
                break;
            }
            logger.info("syncWithDirectoryFailover: +++++++++++++++++++ attempt " + retryNum + " of " + retryMax + " failed");
            try {
                // Sleep for retryInterval seconds before trying again
                Thread.sleep(Integer.parseInt(retryInterval) * 1000L);
            } catch (InterruptedException e) {
                logger.warn("syncWithDirectoryFailover: problem during Thread.sleep, stack trace:\n" + Util.traceFromException(e));
            }
        }
        if (retryNum == Integer.parseInt(retryMax))
            logger.warn("syncWithDirectoryFailover: reached maximum number of retires(" + Integer.parseInt(retryMax) + "), giving up");

        logger.info("+++++++++++++++++++ syncWithDirectoryFailover: done");

        return success;
    }

    private static boolean syncWithDirectory(String fhirStoreUrl, String directoryUrl, String directoryUserName, String directoryUserPass, String directoryDefaultCollectionId, boolean directoryAllowStarModel, int directoryMinDonors, int directoryMaxFacts, boolean directoryMock, boolean directoryOnlyLogin, boolean directoryWriteToFile, String directoryOutputDirectory) {
        logger.info(">>>>>>>>>>>>>>> syncWithDirectory: entered");
        Map<String, String> correctedDiagnoses = null;
        // Re-initialize helper classes every time this method gets called
        FhirApi fhirApi = new FhirApi(fhirStoreUrl);
        DirectoryApi directoryApi;
        if (directoryWriteToFile) {
            directoryApi = new DirectoryApiWriteToFile(directoryOutputDirectory);
        } else {
            directoryApi = new DirectoryApiGraphql(directoryUrl, directoryMock, directoryUserName, directoryUserPass);

            // First try to log in via the GraphQL API. If that doesn't work, try the REST API.
            if (!directoryApi.login()) {
                logger.info("syncWithDirectory: Directory GraphQL API is not available, trying REST API");
                directoryApi = new DirectoryApiRest(directoryUrl, directoryMock, directoryUserName, directoryUserPass);
                if (!directoryApi.login()) {
                    logger.warn("syncWithDirectory: there was a problem during login to Directory");
                    return false;
                }
            }
        }

        // Login test. Don't perform any further actions on the Directory.
        if (directoryOnlyLogin) {
            logger.info("syncWithDirectory: login was successful, now quitting because onlyLogin was set to true");
            return true;
        }

        logger.debug("syncWithDirectory: starting synchronization");

        // Get FHIR collections from the FHIR store and list them to debug
        BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                .valueOf(directoryDefaultCollectionId)
                .orElse(null);
        List<FhirCollection> fhirCollections = fhirApi.fetchFhirCollections(defaultBbmriEricCollectionId);
        logger.debug("syncWithDirectory: FHIR collections: ");
        for  (FhirCollection collection : fhirCollections) {
            logger.debug(",  " + collection.getId());
        }

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

        logger.info(">>>>>>>>>>>>>>> syncWithDirectory: all synchronization tasks finished");
        return true;
    }
}
