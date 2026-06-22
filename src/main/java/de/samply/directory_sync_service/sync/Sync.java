package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.DirectoryApiWriteToFile;
import de.samply.directory_sync_service.directory.graphql.DirectoryApiGraphql;
import de.samply.directory_sync_service.fhir.FhirApiFactory;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.Collections;
import de.samply.directory_sync_service.directory.rest.DirectoryApiRest;
import de.samply.directory_sync_service.fhir.FhirApi;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.samply.directory_sync_service.fhir.PopulateStarModelInputData;
import de.samply.directory_sync_service.model.FactTable;
import de.samply.directory_sync_service.model.StarModelInput;
import org.hl7.fhir.r4.model.Specimen;
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
     */
    public static boolean syncWithDirectoryFailover(String retryMax, String retryInterval, String fhirStoreUrl, String directoryUrl, String directoryUserName, String directoryUserPass, String directoryUserToken, String directoryDefaultCollectionId, boolean directoryAllowStarModel, int directoryMinDonors, int directoryMaxFacts, boolean directoryMock, boolean directoryOnlyLogin, boolean directoryWriteToFile, String directoryOutputDirectory, boolean importBiobanks, boolean importCollections) {
        logger.info("+++++++++++++++++++ syncWithDirectoryFailover: starting at: " + java.time.LocalDateTime.now().getHour() + ":" + java.time.LocalDateTime.now().getMinute());
        boolean success = false;
        try {
            success = false;
            int retryNum;
            for (retryNum = 0; retryNum < Integer.parseInt(retryMax); retryNum++) {
                logger.info("+++++++++++++++++++ syncWithDirectoryFailover: trying sync, attempt " + retryNum + " of " + retryMax);
                if (syncWithDirectory(fhirStoreUrl, directoryUrl, directoryUserName, directoryUserPass, directoryUserToken, directoryDefaultCollectionId, directoryAllowStarModel, directoryMinDonors, directoryMaxFacts, directoryMock, directoryOnlyLogin, directoryWriteToFile, directoryOutputDirectory, importBiobanks, importCollections)) {
                    success = true;
                    break;
                }
                logger.info("+++++++++++++++++++ syncWithDirectoryFailover: attempt " + retryNum + " of " + retryMax + " failed");
                try {
                    // Sleep for retryInterval seconds before trying again
                    Thread.sleep(Integer.parseInt(retryInterval) * 1000L);
                } catch (InterruptedException e) {
                    logger.warn("syncWithDirectoryFailover: problem during Thread.sleep, stack trace:\n" + Util.traceFromException(e));
                }
            }
            if (retryNum == Integer.parseInt(retryMax))
                logger.warn("syncWithDirectoryFailover: reached maximum number of retries (" + Integer.parseInt(retryMax) + "), giving up");
        } catch (NumberFormatException e) {
            logger.warn("syncWithDirectoryFailover: stack trace:\n" + Util.traceFromException(e));
            logger.warn("syncWithDirectoryFailover: exception" + e);
        }

        logger.info("+++++++++++++++++++ syncWithDirectoryFailover: done");

        return success;
    }

    private static boolean syncWithDirectory(String fhirStoreUrl, String directoryUrl, String directoryUserName, String directoryUserPass, String directoryUserToken, String directoryDefaultCollectionId, boolean directoryAllowStarModel, int directoryMinDonors, int directoryMaxFacts, boolean directoryMock, boolean directoryOnlyLogin, boolean directoryWriteToFile, String directoryOutputDirectory, boolean importBiobanks, boolean importCollections) {
        logger.info(">>>>>>>>>>>>>>> syncWithDirectory: entered");
        Map<String, String> correctedDiagnoses;
        // Re-initialize helper classes every time this method gets called
        //FhirApi fhirApi = new FhirApi(fhirStoreUrl);
        FhirApi fhirApi = FhirApiFactory.create(fhirStoreUrl);

        // Decide which API to use: dump to file, GraphQL or REST.
        DirectoryApi directoryApi;
        if (directoryWriteToFile) {
            directoryApi = new DirectoryApiWriteToFile(directoryOutputDirectory);
        } else {
            directoryApi = new DirectoryApiGraphql(directoryUrl, directoryMock, directoryUserName, directoryUserPass, directoryUserToken);

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

        logger.info("syncWithDirectory: TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT get diagnosis corrections");

        filterOutCollectionsNotInDirectory(fhirApi, directoryApi, directoryDefaultCollectionId);

        // The Directory is very strict about diagnosis codes and will reject an entire collection
        // if even one code is not recognized. So first find out which codes it will accept and
        // make a note of them.
        boolean correctedDiagnosesProblem = false;
        correctedDiagnoses = DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApi, directoryDefaultCollectionId);
        if (correctedDiagnoses == null) {
            logger.warn("syncWithDirectory: problem finding diagnosis corrections");
            // If there was a problem getting corrected diagnoses, carry on with an empty list,
            // but take note of what happened.
            correctedDiagnosesProblem = true;
            correctedDiagnoses = new HashMap<String, String>();
        } else if (correctedDiagnoses.size() == 0)
            logger.warn("syncWithDirectory: no diagnosis corrections were found");

        boolean factTableProblem = false;
        if (directoryAllowStarModel) {
            logger.info("syncWithDirectory: TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT update star model");
            // Pull data from the FHIR store and save it in a format suitable for generating
            // star model hypercubes. If a problem arises, note it, but carry on.
            StarModelInput starModelInput = (new PopulateStarModelInputData(fhirApi)).populate(directoryDefaultCollectionId);
            if (starModelInput == null) {
                logger.warn("syncWithDirectory: Problem getting star model information from FHIR store");
                factTableProblem = true;
            } else {
                logger.info("syncWithDirectory: TTTTTTTTTTTTTTTTTTTTTTTTTTT number of collection IDs: " + starModelInput.getInputCollectionIds().size());

                // Send fact tables to Directory
                FactTable factTable = StarModelUpdater.sendStarModelUpdatesToDirectory(directoryApi, correctedDiagnoses, starModelInput, directoryMinDonors, directoryMaxFacts);
                if (factTable == null) {
                    logger.warn("syncWithDirectory: there was a problem during star model update to Directory");
                    factTableProblem = true;
                } else
                    factTable.runSanityChecks(fhirApi, directoryDefaultCollectionId);
            }
        }

        logger.info("syncWithDirectory: TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT update collections");

        boolean updateCollectionsProblem = false;
        // Mine the FHIR store for all available collections. This gets aggregated
        // information about the collections, like the number of samples, the number
        // of patients, a list of diagnosis codes, etc. If a problem arises, note
        // it, but carry on.
        Collections collections = fhirApi.generateCollections(directoryDefaultCollectionId);
        if (collections == null) {
            logger.warn("syncWithDirectory: Problem getting collections from FHIR store");
            updateCollectionsProblem = true;
        } else {
            logger.info("syncWithDirectory: TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT initial collections.size(): " + collections.size());

            // Apply corrections to ICD 10 diagnoses, to make them compatible with
            // the Directory.
            collections.applyDiagnosisCorrections(correctedDiagnoses);

            // Get basic information for each collection from the Directory.
            // This is stuff like collection name, description, associated biobank,
            // etc. It gets combined with the information from the FHIR store.
            directoryApi.fetchBasicCollectionData(collections);
            logger.info("syncWithDirectory: TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT after fetching data collections.size(): " + collections.size());

            // Push the combined collection information (from the FHIR store and the basic
            // information from the Directory) to the Directory.
            if (!directoryApi.sendUpdatedCollections(collections)) {
                logger.warn("syncWithDirectory: Problem during collection update");
                updateCollectionsProblem = true;
            }
        }

        logger.info("syncWithDirectory: TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT import metadata");

        boolean importProblem = false;
        // Pull metadata from the Directory and insert it into the FHIR store.  If a problem
        // arises, note it, but carry on.
        if (importBiobanks) {
            if (!BiobanksUpdater.updateBiobanksInFhirStore(fhirApi, directoryApi)) {
                logger.warn("syncWithDirectory: there was a problem when updating biobanks in FHIR store");
                importProblem = true;
            }
        }
        if (importCollections) {
            if (!CollectionsUpdater.updateCollectionsInFhirStore(fhirApi, collections)) {
                logger.warn("syncWithDirectory: there was a problem when updating collections in FHIR store");
                importProblem = true;
            }
        }

        logger.info("syncWithDirectory: TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT report problems");

        if (correctedDiagnosesProblem || factTableProblem || updateCollectionsProblem || importProblem) {
            if (correctedDiagnosesProblem)
                logger.warn("syncWithDirectory: there was a problem during diagnosis corrections, see above logs for more details");
            if (factTableProblem)
                logger.warn("syncWithDirectory: there was a problem during fact table generation/insertion, see above logs for more details");
            if (updateCollectionsProblem)
                logger.warn("syncWithDirectory: there was a problem during collection updating, see above logs for more details");
            if (importProblem)
                logger.warn("syncWithDirectory: there was a problem during import of biobank or collection metadata, see above logs for more details");
        } else
            logger.info(">>>>>>>>>>>>>>> syncWithDirectory: all synchronization tasks finished");
        return true;
    }

    /**
     * Keep only those collections that the Directory also knows about. This avoids attempts
     * to push collections or facts that the Directory does not know about.
     *
     * This method takes advantage of a side-effect of FhirApi.fetchSpecimensByCollection():
     * it returns a list of specimens for each collection which it also stores in the FhirApi object.
     * Changes made to the returned list will thus also be stored in the FhirApi object.
     * This object is then returned whenever the fetchSpecimensByCollection subsequently is called, complete
     * with any changes.
     *
     * @param fhirApi
     * @param directoryApi
     * @param directoryDefaultCollectionId
     */
    private static void filterOutCollectionsNotInDirectory(FhirApi fhirApi, DirectoryApi directoryApi, String directoryDefaultCollectionId) {
        BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                .valueOf(directoryDefaultCollectionId)
                .orElse(null);
        Map<String, List<Specimen>> specimensByCollection = fhirApi.fetchSpecimensByCollection(defaultBbmriEricCollectionId);
        if (specimensByCollection == null) {
            logger.info("filterOutCollectionsNotInDirectory: not able to get specimens from FHIR store");
            return;
        }
        Set<String> fhirKnownCollectionIds = specimensByCollection.keySet();
        if (fhirKnownCollectionIds == null) {
            logger.info("filterOutCollectionsNotInDirectory: not able to get known collection ids from FHIR store");
            return;
        }
        if (fhirKnownCollectionIds.isEmpty()) {
            logger.info("filterOutCollectionsNotInDirectory: no collections found in FHIR store");
            return;
        }
        logger.info("filterOutCollectionsNotInDirectory: get collection IDs from Directory");

        // TODO: the next line extracts a country code from a random collection ID. This is OK if all collections are from the same country but will break if collections are from multiple countries.
        String countryCode = directoryApi.extractCountryCodeFromBbmriEricId(fhirKnownCollectionIds.iterator().next());
        List<String> directoryknownCollectionIds = directoryApi.fetchKnownCollectionIds(countryCode);
        if (directoryknownCollectionIds == null) {
            logger.info("filterOutCollectionsNotInDirectory: not able to get known collection ids from Directory");
            return;
        }
        for (String collectionId : fhirKnownCollectionIds) {
            if (!directoryknownCollectionIds.contains(collectionId)) {
                logger.info("filterOutCollectionsNotInDirectory: removing collection " + collectionId + " from collections to be updated, because it is not known to the Directory.");
                specimensByCollection.remove(collectionId);
            }
        }
    }
}
