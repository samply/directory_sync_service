package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirCollectionToDirectoryCollectionPutConverter;
import de.samply.directory_sync_service.directory.CreateFactTablesFromStarModelInputData;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.MergeDirectoryCollectionGetToDirectoryCollectionPut;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.fhir.FhirReporting;
import de.samply.directory_sync_service.fhir.model.FhirCollection;
import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import de.samply.directory_sync_service.model.StarModelData;

import java.io.IOException;
import java.util.Map;

import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.INFORMATION;

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
    private final String retryMax;
    private final String retryInterval;
    private final String fhirStoreUrl;
    private final String directoryUrl;
    private final String directoryUserName;
    private final String directoryUserPass;
    private final String directoryDefaultCollectionId;
    private final boolean directoryAllowStarModel;
    private final int directoryMinDonors;
    private final int directoryMaxFacts;
    private final boolean directoryMock;
    private Map<String, String> correctedDiagnoses = null;
    private FhirApi fhirApi;
    private FhirReporting fhirReporting;
    private DirectoryApi directoryApi;
    public static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_NAME = t -> {
        t.fhirBiobank.setName(t.dirBiobank.getName());
        return t;
    };

    public Sync(String retryMax, String retryInterval, String fhirStoreUrl, String directoryUrl, String directoryUserName, String directoryUserPass, String directoryDefaultCollectionId, boolean directoryAllowStarModel, int directoryMinDonors, int directoryMaxFacts, boolean directoryMock) {
        this.retryMax = retryMax;
        this.retryInterval = retryInterval;
        this.fhirStoreUrl = fhirStoreUrl;
        this.directoryUrl = directoryUrl;
        this.directoryUserName = directoryUserName;
        this.directoryUserPass = directoryUserPass;
        this.directoryDefaultCollectionId = directoryDefaultCollectionId;
        this.directoryAllowStarModel = directoryAllowStarModel;
        this.directoryMinDonors = directoryMinDonors;
        this.directoryMaxFacts = directoryMaxFacts;
        this.directoryMock = directoryMock;
    }

    /**
     * Attempts to perform synchronization with the Directory repeatedly, until it either
     * succeeds, or the number of attempts exceeds a threshold.
     *
     * @throws IOException
     */
    public void syncWithDirectoryFailover() {
        for (int retryNum = 0; retryNum < Integer.parseInt(retryMax); retryNum++) {
            if (retryNum > 0) {
                try {
                    Thread.sleep(Integer.parseInt(retryInterval) * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("syncWithDirectoryFailover: retrying sync, attempt " + retryNum + " of " + retryMax);
            }
            if (syncWithDirectory())
                break;
        }
    }

    public boolean syncWithDirectory() {
        // Re-initialize helper classes every time this method gets called
        fhirApi = new FhirApi(fhirStoreUrl);
        fhirReporting = new FhirReporting(fhirApi);
        directoryApi = new DirectoryApi(directoryUrl, directoryMock, directoryUserName, directoryUserPass);

        if (!Util.reportOperationOutcomes(generateDiagnosisCorrections(directoryDefaultCollectionId))) {
                logger.warn("syncWithDirectory: there was a problem during diagnosis corrections");
            return false;
        }
        if (directoryAllowStarModel)
            if (!Util.reportOperationOutcomes(sendStarModelUpdatesToDirectory(directoryDefaultCollectionId, directoryMinDonors, directoryMaxFacts))) {
                logger.warn("syncWithDirectory: there was a problem during star model update to Directory");
                return false;
            }
        if (!Util.reportOperationOutcomes(sendUpdatesToDirectory(directoryDefaultCollectionId))) {
            logger.warn("syncWithDirectory: there was a problem during sync to Directory");
            return false;
        }

        if (!updateAllBiobanksOnFhirServerIfNecessary()) {
            logger.warn("syncWithDirectory: there was a problem during sync from Directory");
            return false;
        }

        logger.info("__________ syncWithDirectory: all synchronization tasks finished");
        return true;
    }

    /**
     * Updates all biobanks from the FHIR server with information from the Directory.
     *
     * @return the individual {@link OperationOutcome}s from each update
     */
    private boolean updateAllBiobanksOnFhirServerIfNecessary() {
        // Retrieve the list of all biobanks
        List<Organization> organizations = fhirApi.listAllBiobanks();

        // Check if the result is a failure or success
        boolean succeeded = true;
        if (organizations == null) {
            logger.warn("error retrieving the biobanks");
            succeeded = false;
        } else {
            // If successful, process each biobank and update it on the FHIR server if necessary
            for (Organization organization : organizations) {
                // Update each biobank and report any errors
                if (!updateBiobankOnFhirServerIfNecessary(organization)) {
                    logger.warn("updateBiobankOnFhirServerIfNecessary: problem updating: " + organization.getIdElement().getValue());
                    succeeded = false;
                }
            }
        }

        return succeeded;
    }

    /**
     * Takes a biobank from FHIR and updates it with current information from the Directory.
     *
     * @param fhirBiobank the biobank to update.
     * @return the {@link OperationOutcome} from the FHIR server update
     */
    private boolean updateBiobankOnFhirServerIfNecessary(Organization fhirBiobank) {
        logger.info("updateBiobankOnFhirServerIfNecessary: entered");

        // Retrieve the biobank's BBMRI-ERIC identifier from the FHIR organization
        Optional<BbmriEricId> bbmriEricIdOpt = FhirApi.bbmriEricId(fhirBiobank);

        logger.info("updateBiobankOnFhirServerIfNecessary: bbmriEricIdOpt: " + bbmriEricIdOpt);

        // Check if the identifier is present, if not, return false
        if (!bbmriEricIdOpt.isPresent()) {
            logger.warn("updateBiobankOnFhirServerIfNecessary: Missing BBMRI-ERIC identifier");
            return false;
        }
        BbmriEricId bbmriEricId = bbmriEricIdOpt.get();

        logger.info("updateBiobankOnFhirServerIfNecessary: bbmriEricId: " + bbmriEricId);

        // Fetch the corresponding biobank from the Directory API
        Biobank directoryBiobank = directoryApi.fetchBiobank(bbmriEricId);

        logger.info("updateBiobankOnFhirServerIfNecessary: directoryBiobank: " + directoryBiobank);

        // Check if fetching the biobank was successful, if not, return false
        if (directoryBiobank == null) {
            logger.warn("updateBiobankOnFhirServerIfNecessary: Failed to fetch biobank from Directory API");
            return false;
        }

        logger.info("updateBiobankOnFhirServerIfNecessary: Create a BiobankTuple containing the FHIR biobank and the Directory biobank");

        // Create a BiobankTuple containing the FHIR biobank and the Directory biobank
        BiobankTuple biobankTuple = new BiobankTuple(fhirBiobank, directoryBiobank);

        logger.info("updateBiobankOnFhirServerIfNecessary: Update the biobank name if necessary");

        // Update the biobank name if necessary
        BiobankTuple updatedBiobankTuple = UPDATE_BIOBANK_NAME.apply(biobankTuple);

        logger.info("updateBiobankOnFhirServerIfNecessary: Check if any changes have been made; if not, return a no-update necessary outcome");

        // Check if any changes have been made; if not, return true (because this outcome is OK)
        if (!updatedBiobankTuple.hasChanged()) {
            logger.info("updateBiobankOnFhirServerIfNecessary: No update necessary");
            return true;
        }

        logger.info("updateBiobankOnFhirServerIfNecessary: Update the biobank resource on the FHIR server if changes were made");

        // Update the biobank resource on the FHIR server
        OperationOutcome updateOutcome = fhirApi.updateResource(updatedBiobankTuple.fhirBiobank);

        String errorMessage = Util.getErrorMessageFromOperationOutcome(updateOutcome);

        if (!errorMessage.isEmpty()) {
            logger.warn("updateBiobankOnFhirServerIfNecessary: Problem during FHIR store update");
            return false;
        }

        logger.info("updateBiobankOnFhirServerIfNecessary: done!");

        return true;
    }

    /**
     * Generates corrections to the diagnoses obtained from the FHIR store, to make them
     * compatible with the Directory. You should supply this method with an empty map
     * via the correctedDiagnoses variable. This map will be filled by the method and
     * you can subsequently use it elsewhere.
     * <p>
     * This method performs the following steps:
     * <p>
     * * Retrieves diagnoses from the FHIR store for specimens with identifiable collections and their associated patients.
     * * Converts raw ICD-10 codes into MIRIAM-compatible codes.
     * * Collects corrected diagnosis codes from the Directory API based on the MIRIAM-compatible codes.
     *
     * @param defaultCollectionId Default collection ID. May be null.
     * @return A list containing a single OperationOutcome indicating the success of the diagnosis corrections process.
     *         If any errors occur during the process, an OperationOutcome with error details is returned.
     */
    private List<OperationOutcome> generateDiagnosisCorrections(String defaultCollectionId) {
        correctedDiagnoses = new HashMap<String, String>();
        try {
            // Convert string version of collection ID into a BBMRI ERIC ID.
            BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                .valueOf(defaultCollectionId)
                .orElse(null);

            // Get all diagnoses from the FHIR store for specemins with identifiable
            // collections and their associated patients.
            List<String> fhirDiagnoses = fhirReporting.fetchDiagnoses(defaultBbmriEricCollectionId);
            if (fhirDiagnoses == null) {
                logger.warn("Problem getting diagnosis information from FHIR store");
            }
            logger.info("__________ generateDiagnosisCorrections: fhirDiagnoses.size(): " + fhirDiagnoses.size());

            // Convert the raw ICD 10 codes into MIRIAM-compatible codes and put the
            // codes into a map with identical keys and values.
            fhirDiagnoses.forEach(diagnosis -> {
                String miriamDiagnosis = FhirToDirectoryAttributeConverter.convertDiagnosis(diagnosis);
                correctedDiagnoses.put(miriamDiagnosis, miriamDiagnosis);
            });
            logger.info("__________ generateDiagnosisCorrections: 1 correctedDiagnoses.size(): " + correctedDiagnoses.size());

            // Get corrected diagnosis codes from the Directory
            directoryApi.collectDiagnosisCorrections(correctedDiagnoses);
            logger.info("__________ generateDiagnosisCorrections: 2 correctedDiagnoses.size(): " + correctedDiagnoses.size());

            // Return a successful outcome.
            OperationOutcome outcome = new OperationOutcome();
            outcome.addIssue().setSeverity(INFORMATION).setDiagnostics("Diagnosis corrections generated successfully");
            return Collections.singletonList(outcome);
        } catch (Exception e) {
            return Util.createErrorOutcome("generateDiagnosisCorrections - unexpected error: " + Util.traceFromException(e));
        }
    }

    /**
     * Sends updates for Star Model data to the Directory service, based on FHIR store information.
     * This method fetches Star Model input data from the FHIR store, generates star model fact tables,
     * performs diagnosis corrections, and then updates the Directory service with the prepared data.
     * <p>
     * The method handles errors by returning a list of OperationOutcome objects describing the issues.
     * </p>
     *
     * @param defaultCollectionId The default BBMRI-ERIC collection ID for fetching data from the FHIR store.
     * @param minDonors The minimum number of donors required for a fact to be included in the star model output.
     * @param maxFacts The maximum number of facts to be included in the star model output. Negative number means no limit.
     * @return A list of OperationOutcome objects indicating the outcome of the star model updates.
     *
     * @throws IllegalArgumentException if the defaultCollectionId is not a valid BbmriEricId.
     */
    private List<OperationOutcome> sendStarModelUpdatesToDirectory(String defaultCollectionId, int minDonors, int maxFacts) {
        logger.info("__________ sendStarModelUpdatesToDirectory: minDonors: " + minDonors);
        try {
            BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                .valueOf(defaultCollectionId)
                .orElse(null);

            // Pull data from the FHIR store and save it in a format suitable for generating
            // star model hypercubes.
            StarModelData starModelInputData = fhirReporting.fetchStarModelInputData(defaultBbmriEricCollectionId);
            if (starModelInputData == null)
                return Util.createErrorOutcome("Problem getting star model information from FHIR store");
            logger.info("__________ sendStarModelUpdatesToDirectory: number of collection IDs: " + starModelInputData.getInputCollectionIds().size());

            directoryApi.relogin();

            // Hypercubes containing less than the minimum number of donors will not be
            // included in the star model output.
            starModelInputData.setMinDonors(minDonors);

            // Take the patient list and the specimen list from starModelInputData and
            // use them to generate the star model fact tables.
            CreateFactTablesFromStarModelInputData.createFactTables(starModelInputData, maxFacts);
            logger.info("__________ sendStarModelUpdatesToDirectory: 1 starModelInputData.getFactCount(): " + starModelInputData.getFactCount());

            // Apply corrections to ICD 10 diagnoses, to make them compatible with
            // the Directory.
            if (correctedDiagnoses != null)
                starModelInputData.applyDiagnosisCorrections(correctedDiagnoses);
            logger.info("__________ sendStarModelUpdatesToDirectory: 2 starModelInputData.getFactCount(): " + starModelInputData.getFactCount());

            // Send fact tables to Direcory.
            directoryApi.relogin();
            List<OperationOutcome> starModelUpdateOutcome = Collections.singletonList(directoryApi.updateStarModel(starModelInputData));
            logger.info("__________ sendStarModelUpdatesToDirectory: star model has been updated");
            // Return some kind of results count or whatever
            return starModelUpdateOutcome;
        } catch (Exception e) {
            return Util.createErrorOutcome("sendStarModelUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
        }
    }
    
     /**
     * Take information from the FHIR store and send aggregated updates to the Directory.
     * <p>
     * This is a multi step process:
     *  1. Fetch a list of collections objects from the FHIR store. These contain aggregated
     *     information over all specimens in the collections.
     *  2. Convert the FHIR collection objects into Directory collection PUT DTOs. Copy
     *     over avaialble information from FHIR, converting where necessary.
     *  3. Using the collection IDs found in the FHIR store, send queries to the Directory
     *     and fetch back the relevant GET collections. If any of the collection IDs cannot be
     *     found, this ie a breaking error.
     *  4. Transfer data from the Directory GET collections to the corresponding Directory PUT
     *     collections.
     *  5. Push the new information back to the Directory.
     * 
     * @param defaultCollectionId The default collection ID to use for fetching collections from the FHIR store.
     * @return A list of OperationOutcome objects indicating the outcome of the update operation.
     */
     private List<OperationOutcome> sendUpdatesToDirectory(String defaultCollectionId) {
        try {
            BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                .valueOf(defaultCollectionId)
                .orElse(null);

            List<FhirCollection> fhirCollection = fhirReporting.fetchFhirCollections(defaultBbmriEricCollectionId);
            if (fhirCollection == null)
                return Util.createErrorOutcome("Problem getting collections from FHIR store");
            logger.info("__________ sendUpdatesToDirectory: FHIR collection count): " + fhirCollection.size());

            DirectoryCollectionPut directoryCollectionPut = FhirCollectionToDirectoryCollectionPutConverter.convert(fhirCollection);
            if (directoryCollectionPut == null) 
                return Util.createErrorOutcome("Problem converting FHIR attributes to Directory attributes");
            logger.info("__________ sendUpdatesToDirectory: 1 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());
    
            List<String> collectionIds = directoryCollectionPut.getCollectionIds();
            String countryCode = directoryCollectionPut.getCountryCode();
            directoryApi.relogin();
            DirectoryCollectionGet directoryCollectionGet = directoryApi.fetchCollectionGetOutcomes(countryCode, collectionIds);
            if (directoryCollectionGet == null)
                return Util.createErrorOutcome("Problem getting collections from Directory");
            logger.info("__________ sendUpdatesToDirectory: 1 directoryCollectionGet.getItems().size()): " + directoryCollectionGet.getItems().size());

            if (!MergeDirectoryCollectionGetToDirectoryCollectionPut.merge(directoryCollectionGet, directoryCollectionPut))
                return Util.createErrorOutcome("Problem merging Directory GET attributes to Directory PUT attributes");
            logger.info("__________ sendUpdatesToDirectory: 2 directoryCollectionGet.getItems().size()): " + directoryCollectionGet.getItems().size());
            
            // Apply corrections to ICD 10 diagnoses, to make them compatible with
            // the Directory.
            if (correctedDiagnoses != null)
                directoryCollectionPut.applyDiagnosisCorrections(correctedDiagnoses);
            logger.info("__________ sendUpdatesToDirectory: 2 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

            directoryApi.relogin();
            List<OperationOutcome> outcomes = Collections.singletonList(directoryApi.updateEntities(directoryCollectionPut));
            logger.info("__________ sendUpdatesToDirectory: 2 outcomes: " + outcomes);
            return outcomes;
        } catch (Exception e) {
            return Util.createErrorOutcome("sendUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
        }
    }
}
