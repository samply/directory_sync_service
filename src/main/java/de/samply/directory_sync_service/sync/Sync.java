package de.samply.directory_sync_service.sync;

import ca.uhn.fhir.context.FhirContext;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirCollectionToDirectoryCollectionPutConverter;
import de.samply.directory_sync_service.directory.CreateFactTablesFromStarModelInputData;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.DirectoryService;
import de.samply.directory_sync_service.directory.MergeDirectoryCollectionGetToDirectoryCollectionPut;
import de.samply.directory_sync_service.directory.model.BbmriEricId;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.fhir.FhirReporting;
import de.samply.directory_sync_service.fhir.model.FhirCollection;
import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import de.samply.directory_sync_service.model.StarModelData;
import io.vavr.control.Either;
import io.vavr.control.Option;
import java.util.Map;
import java.util.Objects;

import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.INFORMATION;

/**
 * Provides functionality to synchronize between a BBMRI Directory instance and a FHIR store in both directions.
 * This class provides methods to update biobanks, synchronize collection sizes, generate diagnosis corrections,
 * send star model updates, and perform aggregated updates to the Directory service based on information from
 * the FHIR store.
 * <p>
 * Usage:
 * <p>
 * You will need to first do the initial set up of the Sync class, which includes
 * connecting to the FHIR store and to the Directory. Your code might look something like
 * this:
 * <p>
 * DirectoryService directoryService = new DirectoryService(DirectoryApi.createWithLogin(HttpClients.createDefault(), directoryUrl, directoryUserName, directoryPassCode));
 * FhirReporting fhirReporting = new FhirReporting(ctx, fhirApi);
 * Sync sync = new Sync(fhirApi, fhirReporting, directoryApi, directoryService);
 * sync.initResources()
 * <p>
 * Next, if your FHIR store does not use WHO ICD 10 codes for diagnosis, you should
 * first generate a map, mapping your local ICD 10 codes onto WHO, which are used by
 * the Directory:
 * <p>
 * sync.generateDiagnosisCorrections(directoryDefaultCollectionId); // directoryDefaultCollectionId may be null
 * <p>
 * Now you can start to do some synchronization, e.g.:
 * <p>
 * Only send the collection sizes to the Directory (deprecated):
 * sync.syncCollectionSizesToDirectory
 * <p>
 * Send all standard attributes to Directory:
 * sync.sendUpdatesToDirectory(directoryDefaultCollectionId);
 * <p>
 * Send star model to Directory:
 * sync.sendStarModelUpdatesToDirectory(directoryDefaultCollectionId, directoryMinDonors); // e.g. directoryMinDonors=10
 * <p>
 * Get biobank information from Directory and put into local FHIR store:
 * sync.updateAllBiobanksOnFhirServerIfNecessary();
 */
public class Sync {
  private static final Logger logger = LoggerFactory.getLogger(Sync.class);

    private static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_NAME = t -> {
        t.fhirBiobank.setName(t.dirBiobank.getName());
        return t;
    };

    private final FhirApi fhirApi;
    private final FhirReporting fhirReporting;
    private DirectoryApi directoryApi;
    private final DirectoryService directoryService;

    public Sync(FhirApi fhirApi, FhirReporting fhirReporting, DirectoryApi directoryApi,
        DirectoryService directoryService) {
        this.fhirApi = fhirApi;
        this.fhirReporting = fhirReporting;
        this.directoryApi = directoryApi;
        this.directoryService = directoryService;
    }

    public static void main(String[] args) {
        FhirContext fhirContext = FhirContext.forR4();
        FhirApi fhirApi = new FhirApi(fhirContext.newRestfulGenericClient(args[0]));
        FhirReporting fhirReporting = new FhirReporting(fhirContext, fhirApi);
        Sync sync = new Sync(fhirApi, fhirReporting, null, null);
        Either<String, Void> result = sync.initResources();
        System.out.println("result = " + result);
        Either<OperationOutcome, Map<BbmriEricId, Integer>> collectionSizes = fhirReporting.fetchCollectionSizes();
        System.out.println("collectionSizes = " + collectionSizes);
    }

    /**
     * Initializes necessary resources for the synchronization process.
     *
     * @return An {@link Either} containing either a success message or an error message.
     */
    public Either<String, Void> initResources() {
        logger.info("initResources: Initializes necessary resources for the synchronization process");
        return fhirReporting.initLibrary().flatMap(_void -> fhirReporting.initMeasure());
    }

    private static OperationOutcome missingIdentifierOperationOutcome() {
        OperationOutcome outcome = new OperationOutcome();
        outcome.addIssue().setSeverity(ERROR).setDiagnostics("No BBMRI Identifier for Organization");
        return outcome;
    }

    private static OperationOutcome noUpdateNecessaryOperationOutcome() {
        OperationOutcome outcome = new OperationOutcome();
        outcome.addIssue().setSeverity(INFORMATION).setDiagnostics("No Update " +
                "necessary");
        return outcome;
    }

    /**
     * Updates all biobanks from the FHIR server with information from the Directory.
     *
     * @return the individual {@link OperationOutcome}s from each update
     */
    public List<OperationOutcome> updateAllBiobanksOnFhirServerIfNecessary() {
        return fhirApi.listAllBiobanks()
                .map(orgs -> orgs.stream().map(this::updateBiobankOnFhirServerIfNecessary).collect(Collectors.toList()))
                .fold(Collections::singletonList, Function.identity());
    }

//    /**
//     * Takes a biobank from FHIR and updates it with current information from the Directory.
//     *
//     * @param fhirBiobank the biobank to update.
//     * @return the {@link OperationOutcome} from the FHIR server update
//     */
//    OperationOutcome updateBiobankOnFhirServerIfNecessary(Organization fhirBiobank) {
//        return Option.ofOptional(FhirApi.bbmriEricId(fhirBiobank))
//                .toEither(missingIdentifierOperationOutcome())
//                .flatMap(directoryApi::fetchBiobank)
//                .map(dirBiobank -> new BiobankTuple(fhirBiobank, dirBiobank))
//                .map(UPDATE_BIOBANK_NAME)
//                .filterOrElse(BiobankTuple::hasChanged, tuple -> noUpdateNecessaryOperationOutcome())
//                .map(tuple -> fhirApi.updateResource(tuple.fhirBiobank))
//                .fold(Function.identity(), Function.identity());
//    }
    /**
     * Takes a biobank from FHIR and updates it with current information from the Directory.
     *
     * @param fhirBiobank the biobank to update.
     * @return the {@link OperationOutcome} from the FHIR server update
     */
    OperationOutcome updateBiobankOnFhirServerIfNecessary(Organization fhirBiobank) {
        logger.info("updateBiobankOnFhirServerIfNecessary: Step 1: Retrieve the biobank's BBMRI-ERIC identifier from the FHIR organization");

        // Step 1: Retrieve the biobank's BBMRI-ERIC identifier from the FHIR organization
        Optional<BbmriEricId> bbmriEricIdOpt = FhirApi.bbmriEricId(fhirBiobank);

        logger.info("updateBiobankOnFhirServerIfNecessary: Step 2: Convert the Optional to an Either type");

        // Step 2: Convert the Optional to an Either type
        Either<OperationOutcome, BbmriEricId> bbmriEricIdEither = Option.ofOptional(bbmriEricIdOpt)
                .toEither(missingIdentifierOperationOutcome());

        logger.info("updateBiobankOnFhirServerIfNecessary: Step 3: Fetch the corresponding biobank from the Directory API");

        // Step 3: Fetch the corresponding biobank from the Directory API
        Either<OperationOutcome, Biobank> dirBiobankEither = bbmriEricIdEither
                .flatMap(directoryApi::fetchBiobank);

        logger.info("updateBiobankOnFhirServerIfNecessary: Step 4: Create a BiobankTuple containing the FHIR biobank and the Directory biobank");

        // Step 4: Create a BiobankTuple containing the FHIR biobank and the Directory biobank
        Either<OperationOutcome, BiobankTuple> biobankTupleEither = dirBiobankEither
                .map(dirBiobank -> new BiobankTuple(fhirBiobank, dirBiobank));

        logger.info("updateBiobankOnFhirServerIfNecessary: Step 5: Update the biobank name if necessary");

        // Step 5: Update the biobank name if necessary
        Either<OperationOutcome, BiobankTuple> updatedBiobankTupleEither = biobankTupleEither
                .map(UPDATE_BIOBANK_NAME);

        logger.info("updateBiobankOnFhirServerIfNecessary: Step 6: Check if any changes have been made; if not, return a no-update necessary outcome");

        // Step 6: Check if any changes have been made; if not, return a no-update necessary outcome
        Either<OperationOutcome, BiobankTuple> finalBiobankTupleEither = updatedBiobankTupleEither
                .filterOrElse(BiobankTuple::hasChanged, tuple -> noUpdateNecessaryOperationOutcome());

        logger.info("updateBiobankOnFhirServerIfNecessary: Step 7: Update the biobank resource on the FHIR server if changes were made");

        // Step 7: Update the biobank resource on the FHIR server if changes were made
        Either<OperationOutcome, OperationOutcome> updateOutcomeEither = finalBiobankTupleEither
                .map(tuple -> fhirApi.updateResource(tuple.fhirBiobank));

        logger.info("updateBiobankOnFhirServerIfNecessary: Step 8: Return the result of the update, folding Either to an OperationOutcome");

        // Step 8: Return the result of the update, folding Either to an OperationOutcome
        return updateOutcomeEither.fold(Function.identity(), Function.identity());
    }

    private Map<String, String> correctedDiagnoses = null;

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
    public List<OperationOutcome> generateDiagnosisCorrections(String defaultCollectionId) {
        correctedDiagnoses = new HashMap<String, String>();
        try {
            // Convert string version of collection ID into a BBMRI ERIC ID.
            BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                .valueOf(defaultCollectionId)
                .orElse(null);

            // Get all diagnoses from the FHIR store for specemins with identifiable
            // collections and their associated patients.
            Either<OperationOutcome, List<String>> fhirDiagnosesOutcome = fhirReporting.fetchDiagnoses(defaultBbmriEricCollectionId);
            if (fhirDiagnosesOutcome.isLeft())
                return createErrorOutcome("Problem getting diagnosis information from FHIR store, " + errorMessageFromOperationOutcome(fhirDiagnosesOutcome.getLeft()));
            List<String> fhirDiagnoses = fhirDiagnosesOutcome.get();
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
            return createErrorOutcome("generateDiagnosisCorrections - unexpected error: " + Util.traceFromException(e));
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
    public List<OperationOutcome> sendStarModelUpdatesToDirectory(String defaultCollectionId, int minDonors, int maxFacts) {
        logger.info("__________ sendStarModelUpdatesToDirectory: minDonors: " + minDonors);
        try {
            BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                .valueOf(defaultCollectionId)
                .orElse(null);

            // Pull data from the FHIR store and save it in a format suitable for generating
            // star model hypercubes.
            Either<OperationOutcome, StarModelData> starModelInputDataOutcome = fhirReporting.fetchStarModelInputData(defaultBbmriEricCollectionId);
            if (starModelInputDataOutcome.isLeft())
                return createErrorOutcome("Problem getting star model information from FHIR store, " + errorMessageFromOperationOutcome(starModelInputDataOutcome.getLeft()));
            StarModelData starModelInputData = starModelInputDataOutcome.get();
            logger.info("__________ sendStarModelUpdatesToDirectory: number of collection IDs: " + starModelInputData.getInputCollectionIds().size());

            relogin();

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
            relogin();
            List<OperationOutcome> starModelUpdateOutcome = directoryService.updateStarModel(starModelInputData);
            logger.info("__________ sendStarModelUpdatesToDirectory: star model has been updated");
            // Return some kind of results count or whatever
            return starModelUpdateOutcome;
        } catch (Exception e) {
            return createErrorOutcome("sendStarModelUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
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
    public List<OperationOutcome> sendUpdatesToDirectory(String defaultCollectionId) {
        try {
            BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                .valueOf(defaultCollectionId)
                .orElse(null);

            Either<OperationOutcome, List<FhirCollection>> fhirCollectionOutcomes = fhirReporting.fetchFhirCollections(defaultBbmriEricCollectionId);
            if (fhirCollectionOutcomes.isLeft())
                return createErrorOutcome("Problem getting collections from FHIR store, " + errorMessageFromOperationOutcome(fhirCollectionOutcomes.getLeft()));
            logger.info("__________ sendUpdatesToDirectory: FHIR collection count): " + fhirCollectionOutcomes.get().size());

            DirectoryCollectionPut directoryCollectionPut = FhirCollectionToDirectoryCollectionPutConverter.convert(fhirCollectionOutcomes.get());
            if (directoryCollectionPut == null) 
                return createErrorOutcome("Problem converting FHIR attributes to Directory attributes");
            logger.info("__________ sendUpdatesToDirectory: 1 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());
    
            List<String> collectionIds = directoryCollectionPut.getCollectionIds();
            String countryCode = directoryCollectionPut.getCountryCode();
            relogin();
            Either<OperationOutcome, DirectoryCollectionGet> directoryCollectionGetOutcomes = directoryService.fetchDirectoryCollectionGetOutcomes(countryCode, collectionIds);
            if (directoryCollectionGetOutcomes.isLeft())
                return createErrorOutcome("Problem getting collections from Directory, " + errorMessageFromOperationOutcome(directoryCollectionGetOutcomes.getLeft()));
            DirectoryCollectionGet directoryCollectionGet = directoryCollectionGetOutcomes.get();
            logger.info("__________ sendUpdatesToDirectory: 1 directoryCollectionGet.getItems().size()): " + directoryCollectionGet.getItems().size());

            if (!MergeDirectoryCollectionGetToDirectoryCollectionPut.merge(directoryCollectionGet, directoryCollectionPut))
                return createErrorOutcome("Problem merging Directory GET attributes to Directory PUT attributes");
            logger.info("__________ sendUpdatesToDirectory: 2 directoryCollectionGet.getItems().size()): " + directoryCollectionGet.getItems().size());
            
            // Apply corrections to ICD 10 diagnoses, to make them compatible with
            // the Directory.
            if (correctedDiagnoses != null)
                directoryCollectionPut.applyDiagnosisCorrections(correctedDiagnoses);
            logger.info("__________ sendUpdatesToDirectory: 2 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

            relogin();
            List<OperationOutcome> outcomes = directoryService.updateEntities(directoryCollectionPut);
            logger.info("__________ sendUpdatesToDirectory: 2 outcomes: " + outcomes);
            return outcomes;
        } catch (Exception e) {
            return createErrorOutcome("sendUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
        }
    }

    /**
     * Renew the Directory login.
     * <p>
     * This generates a new DirectoryApi object, which needs to be distributed to the places
     * where it will be used.
     * <p>
     * Consequence: this method has significant side effects.
     */
    private void relogin() {
        directoryApi.relogin();
    }

    private String errorMessageFromOperationOutcome(OperationOutcome operationOutcome) {
        return operationOutcome.getIssue().stream()
                .filter(issue -> issue.getSeverity() == OperationOutcome.IssueSeverity.ERROR || issue.getSeverity() == OperationOutcome.IssueSeverity.FATAL)
                .map(OperationOutcome.OperationOutcomeIssueComponent::getDiagnostics)
                .collect(Collectors.joining("\n"));
    }
    
    private List<OperationOutcome> createErrorOutcome(String diagnostics) {
        OperationOutcome outcome = new OperationOutcome();
        outcome.addIssue().setSeverity(ERROR).setDiagnostics(diagnostics);
        return Collections.singletonList(outcome);
    }
    
    private static class BiobankTuple {

        private final Organization fhirBiobank;
        private final Organization fhirBiobankCopy;
        private final Biobank dirBiobank;

        private BiobankTuple(Organization fhirBiobank, Biobank dirBiobank) {
            this.fhirBiobank = Objects.requireNonNull(fhirBiobank);
            this.fhirBiobankCopy = fhirBiobank.copy();
            this.dirBiobank = Objects.requireNonNull(dirBiobank);
        }

        private boolean hasChanged() {
            return !fhirBiobank.equalsDeep(fhirBiobankCopy);
        }
    }
}
