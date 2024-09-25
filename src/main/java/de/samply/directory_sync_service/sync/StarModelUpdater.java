package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.CreateFactTablesFromStarModelInputData;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.fhir.PopulateStarModelInputData;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.StarModelData;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Update the star model data in the Directory with data from the local FHIR store.
 */
public class StarModelUpdater {
    private static final Logger logger = LoggerFactory.getLogger(StarModelUpdater.class);

    /**
     * Sends updates for Star Model data to the Directory service, based on FHIR store information.
     * This method fetches Star Model input data from the FHIR store, generates star model fact tables,
     * performs diagnosis corrections, and then updates the Directory service with the prepared data.
     * <p>
     * The method handles errors by returning a list of OperationOutcome objects describing the issues.
     * </p>
     * @param fhirApi
     * @param directoryApi
     * @param correctedDiagnoses
     * @param defaultCollectionId The default BBMRI-ERIC collection ID for fetching data from the FHIR store.
     * @param minDonors The minimum number of donors required for a fact to be included in the star model output.
     * @param maxFacts The maximum number of facts to be included in the star model output. Negative number means no limit.
     * @return A list of OperationOutcome objects indicating the outcome of the star model updates.
     *
     * @throws IllegalArgumentException if the defaultCollectionId is not a valid BbmriEricId.
     */
    public static boolean sendStarModelUpdatesToDirectory(FhirApi fhirApi, DirectoryApi directoryApi, Map<String, String> correctedDiagnoses, String defaultCollectionId, int minDonors, int maxFacts) {
        logger.info("__________ sendStarModelUpdatesToDirectory: minDonors: " + minDonors);
        try {
            BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                    .valueOf(defaultCollectionId)
                    .orElse(null);

            // Pull data from the FHIR store and save it in a format suitable for generating
            // star model hypercubes.
            StarModelData starModelInputData = (new PopulateStarModelInputData(fhirApi)).populate(defaultBbmriEricCollectionId);
            if (starModelInputData == null) {
                logger.warn("Problem getting star model information from FHIR store");
                return false;
            }
            logger.info("__________ sendStarModelUpdatesToDirectory: number of collection IDs: " + starModelInputData.getInputCollectionIds().size());

            directoryApi.login();

            // Hypercubes containing less than the minimum number of donors will not be
            // included in the star model output.
            starModelInputData.setMinDonors(minDonors);

            // Take the patient list and the specimen list from starModelInputData and
            // use them to generate the star model fact tables.
            CreateFactTablesFromStarModelInputData.createFactTables(starModelInputData, maxFacts);
            logger.info("__________ sendStarModelUpdatesToDirectory: 1 starModelInputData.getFactCount(): " + starModelInputData.getFactCount());

            // Apply corrections to ICD 10 diagnoses, to make them compatible with
            // the Directory.
            starModelInputData.applyDiagnosisCorrections(correctedDiagnoses);
            logger.info("__________ sendStarModelUpdatesToDirectory: 2 starModelInputData.getFactCount(): " + starModelInputData.getFactCount());

            // Send fact tables to Direcory.
            directoryApi.login();

            if (!directoryApi.updateStarModel(starModelInputData)) {
                logger.warn("sendStarModelUpdatesToDirectory: Problem during star model update");
                return false;
            }

            return true;
        } catch (Exception e) {
            logger.warn("sendStarModelUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
            return false;
        }
    }
}
