package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.CreateFactTablesFromStarModelInputData;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.model.FactTable;
import de.samply.directory_sync_service.model.StarModelInput;
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
     * @param directoryApi
     * @param correctedDiagnoses
     * @param starModelInput Data in a format suitable for generating star model hypercubes.
     * @param minDonors The minimum number of donors required for a fact to be included in the star model output.
     * @param maxFacts The maximum number of facts to be included in the star model output. Negative number means no limit.
     * @return A list of OperationOutcome objects indicating the outcome of the star model updates.
     *
     * @throws IllegalArgumentException if the defaultCollectionId is not a valid BbmriEricId.
     */
    public static FactTable sendStarModelUpdatesToDirectory(DirectoryApi directoryApi, Map<String, String> correctedDiagnoses, StarModelInput starModelInput, int minDonors, int maxFacts) {
        logger.debug("sendStarModelUpdatesToDirectory: minDonors: " + minDonors);
        try {
            directoryApi.login();

            // Hypercubes containing less than the minimum number of donors will not be
            // included in the star model output.
            starModelInput.setMinDonors(minDonors);

            // Take the patient list and the specimen list from starModelInputData and
            // use them to generate the star model fact tables.
            FactTable factTable = CreateFactTablesFromStarModelInputData.createFactTables(starModelInput, maxFacts);
            logger.debug("sendStarModelUpdatesToDirectory: 1 starModelInputData.getFactCount(): " + factTable.getFactCount());
            if (factTable.getFactCount() == 0)
                logger.warn("sendStarModelUpdatesToDirectory: no starModelInputData has been generated, FHIR store might be empty");
            logger.info("sendStarModelUpdatesToDirectory: after creation, fact count: " + factTable.getFactCount());

            // Apply corrections to ICD 10 diagnoses, to make them compatible with
            // the Directory.
            factTable.applyDiagnosisCorrections(correctedDiagnoses);
            logger.info("sendStarModelUpdatesToDirectory: after diagnosis corrections, fact count: " + factTable.getFactCount());

            // Send fact tables to Direcory.
            directoryApi.login();

            if (!directoryApi.updateStarModel(factTable, starModelInput.getInputCollectionIds())) {
                logger.warn("sendStarModelUpdatesToDirectory: Problem during star model update");
                return null;
            }

            logger.info("sendStarModelUpdatesToDirectory: star model update successful");
            return factTable;
        } catch (Exception e) {
            logger.warn("sendStarModelUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
            return null;
        }
    }
}
