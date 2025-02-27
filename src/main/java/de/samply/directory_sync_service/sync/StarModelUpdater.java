package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import de.samply.directory_sync_service.directory.CreateFactTablesFromStarModelInputData;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.fhir.PopulateStarModelInputData;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.StarModelData;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Specimen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
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
        logger.info("sendStarModelUpdatesToDirectory: minDonors: " + minDonors);
        try {
            BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                    .valueOf(defaultCollectionId)
                    .orElse(null);

            // Pull data from the FHIR store and save it in a format suitable for generating
            // star model hypercubes.
            StarModelData starModelInputData = (new PopulateStarModelInputData(fhirApi)).populate(defaultBbmriEricCollectionId);
            if (starModelInputData == null) {
                logger.warn("sendStarModelUpdatesToDirectory: Problem getting star model information from FHIR store");
                return false;
            }
            logger.info("sendStarModelUpdatesToDirectory: number of collection IDs: " + starModelInputData.getInputCollectionIds().size());

            directoryApi.login();

            // Hypercubes containing less than the minimum number of donors will not be
            // included in the star model output.
            starModelInputData.setMinDonors(minDonors);

            // Take the patient list and the specimen list from starModelInputData and
            // use them to generate the star model fact tables.
            CreateFactTablesFromStarModelInputData.createFactTables(starModelInputData, maxFacts);
            logger.info("sendStarModelUpdatesToDirectory: 1 starModelInputData.getFactCount(): " + starModelInputData.getFactCount());
            if (starModelInputData.getFactCount() == 0) {
                logger.warn("sendStarModelUpdatesToDirectory: no starModelInputData has been generated, FHIR store might be empty");
                return true;
            }

            // Apply corrections to ICD 10 diagnoses, to make them compatible with
            // the Directory.
            starModelInputData.applyDiagnosisCorrections(correctedDiagnoses);
            logger.info("sendStarModelUpdatesToDirectory: 2 starModelInputData.getFactCount(): " + starModelInputData.getFactCount());

            // Send fact tables to Direcory.
            directoryApi.login();

            if (!directoryApi.updateStarModel(starModelInputData)) {
                logger.warn("sendStarModelUpdatesToDirectory: Problem during star model update");
                return false;
            }

            sampleCountSanityCheck(fhirApi, defaultBbmriEricCollectionId, starModelInputData);
            materialTypeSanityCheck(fhirApi, defaultBbmriEricCollectionId, starModelInputData);

            logger.info("sendStarModelUpdatesToDirectory: star model update successful");
            return true;
        } catch (Exception e) {
            logger.warn("sendStarModelUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
            return false;
        }
    }

    /**
     * Sanity check: compare sample counts from source data with sample counts derived from star model.
     *
     * @param fhirApi
     * @param defaultBbmriEricCollectionId
     * @param starModelInputData
     */
    private static void sampleCountSanityCheck(FhirApi fhirApi, BbmriEricId defaultBbmriEricCollectionId, StarModelData starModelInputData) {
        int totalFhirSpecimenCount = fhirApi.calculateTotalSpecimenCount(defaultBbmriEricCollectionId);
        int totalStarModelSpecimenCount = 0;
        for (Map<String, String> factTable: starModelInputData.getFactTables())
            if (factTable.containsKey("number_of_samples")) {
                int numberOfSamples = Integer.parseInt(factTable.get("number_of_samples"));
                totalStarModelSpecimenCount += numberOfSamples;
                if (numberOfSamples > 1)
                    logger.debug("sampleCountSanityCheck: number of samples: " + numberOfSamples);
            }
        if (totalFhirSpecimenCount < totalStarModelSpecimenCount)
            logger.warn("sampleCountSanityCheck: !!!!!!! FHIR sample count (" + totalFhirSpecimenCount + ") is less than star model sample count (" + totalStarModelSpecimenCount + ")");
        logger.debug("sendStarModelUpdatesToDirectory: totalFhirSpecimenCount: " + totalFhirSpecimenCount);
        logger.debug("sendStarModelUpdatesToDirectory: totalStarModelSpecimenCount: " + totalStarModelSpecimenCount);
    }

    /**
     * Sanity check: do the material types in the star model match the material types in the FHIR store?
     *
     * @param fhirApi
     * @param defaultBbmriEricCollectionId
     * @param starModelInputData
     */
    private static void materialTypeSanityCheck(FhirApi fhirApi, BbmriEricId defaultBbmriEricCollectionId, StarModelData starModelInputData) {
        Map<String, String> fhirSampleMaterials = fhirApi.getSampleMaterials(defaultBbmriEricCollectionId);
        Map<String,String> convertedFhirSampleMaterials = new HashMap<>();
        for (String sampleMaterial: fhirSampleMaterials.keySet()) {
            String convertedSampleMaterial = FhirToDirectoryAttributeConverter.convertMaterial(sampleMaterial);
            if (convertedSampleMaterial != null && !convertedFhirSampleMaterials.containsKey(convertedSampleMaterial))
                convertedFhirSampleMaterials.put(convertedSampleMaterial, convertedSampleMaterial);
        }
        int totalStarModelSampleMaterialCount = 0;
        Map<String,String> starModelSampleMaterials = new HashMap<>();
        for (Map<String, String> factTable: starModelInputData.getFactTables())
            if (factTable.containsKey("sample_material")) {
                String sampleMaterial = factTable.get("sample_material");
                if (!starModelSampleMaterials.containsKey(sampleMaterial)) {
                    totalStarModelSampleMaterialCount++;
                    starModelSampleMaterials.put(sampleMaterial, sampleMaterial);}
            } else if (totalStarModelSampleMaterialCount == 0 && starModelInputData.getFactCount() > 0) {
                logger.warn("sendStarModelUpdatesToDirectory: !!!!!!! did not find sample_material in fact 0: " + Util.jsonStringFomObject(starModelInputData.getFactTables().get(0)));
            }
        if (convertedFhirSampleMaterials.size() != totalStarModelSampleMaterialCount && starModelInputData.getFactTables().size() != 0) {
            logger.warn("sendStarModelUpdatesToDirectory: !!!!!!! converted FHIR material type count (" + convertedFhirSampleMaterials.size() + ") is different from star model material type count (" + totalStarModelSampleMaterialCount + ")");
            logger.warn("sendStarModelUpdatesToDirectory: FHIR material types: " + Util.orderedKeylistFromMap(fhirSampleMaterials));
            logger.warn("sendStarModelUpdatesToDirectory: converted FHIR material types: " + Util.orderedKeylistFromMap(convertedFhirSampleMaterials));
            logger.warn("sendStarModelUpdatesToDirectory: star model material types: " + Util.orderedKeylistFromMap(starModelSampleMaterials));
            logger.warn("sendStarModelUpdatesToDirectory: star model material fact count: " + starModelInputData.getFactCount());
        }
    }
}
