package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import de.samply.directory_sync_service.directory.CreateFactTablesFromStarModelInputData;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.fhir.PopulateStarModelInputData;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.StarModelData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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
     * @param starModelInputData Data in a format suitable for generating star model hypercubes.
     * @param minDonors The minimum number of donors required for a fact to be included in the star model output.
     * @param maxFacts The maximum number of facts to be included in the star model output. Negative number means no limit.
     * @return A list of OperationOutcome objects indicating the outcome of the star model updates.
     *
     * @throws IllegalArgumentException if the defaultCollectionId is not a valid BbmriEricId.
     */
    public static boolean sendStarModelUpdatesToDirectory(DirectoryApi directoryApi, Map<String, String> correctedDiagnoses, StarModelData starModelInputData, int minDonors, int maxFacts) {
        logger.debug("sendStarModelUpdatesToDirectory: minDonors: " + minDonors);
        try {
            directoryApi.login();

            // Hypercubes containing less than the minimum number of donors will not be
            // included in the star model output.
            starModelInputData.setMinDonors(minDonors);

            // Take the patient list and the specimen list from starModelInputData and
            // use them to generate the star model fact tables.
            CreateFactTablesFromStarModelInputData.createFactTables(starModelInputData, maxFacts);
            logger.debug("sendStarModelUpdatesToDirectory: 1 starModelInputData.getFactCount(): " + starModelInputData.getFactCount());
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

            logger.info("sendStarModelUpdatesToDirectory: star model update successful");
            return true;
        } catch (Exception e) {
            logger.warn("sendStarModelUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
            return false;
        }
    }
//
//    /**
//     * Sanity check: compare sample counts from source data with sample counts derived from star model.
//     *
//     * @param fhirApi
//     * @param directoryDefaultCollectionId
//     * @param starModelInputData
//     */
//    public static void sampleCountSanityCheck(FhirApi fhirApi, String directoryDefaultCollectionId, StarModelData starModelInputData) {
//        BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
//                        .valueOf(directoryDefaultCollectionId)
//                        .orElse(null);
//        int totalFhirSpecimenCount = fhirApi.calculateTotalSpecimenCount(defaultBbmriEricCollectionId);
//        int totalStarModelSpecimenCount = 0;
//        for (Map<String, String> factTable: starModelInputData.getFactTables())
//            if (factTable.containsKey("number_of_samples")) {
//                int numberOfSamples = Integer.parseInt(factTable.get("number_of_samples"));
//                totalStarModelSpecimenCount += numberOfSamples;
//            }
//        if (totalFhirSpecimenCount < totalStarModelSpecimenCount)
//            logger.warn("sampleCountSanityCheck: !!!!!!!!!!!!!!!!!!!!! FHIR sample count (" + totalFhirSpecimenCount + ") is less than star model sample count (" + totalStarModelSpecimenCount + ")");
//        logger.debug("sampleCountSanityCheck: totalFhirSpecimenCount: " + totalFhirSpecimenCount);
//        logger.debug("sampleCountSanityCheck: totalStarModelSpecimenCount: " + totalStarModelSpecimenCount);
//    }
//
//    static int starModelMaterialTypeMissingCount = 0;
//    /**
//     * Sanity check: do the material types in the star model match the material types in the FHIR store?
//     *
//     * @param fhirApi
//     * @param directoryDefaultCollectionId
//     * @param starModelInputData
//     */
//    public static void materialTypeSanityCheck(FhirApi fhirApi, String directoryDefaultCollectionId, StarModelData starModelInputData) {
//        BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
//                .valueOf(directoryDefaultCollectionId)
//                .orElse(null);
//        Map<String, String> fhirSampleMaterials = fhirApi.getSampleMaterials(defaultBbmriEricCollectionId);
//        Map<String,String> convertedFhirSampleMaterials = new HashMap<>();
//        for (String sampleMaterial: fhirSampleMaterials.keySet()) {
//            String convertedSampleMaterial = FhirToDirectoryAttributeConverter.convertMaterial(sampleMaterial);
//            if (convertedSampleMaterial != null && !convertedFhirSampleMaterials.containsKey(convertedSampleMaterial))
//                convertedFhirSampleMaterials.put(convertedSampleMaterial, convertedSampleMaterial);
//        }
//        int totalStarModelSampleMaterialCount = 0;
//        Map<String,String> starModelSampleMaterials = new HashMap<>();
//        for (Map<String, String> factTable: starModelInputData.getFactTables())
//            if (factTable.containsKey("sample_type")) {
//                String sampleMaterial = factTable.get("sample_type");
//                if (!starModelSampleMaterials.containsKey(sampleMaterial)) {
//                    totalStarModelSampleMaterialCount++;
//                    starModelSampleMaterials.put(sampleMaterial, sampleMaterial);
//                }
//            } else if (starModelMaterialTypeMissingCount++ < 5)
//                logger.warn("materialTypeSanityCheck: fact table does not contain sample_type: " + Util.jsonStringFomObject(factTable));
//        if (convertedFhirSampleMaterials.size() != totalStarModelSampleMaterialCount && starModelInputData.getFactTables().size() != 0) {
//            logger.warn("materialTypeSanityCheck: !!!!!!!!!!!!!!!!!!!!! converted FHIR material type count (" + convertedFhirSampleMaterials.size() + ") is different from star model material type count (" + totalStarModelSampleMaterialCount + ")");
//            logger.warn("materialTypeSanityCheck: FHIR material types: " + Util.orderedKeylistFromMap(fhirSampleMaterials));
//            logger.warn("materialTypeSanityCheck: converted FHIR material types: " + Util.orderedKeylistFromMap(convertedFhirSampleMaterials));
//            logger.warn("materialTypeSanityCheck: star model material types: " + Util.orderedKeylistFromMap(starModelSampleMaterials));
//            logger.warn("materialTypeSanityCheck: star model material fact count: " + starModelInputData.getFactCount());
//        }
//    }
//
//    /**
//     * Sanity check: is the disease count less than the fact table count?
//     *
//     * @param starModelInputData
//     */
//    public static void diseaseSanityCheck(StarModelData starModelInputData) {
//        int diseaseCount = 0;
//        for (Map<String, String> factTable: starModelInputData.getFactTables())
//            if (factTable.containsKey("disease"))
//                diseaseCount++;
//        if (diseaseCount < starModelInputData.getFactTables().size())
//            logger.warn("diseaseSanityCheck: !!!!!!!!!!!!!!!!!!!!! disease count (" + diseaseCount + ") is different from fact table count (" + starModelInputData.getFactCount() + ")");
//    }
}
