package de.samply.directory_sync_service.model;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import de.samply.directory_sync_service.fhir.FhirApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * STAR model data in a format that is ready to be exported to the Directory.
 */
public class FactTable {
    private static final Logger logger = LoggerFactory.getLogger(FactTable.class);

    // *** Miscellaneous data

    // Minimum number of donors per fact
    private int minDonors = 10; // default value

    /**
     * Gets the current minimum number of donors required per fact.
     *
     * @return The minimum number of donors per fact.
     */
    public int getMinDonors() {
        return minDonors;
    }

    /**
     * Sets the minimum number of donors required per fact.
     *
     * @param minDonors The new minimum number of donors per fact to be set.
     */
    public void setMinDonors(int minDonors) {
        this.minDonors = minDonors;
    }

    // One big fact table for everything. Every fact contains a mandatory collection ID.
    // A single "fact" is a simple String map, with medically relevant keys.
    private final List<Map<String, String>> factTables = new ArrayList<Map<String, String>>();

    /**
     * Adds a collection of facts to the existing fact table.
     *
     * @param factTable The list of facts represented as String maps to be added to the fact table.
     */
    public void addFactTable(List<Map<String, String>> factTable) {
        factTables.addAll(factTable);
    }

    /**
     * Retrieves the entire fact table containing medically relevant information.
     *
     * @return The list of facts represented as String maps with mandatory collection ID.
     */
    public List<Map<String, String>> getFactTables() {
        return factTables;
    }
    
    /**
     * Gets the count of facts in the fact table.
     *
     * @return The number of facts present in the fact table.
     */
    public int getFactCount() {
        return factTables.size();
    }

    /**
     * Implements diagnosis corrections for the facts in the factTables.
     * For each fact in the factTables, checks if it contains a "disease" key.
     * If the "disease" key is present, it attempts to replace its value with
     * the corrected diagnosis from the diagnoses map. If the corrected diagnosis
     * is found, it updates the fact's "disease" value; otherwise, it removes
     * the "disease" key from the fact.
     * <p>
     * Note: This method directly modifies the factTables in-place.
     * 
     * @param diagnoses Maps FHIR diagnoses onto Directory diagnoses. If null, no corrections are applied.
     * @throws NullPointerException if diagnoses or any fact in factTables is null.
     */
    public void applyDiagnosisCorrections(Map<String,String> diagnoses) {
        if (diagnoses == null)
            return;
         for (Map<String, String> fact: factTables) {
            if (!fact.containsKey("disease"))
                continue;
            String disease = fact.get("disease");
            if (disease != null && diagnoses.containsKey(disease))
                fact.put("disease", diagnoses.get(disease));
            if (fact.get("disease") == null)
                fact.remove("disease");
        }
    }

    /**
     * Gets the country code for the collections, e.g. "DE".
     * <p>
     * Assumes that all collections will have the same code and simply returns
     * the code of the first collection.
     * <p>
     * If there are no collections, returns null.
     * <p>
     * May throw a null pointer exception.
     * 
     * @return Country code
     */
    public String getCountryCode() {
        if (factTables == null || factTables.size() == 0)
            return null;

        String countryCode = BbmriEricId
                .valueOf(factTables.get(0).get("collection"))
                .orElse(null)
                .getCountryCode();

        return countryCode;
    }

    /**
     * Runs sanity checks on the star model data.
     *
     * @param fhirApi
     * @param directoryDefaultCollectionId
     */
    public void runSanityChecks(FhirApi fhirApi, String directoryDefaultCollectionId) {
        sampleCountSanityCheck(fhirApi, directoryDefaultCollectionId);
        materialTypeSanityCheck(fhirApi, directoryDefaultCollectionId);
        diseaseSanityCheck();
    }

    /**
     * Sanity check: compare sample counts from source data with sample counts derived from star model.
     *
     * @param fhirApi
     * @param directoryDefaultCollectionId
     */
    public void sampleCountSanityCheck(FhirApi fhirApi, String directoryDefaultCollectionId) {
        BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                .valueOf(directoryDefaultCollectionId)
                .orElse(null);
        int totalFhirSpecimenCount = fhirApi.calculateTotalSpecimenCount(defaultBbmriEricCollectionId);
        int totalStarModelSpecimenCount = 0;
        for (Map<String, String> factTable: getFactTables())
            if (factTable.containsKey("number_of_samples")) {
                int numberOfSamples = Integer.parseInt(factTable.get("number_of_samples"));
                totalStarModelSpecimenCount += numberOfSamples;
            }
        if (totalFhirSpecimenCount < totalStarModelSpecimenCount)
            logger.info("sampleCountSanityCheck: !!!!!!!!!!!!!!!!!!!!! FHIR sample count (" + totalFhirSpecimenCount + ") is less than star model sample count (" + totalStarModelSpecimenCount + ")");
        if (totalStarModelSpecimenCount < totalFhirSpecimenCount)
            logger.info("sampleCountSanityCheck: !!!!!!!!!!!!!!!!!!!!! star model sample count (" + totalStarModelSpecimenCount + ") is less than FHIR sample count (" + totalFhirSpecimenCount + ")");
        if (totalStarModelSpecimenCount < (totalFhirSpecimenCount * 0.8))
            logger.warn("sampleCountSanityCheck: !!!!!!!!!!!!!!!!!!!!! star model sample count (" + totalStarModelSpecimenCount + ") is much less than FHIR sample count (" + totalFhirSpecimenCount + ")");
        logger.debug("sampleCountSanityCheck: totalFhirSpecimenCount: " + totalFhirSpecimenCount);
        logger.debug("sampleCountSanityCheck: totalStarModelSpecimenCount: " + totalStarModelSpecimenCount);
    }

    static int starModelMaterialTypeMissingCount = 0;
    /**
     * Sanity check: do the material types in the star model match the material types in the FHIR store?
     *
     * @param fhirApi
     * @param directoryDefaultCollectionId
     */
    public void materialTypeSanityCheck(FhirApi fhirApi, String directoryDefaultCollectionId) {
        BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                .valueOf(directoryDefaultCollectionId)
                .orElse(null);
        Map<String, String> fhirSampleMaterials = fhirApi.getSampleMaterials(defaultBbmriEricCollectionId);
        Map<String,String> convertedFhirSampleMaterials = new HashMap<>();
        for (String sampleMaterial: fhirSampleMaterials.keySet()) {
            String convertedSampleMaterial = FhirToDirectoryAttributeConverter.convertMaterial(sampleMaterial);
            if (convertedSampleMaterial != null && !convertedFhirSampleMaterials.containsKey(convertedSampleMaterial))
                convertedFhirSampleMaterials.put(convertedSampleMaterial, convertedSampleMaterial);
        }
        int totalStarModelSampleMaterialCount = 0;
        Map<String,String> starModelSampleMaterials = new HashMap<>();
        for (Map<String, String> factTable: getFactTables())
            if (factTable.containsKey("sample_type")) {
                String sampleMaterial = factTable.get("sample_type");
                if (!starModelSampleMaterials.containsKey(sampleMaterial)) {
                    totalStarModelSampleMaterialCount++;
                    starModelSampleMaterials.put(sampleMaterial, sampleMaterial);
                }
            } else if (starModelMaterialTypeMissingCount++ < 5)
                logger.warn("materialTypeSanityCheck: fact table does not contain sample_type: " + Util.jsonStringFomObject(factTable));
        if (convertedFhirSampleMaterials.size() != totalStarModelSampleMaterialCount && getFactTables().size() != 0) {
            logger.warn("materialTypeSanityCheck: !!!!!!!!!!!!!!!!!!!!! converted FHIR material type count (" + convertedFhirSampleMaterials.size() + ") is different from star model material type count (" + totalStarModelSampleMaterialCount + ")");
            logger.warn("materialTypeSanityCheck: FHIR material types: " + Util.orderedKeylistFromMap(fhirSampleMaterials));
            logger.warn("materialTypeSanityCheck: converted FHIR material types: " + Util.orderedKeylistFromMap(convertedFhirSampleMaterials));
            logger.warn("materialTypeSanityCheck: star model material types: " + Util.orderedKeylistFromMap(starModelSampleMaterials));
            logger.warn("materialTypeSanityCheck: star model material fact count: " + getFactCount());
        }
    }

    /**
     * Sanity check: is the disease count less than the fact table count?
     *
     */
    public void diseaseSanityCheck() {
        int diseaseCount = 0;
        for (Map<String, String> factTable: getFactTables())
            if (factTable.containsKey("disease"))
                diseaseCount++;
        if (diseaseCount < getFactTables().size())
            logger.warn("diseaseSanityCheck: !!!!!!!!!!!!!!!!!!!!! disease count (" + diseaseCount + ") is different from fact table count (" + getFactCount() + ")");
    }
}
