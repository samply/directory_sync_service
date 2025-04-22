package de.samply.directory_sync_service.model;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import de.samply.directory_sync_service.fhir.FhirApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents data for the STAR model, organized by collection.
 * <p>
 * Input data represents data read in from the FHIR store.
 * <p>
 * Output data is in a format that is ready to be exported to the Directory.
 */
public class StarModelData {
    private static final Logger logger = LoggerFactory.getLogger(StarModelData.class);

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

    public List<Map<String, String>> getInputRowsAsStringMaps(String collectionId) {
        List<Map<String, String>> rowsAsStringMaps = new ArrayList<Map<String, String>>();
        for (StarModelInputRow row: inputData.get(collectionId))
            rowsAsStringMaps.add(row.asMap());

        return rowsAsStringMaps;
    }

    // *** Input data for the star model.

//    int ageAtPrimaryDiagnosisWarningCounter = 0;
//    /**
//     * Represents an input row of the inputData table, with attributes commonly associated with medical data.
//     * Extends the HashMap class to provide a key-value mapping for attributes.
//     * <p>
//     * The attributes include collection, sample material, patient ID, sex, histological location,
//     * and age at primary diagnosis.
//     */
//    public class InputRow {
//        private String age_at_primary_diagnosis;
//        private String hist_loc;
//        private String sex;
//        private String id;
//        private String sample_material;
//        private String collection;
//
//        /**
//         * Constructs a new InputRow with the specified attributes.
//         *
//         * @param collection The identifier for the collection associated with the input row.
//         * @param sampleMaterial The sample material associated with the input row.
//         * @param patientId The identifier of the patient associated with the input row.
//         * @param sex The gender information of the patient.
//         * @param age The age at primary diagnosis of the patient.
//         */
//        public InputRow(String collection, String sampleMaterial, String patientId, String sex, String age) {
//            setCollection(collection);
//            setSampleMaterial(sampleMaterial);
//            setId(patientId);
//            setSex(sex);
//            setAgeAtPrimaryDiagnosis(age);
//        }
//
//        /**
//         * Constructs a new InputRow based on an existing row and updates it with a new diagnosis.
//         *
//         * @param row The existing input row to base the new row on.
//         * @param histLoc The new histological location to be associated with the input row.
//         */
//        public InputRow(InputRow row, String histLoc) {
//            setAgeAtPrimaryDiagnosis(row.getAgeAtPrimaryDiagnosis());
//            setCollection(row.getCollection());
//            setId(row.getId());
//            setSex(row.getSex());
//            setSampleMaterial(row.getSampleMaterial());
//            setHistLoc(histLoc);
//        }
//
//        public Map<String, String> asMap() {
//            Map<String, String> row = new HashMap<String, String>();
//            row.put("age_at_primary_diagnosis", age_at_primary_diagnosis);
//            row.put("hist_loc", hist_loc);
//            row.put("sex", sex);
//            row.put("id", id);
//            row.put("sample_material", sample_material);
//            row.put("collection", collection);
//            return row;
//        }
//
//        public String getCollection() {
//            return collection;
//        }
//
//        /**
//         * Sets the collection attribute for the input row.
//         *
//         * @param collection The identifier for the collection to be associated with the input row.
//         */
//        public void setCollection(String collection) {
//            if (collection == null)
//                return;
//            this.collection = collection;
//        }
//
//        public String getSampleMaterial() {
//            return sample_material;
//        }
//
//        /**
//         * Sets the sample material attribute for the input row.
//         * Converts the provided sample material using the FhirToDirectoryAttributeConverter.
//         *
//         * @param sampleMaterial The sample material to be associated with the input row.
//         *
//         * @see FhirToDirectoryAttributeConverter#convertMaterial(String)
//         */
//        public void setSampleMaterial(String sampleMaterial) {
//            if (sampleMaterial == null)
//                return;
//            this.sample_material = FhirToDirectoryAttributeConverter.convertMaterial(sampleMaterial);
//        }
//
//        public String getId() {
//            return id;
//        }
//
//        /**
//         * Sets the patient ID attribute for the input row.
//         *
//         * @param id The identifier of the patient to be associated with the input row.
//         */
//        public void setId(String id) {
//            if (id == null)
//                return;
//            this.id = id;
//        }
//
//        public String getSex() {
//            return sex;
//        }
//
//        /**
//         * Sets the sex attribute for the input row.
//         * Converts the provided sex information using the FhirToDirectoryAttributeConverter.
//         *
//         * @param sex The gender information to be associated with the input row.
//         *
//         * @see FhirToDirectoryAttributeConverter#convertSex(String)
//         */
//        public void setSex(String sex) {
//            if (sex == null)
//                return;
//            this.sex = FhirToDirectoryAttributeConverter.convertSex(sex);
//        }
//
//        public String getHistLoc() {
//            return hist_loc;
//        }
//
//        /**
//         * Sets the histological location attribute for the input row.
//         * Converts the provided histological location using the FhirToDirectoryAttributeConverter.
//         *
//         * @param histLoc The histological location to be associated with the input row.
//         *
//         * @see FhirToDirectoryAttributeConverter#convertDiagnosis(String)
//         */
//        public void setHistLoc(String histLoc) {
//            if (histLoc == null)
//                return;
//            this.hist_loc = FhirToDirectoryAttributeConverter.convertDiagnosis(histLoc);
//        }
//
//        public String getAgeAtPrimaryDiagnosis() {
//            return age_at_primary_diagnosis;
//        }
//
//        /**
//         * Sets the age at primary diagnosis attribute for the input row.
//         *
//         * @param age The age at primary diagnosis to be associated with the input row.
//         */
//        public void setAgeAtPrimaryDiagnosis(String age) {
//            if (age == null) {
//                if (ageAtPrimaryDiagnosisWarningCounter++ < 5) // Don't print too many warnings
//                    logger.warn("setAgeAtPrimaryDiagnosis: age is null, ignoring.");
//                return;
//            }
//            this.age_at_primary_diagnosis = age;
//        }
//    }

    // Data relevant for Directory sync that has been read in from the FHIR store.
    // This comprises of one row per Patient/Specimen/Diagnosis combination.
    // Each row is a map containing attributes relevant to the star model.
    // A Map of a List of Maps: collectionID_1 -> [row0, row1, ...]
    private final Map<String,List<StarModelInputRow>> inputData = new HashMap<String,List<StarModelInputRow>>();

    /**
     * Adds an input row to the specified collection in the inputData map.
     * If the collectionId does not exist in the map, a new entry is created.
     * 
     * @param collectionId The identifier for the collection where the input row will be added.
     * @param row The input row to be added to the collection.
     * 
     * @throws NullPointerException if collectionId or row is null.
     */
    public void addInputRow(String collectionId, StarModelInputRow row) {
        if (!inputData.containsKey(collectionId))
            inputData.put(collectionId, new ArrayList<StarModelInputRow>());
        List<StarModelInputRow> rows = inputData.get(collectionId);
        rows.add(row);
    }

    /**
     * Creates and returns a new InputRow with the specified attributes.
     * 
     * @param collection The identifier for the collection of the new input row.
     * @param sampleMaterial The sample material associated with the input row.
     * @param patientId The identifier of the patient associated with the input row.
     * @param sex The gender information of the patient.
     * @param age The age information of the patient.
     * 
     * @return A new InputRow with the provided attributes.
     * 
     * @throws NullPointerException if any of the parameters is null.
     * 
     * @see StarModelInputRow
     */
    public StarModelInputRow newInputRow(String collection, String sampleMaterial, String patientId, String sex, String age) {
        return new StarModelInputRow(collection, sampleMaterial, patientId, sex, age);
    }

    /**
     * Creates a new InputRow based on an existing row and adds a diagnosis.
     * 
     * @param row The existing input row to base the new row on.
     * @param histLoc The diagnosis.
     * 
     * @return A new InputRow with the diagnosis added.
     * 
     * @throws NullPointerException if row or histLoc is null.
     */
    public StarModelInputRow newInputRow(StarModelInputRow row, String histLoc) {
        return new StarModelInputRow(row, histLoc);
    }

    public Set<String> getInputCollectionIds() {
        return inputData.keySet();
    }

    // *** Output data.

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
            logger.warn("sampleCountSanityCheck: !!!!!!!!!!!!!!!!!!!!! FHIR sample count (" + totalFhirSpecimenCount + ") is less than star model sample count (" + totalStarModelSpecimenCount + ")");
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
