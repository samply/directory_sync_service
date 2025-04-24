package de.samply.directory_sync_service.model;

import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * Represents an input row for star model generation, with attributes commonly associated with medical data.
 * <p>
 * The attributes include collection, sample material, patient ID, sex, histological location,
 * and age at primary diagnosis.
 */
public class StarModelInputRow {
    private static final Logger logger = LoggerFactory.getLogger(StarModelInputRow.class);
    private static int ageAtPrimaryDiagnosisWarningCounter = 0;

    private String age_at_primary_diagnosis;
    private String hist_loc;
    private String sex;
    private String id;
    private String sample_material;
    private String collection;

    /**
     * Constructs a new StarModelInputRow with the specified attributes.
     *
     * @param collection The identifier for the collection associated with the input row.
     * @param sampleMaterial The sample material associated with the input row.
     * @param patientId The identifier of the patient associated with the input row.
     * @param sex The gender information of the patient.
     * @param age The age at primary diagnosis of the patient.
     */
    public StarModelInputRow(String collection, String sampleMaterial, String patientId, String sex, String age) {
        setCollection(collection);
        setSampleMaterial(sampleMaterial);
        setId(patientId);
        setSex(sex);
        setAgeAtPrimaryDiagnosis(age);
    }

    /**
     * Constructs a new StarModelInputRow based on an existing row and updates it with a new diagnosis.
     *
     * @param row The existing input row to base the new row on.
     * @param histLoc The new histological location to be associated with the input row.
     */
    public StarModelInputRow(StarModelInputRow row, String histLoc) {
        setAgeAtPrimaryDiagnosis(row.getAgeAtPrimaryDiagnosis());
        setCollection(row.getCollection());
        setId(row.getId());
        setSex(row.getSex());
        setSampleMaterial(row.getSampleMaterial());
        setHistLoc(histLoc);
    }

    public Map<String, String> asMap() {
        Map<String, String> row = new HashMap<String, String>();
        row.put("age_at_primary_diagnosis", age_at_primary_diagnosis);
        row.put("hist_loc", hist_loc);
        row.put("sex", sex);
        row.put("id", id);
        row.put("sample_material", sample_material);
        row.put("collection", collection);
        return row;
    }

    public String getCollection() {
        return collection;
    }

    /**
     * Sets the collection attribute for the input row.
     *
     * @param collection The identifier for the collection to be associated with the input row.
     */
    public void setCollection(String collection) {
        if (collection == null)
            return;
        this.collection = collection;
    }

    public String getSampleMaterial() {
        return sample_material;
    }

    /**
     * Sets the sample material attribute for the input row.
     * Converts the provided sample material using the FhirToDirectoryAttributeConverter.
     *
     * @param sampleMaterial The sample material to be associated with the input row.
     *
     * @see FhirToDirectoryAttributeConverter#convertMaterial(String)
     */
    public void setSampleMaterial(String sampleMaterial) {
        if (sampleMaterial == null)
            return;
        this.sample_material = FhirToDirectoryAttributeConverter.convertMaterial(sampleMaterial);
    }

    public String getId() {
        return id;
    }

    /**
     * Sets the patient ID attribute for the input row.
     *
     * @param id The identifier of the patient to be associated with the input row.
     */
    public void setId(String id) {
        if (id == null)
            return;
        this.id = id;
    }

    public String getSex() {
        return sex;
    }

    /**
     * Sets the sex attribute for the input row.
     * Converts the provided sex information using the FhirToDirectoryAttributeConverter.
     *
     * @param sex The gender information to be associated with the input row.
     *
     * @see FhirToDirectoryAttributeConverter#convertSex(String)
     */
    public void setSex(String sex) {
        if (sex == null)
            return;
        this.sex = FhirToDirectoryAttributeConverter.convertSex(sex);
    }

    /**
     * Sets the histological location attribute for the input row.
     * Converts the provided histological location using the FhirToDirectoryAttributeConverter.
     *
     * @param histLoc The histological location to be associated with the input row.
     *
     * @see FhirToDirectoryAttributeConverter#convertDiagnosis(String)
     */
    public void setHistLoc(String histLoc) {
        if (histLoc == null)
            return;
        this.hist_loc = FhirToDirectoryAttributeConverter.convertDiagnosis(histLoc);
    }

    public String getAgeAtPrimaryDiagnosis() {
        return age_at_primary_diagnosis;
    }

    /**
     * Sets the age at primary diagnosis attribute for the input row.
     *
     * @param age The age at primary diagnosis to be associated with the input row.
     */
    public void setAgeAtPrimaryDiagnosis(String age) {
        if (age == null) {
            if (ageAtPrimaryDiagnosisWarningCounter++ < 5) // Don't print too many warnings
                logger.warn("setAgeAtPrimaryDiagnosis: age is null, ignoring.");
            return;
        }
        this.age_at_primary_diagnosis = age;
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
    public static StarModelInputRow newInputRow(StarModelInputRow row, String histLoc) {
        return new StarModelInputRow(row, histLoc);
    }
}
