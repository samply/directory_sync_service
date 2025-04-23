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

    public List<String> getInputCollectionIds() {
        return List.copyOf(inputData.keySet());
    }
}
