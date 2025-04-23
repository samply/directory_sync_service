package de.samply.directory_sync_service.sync;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.samply.directory_sync_service.directory.DirectoryApiWriteToFile;
import de.samply.directory_sync_service.directory.model.Collection;
import de.samply.directory_sync_service.directory.model.Collections;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CollectionUpdaterTest {
    @Test
    public void testSendStarModelUpdatesToDirectory() {
        // Use DirectoryApiWriteToFile because it caches intermediate results
        // and we can use these for checking the operation of the method being tested.
        DirectoryApiWriteToFile directoryApi = new DirectoryApiWriteToFile(null);

        // Synthetic input data for the method being tested.
        String fhirCollectionsJsonString = "{\"collections\": {\n" +
                "  \"bbmri-eric:ID:EU_BBMRI-ERIC:collection:CRC-Cohort\": {\n" +
                "    \"id\": \"bbmri-eric:ID:EU_BBMRI-ERIC:collection:CRC-Cohort\",\n" +
                "    \"size\": 1,\n" +
                "    \"numberOfDonors\": 1,\n" +
                "    \"sex\": [\n" +
                "      \"female\"\n" +
                "    ],\n" +
                "    \"ageLow\": 42,\n" +
                "    \"ageHigh\": 42,\n" +
                "    \"materials\": [\n" +
                "      \"TISSUE_PARAFFIN_EMBEDDED\"\n" +
                "    ],\n" +
                "    \"storageTemperatures\": [],\n" +
                "    \"diagnosisAvailable\": [\n" +
                "      \"C18.0\"\n" +
                "    ]\n" +
                "  }\n" +
                "}}";
        Gson gson = new Gson();
        Collections fhirCollections = gson.fromJson(fhirCollectionsJsonString, Collections.class);
        Map<String, String> correctedDiagnoses = null;

        // Call the method
        boolean result = directoryApi.sendUpdatedCollections(fhirCollections);

        // Assert expected outcomes
        assertTrue(result, "Method should return true if the updates were successful");

        // Get the results from the DirectoryApiWriteToFile object.
        String entityTableString = directoryApi.getEntityTableString();
        assertNotNull(entityTableString, "factTableString should not be null");

        // Assert that the fact table has the expected number of lines and columns
        String[] entityTableLines = entityTableString.split("\n");
        assertEquals(2, entityTableLines.length, "DirectoryApiWriteToFile should write 2 lines to the entity table");
        String[] headerRowColumns = entityTableLines[0].split(";");
        assertEquals(16, headerRowColumns.length, "DirectoryApiWriteToFile should write 16 columns in the header row");
        String[] dataRowColumns = entityTableLines[1].split(";");
        assertEquals(16, dataRowColumns.length, "DirectoryApiWriteToFile should write 16 columns in the data row");

        // Assert that the data in the fact table is as expected
        assertEquals("bbmri-eric:ID:EU_BBMRI-ERIC:collection:CRC-Cohort", dataRowColumns[0], "The first column in the data row should be the collection ID");
        assertEquals("EU", dataRowColumns[1], "The second column in the data row should be country EU");
        assertEquals("SAMPLE", dataRowColumns[2], "The third column in the data row should be type SAMPLE");
        assertEquals("BIOLOGICAL_SAMPLES", dataRowColumns[3], "The fourth column in the data row should be data category BIOLOGICAL_SAMPLES");
        assertEquals("0", dataRowColumns[4], "The fifth column in the data row should be order of magnitude 0");
        assertEquals("1", dataRowColumns[5], "The sixth column in the data row should be size 1");
        assertEquals("1", dataRowColumns[7], "The eigth column in the data row should be number of donors 1");
        assertEquals("0", dataRowColumns[8], "The ninth column in the data row should be the order of magnitude donors 0");
        assertEquals("FEMALE", dataRowColumns[9], "The tenth column in the data row should be the sex FEMALE");
        assertEquals("urn:miriam:icd:C18.0", dataRowColumns[10], "The eleventh column in the data row should be the diagnosis urn:miriam:icd:C18.0");
        assertEquals("42", dataRowColumns[11], "The twelfth column in the data row should be the age low 42");
        assertEquals("42", dataRowColumns[12], "The thirteenth column in the data row should be the age high 42");
        assertEquals("TISSUE_PARAFFIN_EMBEDDED", dataRowColumns[13], "The fourteenth column in the data row should be the materials TISSUE_PARAFFIN_EMBEDDED");
        assertEquals("EU", dataRowColumns[15], "The sixteenth column in the data row should be the national node EU");
    }
}
