package de.samply.directory_sync_service.sync;

import com.google.gson.Gson;
import de.samply.directory_sync_service.directory.DirectoryApiWriteToFile;
import de.samply.directory_sync_service.model.FactTable;
import de.samply.directory_sync_service.model.StarModelInput;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

public class StarModelUpdaterTest {
    @Test
    public void testSendStarModelUpdatesToDirectory() {
        // Use DirectoryApiWriteToFile because it caches intermediate results
        // and we can use these for checking the operation of the method being tested.
        DirectoryApiWriteToFile directoryApi = new DirectoryApiWriteToFile(null);

        // Synthetic input data for the method being tested.
        String starModelInputDataJsonString = "{\n" +
                "  \"minDonors\": 10,\n" +
                "  \"ageAtPrimaryDiagnosisWarningCounter\": 0,\n" +
                "  \"inputData\": {\n" +
                "    \"bbmri-eric:ID:EU_BBMRI-ERIC:collection:CRC-Cohort\": [\n" +
                "      {\n" +
                "        \"sample_material\": \"TISSUE_PARAFFIN_EMBEDDED\",\n" +
                "        \"hist_loc\": \"urn:miriam:icd:C18.0\",\n" +
                "        \"sex\": \"FEMALE\",\n" +
                "        \"age_at_primary_diagnosis\": \"35\",\n" +
                "        \"collection\": \"bbmri-eric:ID:EU_BBMRI-ERIC:collection:CRC-Cohort\",\n" +
                "        \"id\": \"70566273-0951-45e1-91a7-be8387f8edd0\"\n" +
                "      }\n" +
                "    ]\n" +
                "  },\n" +
                "  \"factTables\": []\n" +
                "}";
        Gson gson = new Gson();
        StarModelInput starModelInputData = gson.fromJson(starModelInputDataJsonString, StarModelInput.class);
        Map<String, String> correctedDiagnoses = null;
        int minDonors = 0;
        int maxFacts = (-1);

        // Call the method
        FactTable result = StarModelUpdater.sendStarModelUpdatesToDirectory(directoryApi, correctedDiagnoses, starModelInputData, minDonors, maxFacts);

        // Assert expected outcomes
        assertNotNull(result, "Method should return non-null if the updates were successful");

        // Get the results from the DirectoryApiWriteToFile object.
        String factTableString = directoryApi.getFactTableString();
        assertNotNull(factTableString, "factTableString should not be null");

        // Assert that the fact table has the expected number of lines and columns
        String[] factTableLines = factTableString.split("\n");
        assertEquals(2, factTableLines.length, "DirectoryApiWriteToFile should write 2 lines to the fact table");
        String[] headerRowColumns = factTableLines[0].split(";");
        assertEquals(10, headerRowColumns.length, "DirectoryApiWriteToFile should write 10 columns in the header row");
        String[] dataRowColumns = factTableLines[1].split(";");
        assertEquals(10, dataRowColumns.length, "DirectoryApiWriteToFile should write 10 columns in the data row");

        // Assert that the data in the fact table is as expected
        assertEquals("bbmri-eric:factID:EU_BBMRI-ERIC:collection:CRC-Cohort:663481554", dataRowColumns[0], "The first column in the data row should be the fact ID");
        assertEquals("FEMALE", dataRowColumns[1], "The second column in the data row should be sex FEMALE");
        assertEquals("urn:miriam:icd:C18.0", dataRowColumns[2], "The third column in the data row should be disease urn:miriam:icd:C18.0");
        assertEquals("Adult", dataRowColumns[3], "The fourth column in the data row should be age Adult");
        assertEquals("TISSUE_PARAFFIN_EMBEDDED", dataRowColumns[4], "The fifth column in the data row should be sample type TISSUE_PARAFFIN_EMBEDDED");
        assertEquals("1", dataRowColumns[5], "The sixth column in the data row should be number of donors 1");
        assertEquals("1", dataRowColumns[6], "The seventh column in the data row should be number of samples 1");
        assertEquals("bbmri-eric:ID:EU_BBMRI-ERIC:collection:CRC-Cohort", dataRowColumns[8], "The ninth column in the data row should be the collection ID");
        assertEquals("EU", dataRowColumns[9], "The tenth column in the data row should be the national node EU");
    }
}
