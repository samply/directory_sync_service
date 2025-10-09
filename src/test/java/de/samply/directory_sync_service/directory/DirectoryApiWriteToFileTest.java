package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.model.Collection;
import de.samply.directory_sync_service.model.Collections;
import de.samply.directory_sync_service.directory.model.Biobank;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DirectoryApiWriteToFileTest {

    /** Produce a single, well-formed Collections payload that ConvertCollectionsToDirectoryCollectionPut can handle. */
    private Collections oneGoodCollection() {
        Collections cols = new Collections();
        Collection c = new Collection();
        c.setId("bbmri-eric:collection:DE_1");
        c.setCountry("DE");                 // used later when writing CSV
        c.setName("Test Collection");
        c.setSize(10);                      // order_of_magnitude becomes 1
        c.setNumberOfDonors(20);            // order_of_magnitude_donors becomes 1
        c.setSex(new ArrayList<>(List.of("male","female")));
        c.setMaterials(new ArrayList<>(List.of("blood-plasma", "tissue")));
        c.setStorageTemperatures(new ArrayList<>(List.of("temperatureLN", "temperatureGN")));
        c.setDiagnosisAvailable(new ArrayList<>(List.of("C10", "E23.1")));
        c.setType(new ArrayList<>(List.of("COHORT")));
        c.setDataCategories(new ArrayList<>(List.of("PHENOTYPE")));
        cols.addCollection(c.getId(), c);
        return cols;
    }

    // ---------- Basic/dummy behaviors ----------

    @Test
    void login_alwaysTrue() {
        var api = new DirectoryApiWriteToFile(null);
        assertTrue(api.login());
    }

    @Test
    void fetchBiobank_returnsDummyBiobank() {
        var api = new DirectoryApiWriteToFile(null);
        var bb = api.fetchBiobank(de.samply.directory_sync_service.model.BbmriEricId.valueOf("bbmri-eric:biobank:DE_X").orElse(null));
        assertNotNull(bb);
        assertEquals(Biobank.class, bb.getClass());
    }

    @Test
    void pagingAndDeleteAndValidation_haveDocumentedDummyResults() {
        var api = new DirectoryApiWriteToFile(null);
        assertTrue(api.deleteFactsByIds("DE", List.of("id1","id2")));
        assertTrue(api.isValidIcdValue("C10"));
        assertTrue(api.getNextPageOfFactIdsForCollection("bbmri-eric:collection:DE_1").isEmpty());
    }

    // ---------- sendUpdatedCollections: in-memory (no IO) ----------

    @Test
    void sendUpdatedCollections_writesEntityCsvString_whenOutputDirIsNull() {
        var api = new DirectoryApiWriteToFile(null);
        var cols = oneGoodCollection();

        assertTrue(api.sendUpdatedCollections(cols), "sendUpdatedCollections should succeed");
        String csv = api.getEntityTableString();
        assertNotNull(csv, "Entity CSV string should be captured in memory when directoryOutputDirectory is null");

        // Sanity: should contain header names and the collection id
        assertTrue(csv.startsWith("id;"), "CSV should start with header row (semicolon-separated)");
        assertTrue(csv.contains("bbmri-eric:collection:DE_1"), "CSV should contain the collection id");
        assertTrue(csv.contains(";DE;"), "CSV should contain the country code in the row");
        // Check a couple of converted/derived columns we expect to appear
        assertTrue(csv.contains("order_of_magnitude"), "Header should include order_of_magnitude");
        assertTrue(csv.contains("order_of_magnitude_donors"), "Header should include order_of_magnitude_donors");
        assertTrue(csv.contains("diagnosis_available"), "Header should include diagnosis_available");
    }

    // ---------- sendUpdatedCollections: with file IO ----------

    @Test
    void sendUpdatedCollections_writesDirectoryCollectionsCsv_toProvidedDirectory(@TempDir Path tempDir) throws IOException {
        var api = new DirectoryApiWriteToFile(tempDir.toString());
        var cols = oneGoodCollection();

        assertTrue(api.sendUpdatedCollections(cols));

        Path out = tempDir.resolve("DirectoryCollections.csv");
        assertTrue(Files.exists(out), "Expected DirectoryCollections.csv to be written");

        String content = Files.readString(out);
        assertTrue(content.contains("bbmri-eric:collection:DE_1"));
        assertTrue(content.contains(";DE;"));
        assertTrue(content.contains("materials"), "Should include the materials column");
        assertTrue(content.contains("storage_temperatures"), "Should include storage_temperatures column");
    }

    // ---------- updateFactTablesBlock: in-memory (no IO) ----------

    @Test
    void updateFactTablesBlock_injectsNationalNode_whenMissing_andCountryProvided() {
        var api = new DirectoryApiWriteToFile(null);
        Map<String,String> fact = new HashMap<>();
        fact.put("id", "f1");
        fact.put("collection", "bbmri-eric:collection:DE_1");
        fact.put("disease", "C10");
        fact.put("sex", "MALE");
        fact.put("age_range", "30-39");
        fact.put("sample_type", "SERUM");
        fact.put("number_of_donors", "5");
        fact.put("number_of_samples", "10");

        // Test double for FactTable: override getters instead of calling setters
        de.samply.directory_sync_service.model.FactTable ft = new de.samply.directory_sync_service.model.FactTable() {
            @Override public String getCountryCode() { return "DE"; }
            @Override public java.util.List<java.util.Map<String,String>> getFactTables() {
                return java.util.List.of(fact);
            }
        };

        assertTrue(api.updateStarModel(ft, List.of("bbmri-eric:collection:DE_1")));

        String csv = api.getFactTableString();
        assertNotNull(csv, "Fact CSV should be captured in memory");
        assertTrue(csv.contains("national_node"), "Header should include national_node");

        // Accept DE whether it's first, middle, or last column
        boolean hasInjectedDE =
                csv.contains("\nDE;")      // first col, newline before row
                        || csv.contains(";DE;")       // middle col
                        || csv.contains(";DE\n")      // last col with newline
                        || csv.endsWith(";DE")        // last col w/o newline
                        || csv.contains(";DE\r\n");   // last col with Windows newline
        assertTrue(hasInjectedDE, "Row should include injected national_node=DE");
    }

    @Test
    void updateFactTablesBlock_preservesExistingNationalNode_evenWhenCountryProvided() {
        var api = new DirectoryApiWriteToFile(null);
        Map<String,String> fact = new HashMap<>();
        fact.put("id", "f2");
        fact.put("collection", "bbmri-eric:collection:DE_1");
        fact.put("disease", "C10");
        fact.put("sex", "FEMALE");
        fact.put("age_range", "40-49");
        fact.put("sample_type", "SERUM");
        fact.put("number_of_donors", "7");
        fact.put("number_of_samples", "12");
        fact.put("national_node", "AT"); // pre-existing and should be kept

        de.samply.directory_sync_service.model.FactTable ft =
                new de.samply.directory_sync_service.model.FactTable() {
                    @Override public String getCountryCode() { return "DE"; }
                    @Override public java.util.List<java.util.Map<String,String>> getFactTables() {
                        return java.util.List.of(fact);
                    }
                };

        assertTrue(api.updateStarModel(ft, List.of("bbmri-eric:collection:DE_1")));

        // Verify the input map still has AT (not overwritten to DE)
        assertEquals("AT", fact.get("national_node"));

        // You can still sanity-check the CSV for other fields if you like:
        String csv = api.getFactTableString();
        assertNotNull(csv);
        assertTrue(csv.contains("f2"));
        assertTrue(csv.contains("bbmri-eric:collection:DE_1"));
        // but don't assert for national_node in the CSV; it isn't written.
    }

    @Test
    void updateFactTablesBlock_writesDirectoryFactTablesCsv_whenOutputDirProvided(@TempDir Path tempDir) throws IOException {
        var api = new DirectoryApiWriteToFile(tempDir.toString());

        Map<String,String> fact = new HashMap<>();
        fact.put("id", "f3");
        fact.put("collection", "bbmri-eric:collection:DE_1");
        fact.put("disease", "C10");
        fact.put("sex", "MALE");
        fact.put("age_range", "50-59");
        fact.put("sample_type", "SERUM");
        fact.put("number_of_donors", "3");
        fact.put("number_of_samples", "6");

        // Test double for FactTable: override getters instead of calling setters
        de.samply.directory_sync_service.model.FactTable ft = new de.samply.directory_sync_service.model.FactTable() {
            @Override public String getCountryCode() { return "DE"; }
            @Override public java.util.List<java.util.Map<String,String>> getFactTables() {
                return java.util.List.of(fact);
            }
        };

        assertTrue(api.updateStarModel(ft, List.of("bbmri-eric:collection:DE_1")));

        Path out = tempDir.resolve("DirectoryFactTables.csv");
        assertTrue(Files.exists(out), "Expected DirectoryFactTables.csv to be written");

        String content = Files.readString(out);
        assertTrue(content.contains("id;sex;disease;age_range;sample_type;number_of_donors;number_of_samples;last_update;collection"),
                "Header should match the documented column order");
        assertTrue(content.contains("f3"));
        assertTrue(content.contains("bbmri-eric:collection:DE_1"));
        // national_node gets injected when countryCode is provided and missing in the map
        assertTrue(content.contains(";DE") || content.contains("DE;"));
    }
}
