package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.FactTable;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DirectoryApiTest {

    // --- Minimal concrete implementation for testing ---
    static class TestDirectoryApi extends DirectoryApi {
        TestDirectoryApi(boolean mock) { super(mock); }

        // knobs to control behavior
        List<List<String>> pagedFactIds = new ArrayList<>();
        List<String> deleteCountryCodes = new ArrayList<>();
        List<List<String>> deletedPages = new ArrayList<>();
        List<String> updatedBlocksCountry = new ArrayList<>();
        List<List<Map<String,String>>> updatedBlocks = new ArrayList<>();
        boolean deleteFactsReturn = true;
        boolean allBlocksSucceed = true;
        Set<String> validIcd = new HashSet<>();
        @Override public boolean login() { return true; }
        @Override public de.samply.directory_sync_service.directory.model.Biobank fetchBiobank(BbmriEricId id) { return null; }
        @Override public void fetchBasicCollectionData(de.samply.directory_sync_service.model.Collections putCollections) {}
        @Override public boolean sendUpdatedCollections(de.samply.directory_sync_service.model.Collections collections) { return true; }

        @Override
        protected boolean updateFactTablesBlock(String countryCode, List<Map<String, String>> factTablesBlock) {
            updatedBlocksCountry.add(countryCode);
            updatedBlocks.add(factTablesBlock);
            return allBlocksSucceed;
        }

        private final AtomicInteger pageIdx = new AtomicInteger(0);
        @Override
        protected List<String> getNextPageOfFactIdsForCollection(String collectionId) {
            int i = pageIdx.getAndIncrement();
            if (i >= pagedFactIds.size()) return Collections.emptyList();
            return pagedFactIds.get(i);
        }

        @Override
        protected boolean deleteFactsByIds(String countryCode, List<String> factIds) {
            deleteCountryCodes.add(countryCode);
            deletedPages.add(factIds);
            return deleteFactsReturn;
        }

        @Override
        protected boolean isValidIcdValue(String diagnosis) {
            return validIcd.contains(diagnosis);
        }
    }

    // ---------- updateStarModel ----------

    @Test
    void updateStarModel_mockMode_returnsTrue_withoutCallingBlocks() {
        TestDirectoryApi api = new TestDirectoryApi(true); // mockDirectory = true

        FactTable ft = new FactTable();
        // Make the first fact contain a valid collection ID so country extraction works if ever used
        Map<String,String> f = new HashMap<>();
        f.put("collection", "bbmri-eric:ID:DE_ABC");
        ft.addFactTable(List.of(f));

        assertTrue(api.updateStarModel(ft, List.of("bbmri-eric:ID:DE_X")));
        assertTrue(api.updatedBlocks.isEmpty(), "no block calls in mock mode");
    }

    @Test
    void updateStarModel_realMode_blocksOf1000_andCountryTakenFromFactTable() {
        TestDirectoryApi api = new TestDirectoryApi(false);

        FactTable ft = new FactTable();
        // Build 2,100 facts to force 3 blocks: 1000 + 1000 + 100
        List<Map<String,String>> facts = new ArrayList<>();
        for (int i = 0; i < 2100; i++) {
            Map<String,String> row = new HashMap<>();
            row.put("collection", "bbmri-eric:ID:DE_ABC"); // ensures getCountryCode() == DE
            facts.add(row);
        }
        ft.addFactTable(facts);

        assertTrue(api.updateStarModel(ft, List.of("bbmri-eric:ID:DE_OLD")));

        assertEquals(3, api.updatedBlocks.size());
        assertEquals(1000, api.updatedBlocks.get(0).size());
        assertEquals(1000, api.updatedBlocks.get(1).size());
        assertEquals(100,  api.updatedBlocks.get(2).size());
        // country captured for every block
        assertEquals(List.of("DE","DE","DE"), api.updatedBlocksCountry);
    }

    @Test
    void updateStarModel_realMode_stopsOnFirstFailedBlock() {
        TestDirectoryApi api = new TestDirectoryApi(false);
        api.allBlocksSucceed = false; // cause first block to fail

        FactTable ft = new FactTable();
        Map<String,String> row = new HashMap<>();
        row.put("collection", "bbmri-eric:ID:DE_ABC");
        ft.addFactTable(Collections.nCopies(10, row));

        assertFalse(api.updateStarModel(ft, List.of("bbmri-eric:ID:DE_X")));
        assertEquals(1, api.updatedBlocks.size(), "stop after first failed block");
    }

    // ---------- deleteStarModel (paging) ----------

    @Test
    void deleteStarModel_pagesUntilEmpty_andUsesExtractedCountryCode() {
        TestDirectoryApi api = new TestDirectoryApi(false);
        // Two pages then empty
        api.pagedFactIds = List.of(
                List.of("F1","F2"),
                List.of("F3"),
                Collections.emptyList()
        );
        boolean ok = api.deleteStarModel(List.of("bbmri-eric:ID:DE_ABC"));

        assertTrue(ok);
        // deletion called twice (empty page stops before calling delete)
        assertEquals(2, api.deletedPages.size());
        assertEquals(List.of("F1","F2"), api.deletedPages.get(0));
        assertEquals(List.of("F3"),       api.deletedPages.get(1));
        // country always “DE”
        assertEquals(List.of("DE","DE"), api.deleteCountryCodes);
    }

    // ---------- collectDiagnosisCorrections ----------

    @Test
    void collectDiagnosisCorrections_truncatesOrNulls_invalidCodes() {
        TestDirectoryApi api = new TestDirectoryApi(false);
        // Valid codes: C10 and E23 — note no subcodes
        api.validIcd = Set.of("C10", "E23");

        Map<String,String> diagnoses = new HashMap<>();
        diagnoses.put("C10.9", "C10.9"); // invalid → truncate to C10
        diagnoses.put("XYZ",   "XYZ");   // invalid → null
        diagnoses.put("E23.1", "E23.1"); // invalid → truncate to E23
        diagnoses.put("C10",   "C10");   // valid → unchanged

        api.collectDiagnosisCorrections(diagnoses);

        assertEquals("C10", diagnoses.get("C10.9"));
        assertNull(diagnoses.get("XYZ"));
        assertEquals("E23", diagnoses.get("E23.1"));
        assertEquals("C10", diagnoses.get("C10"));
    }

    @Test
    void collectDiagnosisCorrections_noop_whenMockDirectoryTrue() {
        TestDirectoryApi api = new TestDirectoryApi(true); // mock
        Map<String,String> diagnoses = new HashMap<>();
        diagnoses.put("BAD", "BAD");
        api.collectDiagnosisCorrections(diagnoses);
        assertEquals("BAD", diagnoses.get("BAD")); // unchanged
    }

    // ---------- extractCountryCodeFromBbmriEricId ----------

    @Test
    void extractCountryCode_validId_returnsUppercaseCountry() {
        TestDirectoryApi api = new TestDirectoryApi(false);
        assertEquals("DE", api.extractCountryCodeFromBbmriEricId("bbmri-eric:ID:DE_ABC"));
    }

    // ---------- cleanEntity ----------

    @Test
    void cleanEntity_removesEmptyLists_andSingleNullLists_only() {
        TestDirectoryApi api = new TestDirectoryApi(false);
        Map<String,Object> entity = new HashMap<>();
        entity.put("okList", List.of("X"));
        entity.put("emptyList", new ArrayList<>());
        List<Object> singleNull = new ArrayList<>();
        singleNull.add(null);
        entity.put("singleNull", singleNull);
        entity.put("other", 42);

        api.cleanEntity(entity);

        assertTrue(entity.containsKey("okList"));
        assertTrue(entity.containsKey("other"));
        assertFalse(entity.containsKey("emptyList"));
        assertFalse(entity.containsKey("singleNull"));
    }

    // ---------- cleanTimestamp ----------

    @Test
    void cleanTimestamp_stripsTrailingNonDigit() {
        TestDirectoryApi api = new TestDirectoryApi(false);
        assertEquals("2025-01-02T03:04:05", api.cleanTimestamp("2025-01-02T03:04:05Z"));
        assertEquals("2025-01-02T03:04:05", api.cleanTimestamp("2025-01-02T03:04:05"));
    }

    // ---------- transformEntityForEmx2 / transformAttributeForEmx2 ----------

    @Test
    void transformEntityForEmx2_listOfStrings_becomesListOfMapsWithName() {
        TestDirectoryApi api = new TestDirectoryApi(false);
        Map<String,Object> entity = new HashMap<>();
        entity.put("materials", new ArrayList<>(List.of("SERUM","PLASMA")));
        entity.put("sex", new ArrayList<>(List.of("MALE")));

        api.transformEntityForEmx2(entity);

        @SuppressWarnings("unchecked")
        List<Map<String,String>> mats = (List<Map<String,String>>) entity.get("materials");
        @SuppressWarnings("unchecked")
        List<Map<String,String>> sex  = (List<Map<String,String>>) entity.get("sex");

        assertEquals(List.of(Map.of("name","SERUM"), Map.of("name","PLASMA")), mats);
        assertEquals(List.of(Map.of("name","MALE")), sex);
    }

    @Test
    void transformAttributeForEmx2_singleString_becomesSingleMap() {
        TestDirectoryApi api = new TestDirectoryApi(false);
        Map<String,Object> entity = new HashMap<>();
        entity.put("country", "DE");

        api.transformAttributeForEmx2(entity, "country", "name");

        assertEquals(Map.of("name","DE"), entity.get("country"));
    }

    @Test
    void transformEntityForEmx2_idBasedAttributes_mappedToIdElement() {
        TestDirectoryApi api = new TestDirectoryApi(false);
        Map<String,Object> entity = new HashMap<>();
        entity.put("biobank", "BB-01");
        entity.put("national_node", new ArrayList<>(List.of("DE")));
        entity.put("contact", List.of("john.doe@example.org"));

        api.transformEntityForEmx2(entity);

        assertEquals(Map.of("id","BB-01"), entity.get("biobank"));

        @SuppressWarnings("unchecked")
        List<Map<String,String>> nn = (List<Map<String,String>>) entity.get("national_node");
        @SuppressWarnings("unchecked")
        List<Map<String,String>> contact = (List<Map<String,String>>) entity.get("contact");

        assertEquals(List.of(Map.of("id","DE")), nn);
        assertEquals(List.of(Map.of("id","john.doe@example.org")), contact);
    }
}
