package de.samply.directory_sync_service.model;

import static org.junit.jupiter.api.Assertions.*;

import de.samply.directory_sync_service.fhir.FhirApi;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;

/** Unit tests for {@link FactTable}. */
class FactTableTest {

    // --- helper stub FhirApi (avoid network) ---

    /** Minimal FhirApi stub; we override only the methods we exercise. */
    static class StubFhirApi extends FhirApi {
        private final int specimenCount;
        private final Map<String,String> sampleMaterials;

        StubFhirApi(int specimenCount, Map<String,String> sampleMaterials) {
            super("http://stubbed.example"); // not used (we override the methods below)
            this.specimenCount = specimenCount;
            this.sampleMaterials = sampleMaterials;
        }

        @Override
        public int calculateTotalSpecimenCount(BbmriEricId defaultBbmriEricCollectionId) {
            return specimenCount;
        }

        @Override
        public Map<String,String> getSampleMaterials(BbmriEricId defaultBbmriEricCollectionId) {
            return sampleMaterials;
        }
    }

    private static Map<String,String> fact(String... kv) {
        Map<String,String> m = new HashMap<>();
        for (int i=0; i<kv.length; i+=2) m.put(kv[i], kv[i+1]);
        return m;
    }

    // --- basic container behavior ---

    @Test
    @DisplayName("addFactTable aggregates facts; getFactCount reflects total")
    void addAndCount() {
        FactTable ft = new FactTable();
        assertEquals(0, ft.getFactCount());

        ft.addFactTable(List.of(
                fact("collection", "bbmri-eric:ID:DE_A"),
                fact("collection", "bbmri-eric:ID:DE_B")
        ));
        assertEquals(2, ft.getFactCount());

        ft.addFactTable(List.of(fact("collection", "bbmri-eric:ID:DE_C")));
        assertEquals(3, ft.getFactCount());

        // sanity: facts are present
        assertEquals(3, ft.getFactTables().size());
    }

    // --- applyDiagnosisCorrections ---

    @Nested
    class ApplyDiagnosisCorrections {

        @Test
        @DisplayName("Null map -> no changes")
        void nullMap_noop() {
            FactTable ft = new FactTable();
            List<Map<String,String>> facts = new ArrayList<>();
            facts.add(fact("collection","bbmri-eric:ID:DE_A","disease","A00"));
            ft.addFactTable(facts);

            ft.applyDiagnosisCorrections(null);

            assertEquals("A00", ft.getFactTables().get(0).get("disease"));
        }

        @Test
        @DisplayName("Replaces disease values when a mapping exists")
        void replacesWhenMapped() {
            FactTable ft = new FactTable();
            ft.addFactTable(List.of(fact("collection","bbmri-eric:ID:DE_A","disease","A00")));

            Map<String,String> corrections = Map.of(
                    "A00.2", "A00"
            );
            ft.applyDiagnosisCorrections(corrections);

            assertEquals("A00", ft.getFactTables().get(0).get("disease"));
        }

        @Test
        @DisplayName("Removes disease when mapped to null or when value is null")
        void removesWhenMappedToNull_orOriginalNull() {
            FactTable ft = new FactTable();
            Map<String,String> factWithNullDisease = new HashMap<>();
            factWithNullDisease.put("collection", "bbmri-eric:ID:DE_B");
            factWithNullDisease.put("disease", null);
            // one maps to null; one is originally null; one has no 'disease' key
            ft.addFactTable(List.of(
                    fact("collection","bbmri-eric:ID:DE_A","disease","A00"),
                    factWithNullDisease,
                    fact("collection","bbmri-eric:ID:DE_C","sample_type","SERUM")
            ));

            Map<String,String> corrections = new HashMap<>();
            corrections.put("A00", null); // instruct to remove

            ft.applyDiagnosisCorrections(corrections);

            Map<String,String> f0 = ft.getFactTables().get(0);
            Map<String,String> f1 = ft.getFactTables().get(1);
            Map<String,String> f2 = ft.getFactTables().get(2);

            assertFalse(f0.containsKey("disease"), "A00 mapped to null -> removed");
            assertFalse(f1.containsKey("disease"), "original null -> removed");
            assertFalse(f2.containsKey("disease"), "no disease key remains absent");
        }
    }

    // --- getCountryCode ---

    @Nested
    class GetCountryCode {

        @Test
        @DisplayName("Returns country code from first fact's 'collection' BBMRI-ERIC id")
        void validId_returnsCountry() {
            FactTable ft = new FactTable();
            ft.addFactTable(List.of(
                    fact("collection", "bbmri-eric:ID:DE_ABC") // first fact determines code
            ));
            assertEquals("DE", ft.getCountryCode());
        }

        @Test
        @DisplayName("Empty fact table -> null")
        void empty_returnsNull() {
            FactTable ft = new FactTable();
            assertNull(ft.getCountryCode());
        }

        @Test
        @DisplayName("Invalid collection id -> NullPointerException (documented behavior)")
        void invalidId_throwsNpe() {
            FactTable ft = new FactTable();
            ft.addFactTable(List.of(
                    fact("collection", "NOT-BBMRI-FORMAT")
            ));
            assertThrows(NullPointerException.class, ft::getCountryCode);
        }
    }

    // --- sanity checks (behavior is logging-only; we assert no exceptions) ---

    @Test
    @DisplayName("sampleCountSanityCheck runs without throwing (counts derived)")
    void sampleCountSanityCheck_noThrow() {
        FactTable ft = new FactTable();
        // two facts with number_of_samples; one without key
        ft.addFactTable(List.of(
                fact("collection","bbmri-eric:ID:DE_A","number_of_samples","5"),
                fact("collection","bbmri-eric:ID:DE_B","number_of_samples","3"),
                fact("collection","bbmri-eric:ID:DE_C") // ignored
        ));
        FhirApi api = new StubFhirApi(
                10, // FHIR specimen count
                Map.of() // not used here
        );
        assertDoesNotThrow(() -> ft.sampleCountSanityCheck(api, "bbmri-eric:ID:DE_DEF"));
    }

    @Test
    @DisplayName("materialTypeSanityCheck compares converted FHIR materials vs star model types (no throw)")
    void materialTypeSanityCheck_noThrow() {
        FactTable ft = new FactTable();
        // star model distinct sample types: SERUM, PLASMA (2 unique)
        ft.addFactTable(List.of(
                fact("collection","bbmri-eric:ID:DE_A","sample_type","SERUM"),
                fact("collection","bbmri-eric:ID:DE_B","sample_type","PLASMA"),
                fact("collection","bbmri-eric:ID:DE_C","sample_type","SERUM")
        ));
        // FHIR materials include variants that convert to same keys:
        // "blood_serum" -> SERUM, "plasma-vital" -> PLASMA
        Map<String,String> fhirMats = new HashMap<>();
        fhirMats.put("blood_serum","blood_serum");
        fhirMats.put("plasma-vital","plasma-vital");

        FhirApi api = new StubFhirApi(0, fhirMats);
        assertDoesNotThrow(() -> ft.materialTypeSanityCheck(api, "bbmri-eric:ID:DE_DEF"));
    }

    @Test
    @DisplayName("diseaseSanityCheck logs when missing disease keys (no throw)")
    void diseaseSanityCheck_noThrow() {
        FactTable ft = new FactTable();
        ft.addFactTable(List.of(
                fact("collection","bbmri-eric:ID:DE_A","disease","A00"),
                fact("collection","bbmri-eric:ID:DE_B"), // missing disease
                fact("collection","bbmri-eric:ID:DE_C","disease","B01")
        ));
        assertDoesNotThrow(ft::diseaseSanityCheck);
    }
}
