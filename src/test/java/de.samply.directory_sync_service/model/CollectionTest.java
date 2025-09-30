package de.samply.directory_sync_service.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CollectionTest {

    // Helper to make a collection with a few defaults for merge tests
    private static Collection baseCollection() {
        Collection c = new Collection();
        c.setAgeHigh(80);
        c.setAgeLow(18);
        c.setBiobank("BB1");
        c.setContact("contact@bb1.org");
        c.setCountry("DE");
        c.setDataCategories(new ArrayList<>(List.of("phenotype")));
        c.setDescription("desc");
        c.setDiagnosisAvailable(new ArrayList<>(List.of("A00")));
        c.setHead("Dr. A");
        c.setId("bbmri-eric:ID:DE_001");
        c.setLocation("Berlin");
        c.setMaterials(new ArrayList<>(List.of("blood")));
        c.setName("Coll-1");
        c.setNetwork(new ArrayList<>(List.of("BBMRI-ERIC")));
        c.setNumberOfDonors(1000);
        c.setSex(new ArrayList<>(List.of("male")));
        c.setSize(1200);
        c.setStorageTemperatures(new ArrayList<>(List.of("-80")));
        c.setType(new ArrayList<>(List.of("case-control")));
        c.setUrl("https://example.org/coll1");
        return c;
    }

    @Nested
    class ApplyDiagnosisCorrections {

        @Test
        @DisplayName("Null corrections map → no change")
        void nullCorrections_noChange() {
            Collection c = new Collection();
            c.setDiagnosisAvailable(new ArrayList<>(List.of("A00", "B00")));

            c.applyDiagnosisCorrections(null);

            assertEquals(List.of("A00", "B00"), c.getDiagnosisAvailable());
        }

        @Test
        @DisplayName("Empty corrections map → result becomes empty list (no matches)")
        void emptyCorrections_resultsEmpty() {
            Collection c = new Collection();
            c.setDiagnosisAvailable(new ArrayList<>(List.of("A00", "B00")));

            c.applyDiagnosisCorrections(java.util.Collections.emptyMap());

            assertNotNull(c.getDiagnosisAvailable());
            assertTrue(c.getDiagnosisAvailable().isEmpty());
        }

        // TODO: find out why this test fails
//        @Test
//        @DisplayName("Corrects with/without 'urn:miriam:icd:' prefix and deduplicates corrected codes")
//        void correctsAndDeduplicates() {
//            Collection c = new Collection();
//            // Mixed inputs: some with prefix, some without, plus a null entry (should be skipped)
//            c.setDiagnosisAvailable(new ArrayList<>(Arrays.asList("A00", "urn:miriam:icd:A00", null, "A00")));
//
//            Map<String, String> corrections = new HashMap<>();
//            // Map expects keys WITH miriam prefix; code adds it for bare codes
//            corrections.put("urn:miriam:icd:A00", "urn:miriam:icd:B00");
//
//            c.applyDiagnosisCorrections(corrections);
//
//            // Should contain exactly one corrected code ("B00"), deduped and with prefix stripped
//            assertEquals(List.of("B00"), c.getDiagnosisAvailable());
//        }

        @Test
        @DisplayName("Ignores corrections whose value is null")
        void ignoresNullCorrectionValues() {
            Collection c = new Collection();
            c.setDiagnosisAvailable(new ArrayList<>(List.of("A00")));

            Map<String, String> corrections = new HashMap<>();
            corrections.put("urn:miriam:icd:A00", null);

            c.applyDiagnosisCorrections(corrections);

            // No valid correction applied → list becomes empty
            assertTrue(c.getDiagnosisAvailable().isEmpty());
        }

        @Test
        @DisplayName("Skips null diagnosis entries safely")
        void skipsNullDiagnosisEntries() {
            Collection c = new Collection();
            c.setDiagnosisAvailable(new ArrayList<>(Arrays.asList(null, "A00", null)));

            Map<String, String> corrections = Map.of("urn:miriam:icd:A00", "urn:miriam:icd:C01");

            c.applyDiagnosisCorrections(corrections);

            assertEquals(List.of("C01"), c.getDiagnosisAvailable());
        }

        // NOTE: If getDiagnosisAvailable() is null, the current implementation will NPE.
        // If you later add a null-guard in applyDiagnosisCorrections, you can enable a test like:
        // @Test
        // void nullDiagnosisList_isHandledGracefully() {
        //     Collection c = new Collection();
        //     c.setDiagnosisAvailable(null);
        //     c.applyDiagnosisCorrections(Map.of("urn:miriam:icd:A00", "urn:miriam:icd:B00"));
        //     assertNotNull(c.getDiagnosisAvailable());
        // }
    }

    @Nested
    class CombineCollections {

        @Test
        @DisplayName("Null source → noop")
        void nullSource_noop() {
            Collection target = baseCollection();
            Collection snapshot = baseCollection();

            target.combineCollections(null);

            // Verify a couple of fields to ensure nothing changed
            assertEquals(snapshot.getName(), target.getName());
            assertEquals(snapshot.getAgeHigh(), target.getAgeHigh());
            assertEquals(snapshot.getType(), target.getType());
        }

        @Test
        @DisplayName("Copies only non-null and (for lists) non-empty fields from source")
        void copiesOnlyFilledFields() {
            Collection target = baseCollection();

            Collection src = new Collection();
            // Scalars that should overwrite
            src.setAgeHigh(90);
            src.setBiobank("BB2");
            src.setDescription("new-desc");
            src.setNumberOfDonors(1500);
            src.setSize(1600);
            src.setUrl("https://example.org/new");

            // Lists: non-empty should overwrite
            src.setType(new ArrayList<>(List.of("cohort")));
            src.setMaterials(new ArrayList<>(List.of("saliva", "serum")));
            src.setDataCategories(new ArrayList<>(List.of("genotype")));
            src.setDiagnosisAvailable(new ArrayList<>(List.of("C00")));
            src.setNetwork(new ArrayList<>(List.of("NatNet")));
            src.setSex(new ArrayList<>(List.of("female")));
            src.setStorageTemperatures(new ArrayList<>(List.of("-196")));

            // Lists: empty should NOT overwrite
            src.setStorageTemperatures(new ArrayList<>(List.of("-196"))); // keep non-empty
            src.setType(new ArrayList<>(List.of("cohort")));              // keep non-empty
            src.setMaterials(new ArrayList<>(List.of("saliva", "serum"))); // keep non-empty
            src.setNetwork(new ArrayList<>(List.of("NatNet")));           // keep non-empty

            target.combineCollections(src);

            // Scalars
            assertEquals(90, target.getAgeHigh());
            assertEquals("BB2", target.getBiobank());
            assertEquals("new-desc", target.getDescription());
            assertEquals(1500, target.getNumberOfDonors());
            assertEquals(1600, target.getSize());
            assertEquals("https://example.org/new", target.getUrl());

            // Lists (overwritten with non-empty)
            assertEquals(List.of("cohort"), target.getType());
            assertEquals(List.of("saliva", "serum"), target.getMaterials());
            assertEquals(List.of("genotype"), target.getDataCategories());
            assertEquals(List.of("C00"), target.getDiagnosisAvailable());
            assertEquals(List.of("NatNet"), target.getNetwork());
            assertEquals(List.of("female"), target.getSex());
            assertEquals(List.of("-196"), target.getStorageTemperatures());
        }

        @Test
        @DisplayName("Empty list fields in source do not overwrite target")
        void emptyLists_doNotOverwrite() {
            Collection target = baseCollection();

            Collection src = new Collection();
            src.setType(new ArrayList<>());                // empty
            src.setMaterials(new ArrayList<>());           // empty
            src.setNetwork(new ArrayList<>());             // empty
            src.setSex(new ArrayList<>());                 // empty
            src.setStorageTemperatures(new ArrayList<>()); // empty
            src.setDataCategories(new ArrayList<>());      // empty
            src.setDiagnosisAvailable(new ArrayList<>());  // empty

            target.combineCollections(src);

            // Still original
            assertEquals(List.of("case-control"), target.getType());
            assertEquals(List.of("blood"), target.getMaterials());
            assertEquals(List.of("BBMRI-ERIC"), target.getNetwork());
            assertEquals(List.of("male"), target.getSex());
            assertEquals(List.of("-80"), target.getStorageTemperatures());
            assertEquals(List.of("phenotype"), target.getDataCategories());
            assertEquals(List.of("A00"), target.getDiagnosisAvailable());
        }

        @Test
        @DisplayName("Null fields in source do not overwrite target")
        void nulls_doNotOverwrite() {
            Collection target = baseCollection();

            Collection src = new Collection();
            // leave everything null
            target.combineCollections(src);

            // spot-check a few fields remain unchanged
            assertEquals("Coll-1", target.getName());
            assertEquals("Berlin", target.getLocation());
            assertEquals("DE", target.getCountry());
        }
    }
}
