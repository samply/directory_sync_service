package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.model.StarModelInput;
import de.samply.directory_sync_service.model.StarModelInputRow;
import de.samply.directory_sync_service.model.FactTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class CreateFactTablesFromStarModelInputDataTest {

    private static final String COLL = "bbmri-eric:ID:DE_COLL1";

    private StarModelInput baseInput() {
        StarModelInput input = new StarModelInput();
        input.setMinDonors(1); // default low unless a test sets higher
        return input;
    }

    private StarModelInputRow row(String collection, String material, String id, String sex, String age, String histLoc) {
        StarModelInputRow r = new StarModelInputRow(collection, material, id, sex, age);
        // hist_loc is used as “disease” later; set via setter (which prefixes MIRIAM for 3/5-length codes)
        r.setHistLoc(histLoc);
        return r;
    }

    private List<Map<String,String>> facts(FactTable ft) {
        return ft.getFactTables();
    }

    @Nested
    class TransformationsAndAggregation {

        @Test
        @DisplayName("Transforms: age bin, sex, material; aggregates donor/sample counts")
        void transformsAndAggregates() {
            StarModelInput input = baseInput();
            // Two rows same patient (p1) → donors=1, samples=2 after grouping
            input.addInputRow(COLL, row(COLL, "FFPE", "p1", "female", "47", "C10"));   // FFPE → TISSUE_PARAFFIN_EMBEDDED; female → FEMALE; age 47 → Middle-aged
            input.addInputRow(COLL, row(COLL, "FFPE", "p1", "female", "47", "C10"));

            FactTable ft = CreateFactTablesFromStarModelInputData.createFactTables(input, /*maxFacts*/ -1);
            List<Map<String,String>> facts = facts(ft);

            assertEquals(1, facts.size(), "Rows with identical grouping keys should collapse to one fact");

            Map<String,String> f = facts.get(0);
            assertEquals("FEMALE", f.get("sex"));
            assertEquals("urn:miriam:icd:C10", f.get("disease")); // set via StarModelInputRow#setHistLoc
            assertEquals("Middle-aged", f.get("age_range"));      // 45–64 bucket
            assertEquals("TISSUE_PARAFFIN_EMBEDDED", f.get("sample_type"));
            assertEquals("1", f.get("number_of_donors"));         // distinct(p1) = 1
            assertEquals("2", f.get("number_of_samples"));        // two rows
            assertEquals(COLL, f.get("collection"));
            assertEquals(LocalDate.now().toString(), f.get("last_update"));

            // id prefix: "bbmri-eric:factID:" + substring after "bbmri-eric:ID:"
            assertTrue(f.get("id").startsWith("bbmri-eric:factID:DE_COLL1:"), "Fact id should be stubbed from collection id");
        }

        @Test
        @DisplayName("Age bin edges: Unknown, Newborn, Infant, Child, Adolescent, Young Adult, Adult, Middle-aged, Aged (65-79), Aged (>80)")
        void ageBinningEdges() {
            StarModelInput input = baseInput();
            // create rows with different ages but different patient ids to avoid merging
            input.addInputRow(COLL, row(COLL, "Other", "u1", "X", "",     "C10")); // Unknown
            input.addInputRow(COLL, row(COLL, "Other", "u2", "X", "0",    "C10")); // Newborn
            input.addInputRow(COLL, row(COLL, "Other", "u3", "X", "1",    "C10")); // Infant
            input.addInputRow(COLL, row(COLL, "Other", "u4", "X", "7",    "C10")); // Child
            input.addInputRow(COLL, row(COLL, "Other", "u5", "X", "14",   "C10")); // Adolescent
            input.addInputRow(COLL, row(COLL, "Other", "u6", "X", "20",   "C10")); // Young Adult
            input.addInputRow(COLL, row(COLL, "Other", "u7", "X", "30",   "C10")); // Adult
            input.addInputRow(COLL, row(COLL, "Other", "u8", "X", "50",   "C10")); // Middle-aged
            input.addInputRow(COLL, row(COLL, "Other", "u9", "X", "70",   "C10")); // Aged (65-79 years)
            input.addInputRow(COLL, row(COLL, "Other", "uA", "X", "85",   "C10")); // Aged (>80 years)

            FactTable ft = CreateFactTablesFromStarModelInputData.createFactTables(input, -1);
            // Group by age_range to read off bins
            Map<String, Long> byAgeRange = facts(ft).stream()
                    .collect(Collectors.groupingBy(f -> f.get("age_range"), Collectors.counting()));

            assertTrue(byAgeRange.getOrDefault("Unknown", 0L) > 0);
            assertTrue(byAgeRange.getOrDefault("Newborn", 0L) > 0);
            assertTrue(byAgeRange.getOrDefault("Infant", 0L) > 0);
            assertTrue(byAgeRange.getOrDefault("Child", 0L) > 0);
            assertTrue(byAgeRange.getOrDefault("Adolescent", 0L) > 0);
            assertTrue(byAgeRange.getOrDefault("Young Adult", 0L) > 0);
            assertTrue(byAgeRange.getOrDefault("Adult", 0L) > 0);
            assertTrue(byAgeRange.getOrDefault("Middle-aged", 0L) > 0);
            assertTrue(byAgeRange.getOrDefault("Aged (65-79 years)", 0L) > 0);
            assertTrue(byAgeRange.getOrDefault("Aged (>80 years)", 0L) > 0);
        }
    }

    @Nested
    class FilteringAndTruncation {

        @Test
        @DisplayName("Entries below minDonors are filtered out")
        void minDonorsFilters() {
            StarModelInput input = baseInput();
            input.setMinDonors(2);

            // under-donor group (same patient twice), but different AGE_RANGE bucket ("Adult")
            input.addInputRow(COLL, row(COLL, "FFPE", "p1", "female", "30", "C10"));
            input.addInputRow(COLL, row(COLL, "FFPE", "p1", "female", "30", "C10"));

            // valid group (two distinct patients) in another AGE_RANGE bucket ("Middle-aged")
            input.addInputRow(COLL, row(COLL, "FFPE", "p2", "female", "47", "C10"));
            input.addInputRow(COLL, row(COLL, "FFPE", "p3", "female", "47", "C10"));

            FactTable ft = CreateFactTablesFromStarModelInputData.createFactTables(input, -1);

            // Only one fact should survive (the group with p2+p3)
            assertEquals(1, facts(ft).size());
            assertEquals("2", facts(ft).get(0).get("number_of_donors"));
            assertEquals("2", facts(ft).get(0).get("number_of_samples"));
        }

        @Test
        @DisplayName("maxFacts limits the number of facts per collection")
        void maxFactsLimits() {
            StarModelInput input = baseInput();
            // Create 3 distinct groups differing by sex or material or age
            input.addInputRow(COLL, row(COLL, "FFPE", "p1", "female", "47", "C10"));
            input.addInputRow(COLL, row(COLL, "FFPE", "p2", "male",   "47", "C10"));
            input.addInputRow(COLL, row(COLL, "Other","p3", "female", "20", "C10"));

            FactTable ft = CreateFactTablesFromStarModelInputData.createFactTables(input, /*maxFacts*/ 1);
            assertEquals(1, facts(ft).size(), "Should be truncated to 1 fact");
        }
    }

    @Nested
    class MultiCollectionSupport {

        @Test
        @DisplayName("Multiple collections produce combined fact table with proper id prefixes")
        void multipleCollections() {
            StarModelInput input = baseInput();
            String coll2 = "bbmri-eric:ID:AT_COLLX";

            input.addInputRow(COLL,  row(COLL,  "FFPE", "p1", "female", "47", "C10"));
            input.addInputRow(coll2, row(coll2, "FFPE", "q1", "male",   "47", "C10"));

            FactTable ft = CreateFactTablesFromStarModelInputData.createFactTables(input, -1);
            List<Map<String,String>> list = facts(ft);

            assertEquals(2, list.size());

            Map<String,String> de = list.stream().filter(m -> COLL.equals(m.get("collection"))).findFirst().orElseThrow();
            Map<String,String> at = list.stream().filter(m -> coll2.equals(m.get("collection"))).findFirst().orElseThrow();

            assertTrue(de.get("id").startsWith("bbmri-eric:factID:DE_COLL1:"));
            assertTrue(at.get("id").startsWith("bbmri-eric:factID:AT_COLLX:"));
        }
    }

    @Nested
    class EdgeCases {

        @Test
        @DisplayName("Empty input yields empty fact table")
        void emptyInput() {
            StarModelInput input = baseInput();
            FactTable ft = CreateFactTablesFromStarModelInputData.createFactTables(input, -1);
            assertTrue(facts(ft).isEmpty());
        }

        @Test
        @DisplayName("Null/unknown sex or material become empty/unchanged but still aggregate")
        void unknownsHandled() {
            StarModelInput input = baseInput();
            // StarModelInputRow requires non-null constructor values; pass placeholders and then null-out via setters where allowed:
            StarModelInputRow r = new StarModelInputRow(COLL, "Other", "p1", "unknown", "30");
            r.setHistLoc("C10");
            // setSampleMaterial(null) is ignored by the row setter, so we simulate unknown by leaving “Other” (which maps to "OTHER")
            input.addInputRow(COLL, r);

            FactTable ft = CreateFactTablesFromStarModelInputData.createFactTables(input, -1);
            assertEquals(1, facts(ft).size());
            assertEquals("OTHER", facts(ft).get(0).get("sample_type"));
        }
    }
}
