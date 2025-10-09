package de.samply.directory_sync_service.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StarModelInputRowTest {

    @Nested
    class ConstructorsAndAsMap {

        @Test
        @DisplayName("Primary constructor sets and converts fields; asMap contains all keys")
        void ctor_setsFields_and_asMap() {
            StarModelInputRow row = new StarModelInputRow(
                    "bbmri-eric:ID:DE_ABC",
                    "blood_serum",   // -> SERUM
                    "pat-1",
                    "female",        // -> FEMALE
                    "45"
            );

            Map<String,String> m = row.asMap();
            assertEquals("bbmri-eric:ID:DE_ABC", m.get("collection"));
            assertEquals("SERUM", m.get("sample_material")); // via converter
            assertEquals("pat-1", m.get("id"));
            assertEquals("FEMALE", m.get("sex"));            // via converter
            assertEquals("45", m.get("age_at_primary_diagnosis"));
            assertTrue(m.containsKey("hist_loc"));           // present (may be null)
        }

        @Test
        @DisplayName("Copy+diagnosis ctor copies fields and converts hist_loc to MIRIAM")
        void copyConstructor_convertsDiagnosis() {
            StarModelInputRow base = new StarModelInputRow(
                    "bbmri-eric:ID:DE_X",
                    "plasma-vital",     // -> PLASMA
                    "p-123",
                    "MALE",             // -> MALE
                    "60"
            );
            StarModelInputRow withDx = new StarModelInputRow(base, "C75"); // -> urn:miriam:icd:C75

            Map<String,String> m = withDx.asMap();
            assertEquals("bbmri-eric:ID:DE_X", m.get("collection"));
            assertEquals("PLASMA", m.get("sample_material"));
            assertEquals("p-123", m.get("id"));
            assertEquals("MALE", m.get("sex"));
            assertEquals("60", m.get("age_at_primary_diagnosis"));
            assertEquals("urn:miriam:icd:C75", m.get("hist_loc"));
        }
    }

    @Nested
    class SetterNullGuardsAndConversions {

        @Test
        @DisplayName("setSex uppercases; null input leaves previous value unchanged")
        void setSex_converts_and_nullIgnored() {
            StarModelInputRow row = new StarModelInputRow("col","SERUM","p","female","30");
            assertEquals("FEMALE", row.getSex());
            row.setSex(null); // ignored
            assertEquals("FEMALE", row.getSex());
        }

        @Test
        @DisplayName("setSampleMaterial normalizes; null input leaves previous value unchanged")
        void setSampleMaterial_converts_and_nullIgnored() {
            StarModelInputRow row = new StarModelInputRow("col","blood_serum","p","F","30");
            assertEquals("SERUM", row.getSampleMaterial());
            row.setSampleMaterial(null); // ignored
            assertEquals("SERUM", row.getSampleMaterial());
        }

        @Test
        @DisplayName("setHistLoc converts to MIRIAM or null when invalid; null input ignored")
        void setHistLoc_conversion_and_nullIgnored() {
            StarModelInputRow row = new StarModelInputRow("col","SERUM","p","F","30");

            row.setHistLoc("E23.1"); // valid -> MIRIAM
            assertEquals("urn:miriam:icd:E23.1", row.asMap().get("hist_loc"));

            // Invalid diagnosis returns null from converter; field becomes null
            row.setHistLoc("WXYZ");
            assertNull(row.asMap().get("hist_loc"));

            // Passing null to setter is ignored (keeps current null)
            row.setHistLoc(null);
            assertNull(row.asMap().get("hist_loc"));
        }

        @Test
        @DisplayName("setCollection and setId ignore nulls (keep previous)")
        void setCollection_setId_nullIgnored() {
            StarModelInputRow row = new StarModelInputRow("col1","SERUM","p-1","M","40");
            assertEquals("col1", row.getCollection());
            assertEquals("p-1", row.getId());

            row.setCollection(null);
            row.setId(null);

            assertEquals("col1", row.getCollection());
            assertEquals("p-1", row.getId());
        }

        @Test
        @DisplayName("setAgeAtPrimaryDiagnosis stores string; null input ignored")
        void setAgeAtPrimaryDiagnosis_nullIgnored() {
            StarModelInputRow row = new StarModelInputRow("col","SERUM","p","M","50");
            assertEquals("50", row.getAgeAtPrimaryDiagnosis());

            row.setAgeAtPrimaryDiagnosis(null); // ignored per implementation
            assertEquals("50", row.getAgeAtPrimaryDiagnosis());
        }
    }

    @Nested
    class NewInputRowFactory {

        @Test
        @DisplayName("newInputRow copies from base and converts diagnosis")
        void newInputRow_ok() {
            StarModelInputRow base = new StarModelInputRow("col","SERUM","p","male","33");
            StarModelInputRow copy = StarModelInputRow.newInputRow(base, "A00");
            assertEquals("col", copy.getCollection());
            assertEquals("SERUM", copy.getSampleMaterial());
            assertEquals("p", copy.getId());
            assertEquals("MALE", copy.getSex());
            assertEquals("33", copy.getAgeAtPrimaryDiagnosis());
            assertEquals("urn:miriam:icd:A00", copy.asMap().get("hist_loc"));
        }

        @Test
        @DisplayName("newInputRow(null, ...) throws NPE (current behavior)")
        void newInputRow_nullRow_throws() {
            assertThrows(NullPointerException.class,
                    () -> StarModelInputRow.newInputRow(null, "A00"));
        }

        @Test
        @DisplayName("newInputRow(row, null): current impl does NOT throw; hist_loc remains null")
        void newInputRow_nullHistLoc_currentBehavior() {
            StarModelInputRow base = new StarModelInputRow("col","SERUM","p","F","20");
            StarModelInputRow copy = StarModelInputRow.newInputRow(base, null); // setHistLoc ignores null
            assertNull(copy.asMap().get("hist_loc"));
        }
    }
}
