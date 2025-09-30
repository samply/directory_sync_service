package de.samply.directory_sync_service.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StarModelInputTest {

    @Nested
    class MinDonors {
        @Test
        @DisplayName("Default minDonors is 10 and can be updated")
        void minDonors_default_and_setter() {
            StarModelInput smi = new StarModelInput();
            assertEquals(10, smi.getMinDonors());

            smi.setMinDonors(25);
            assertEquals(25, smi.getMinDonors());
        }
    }

    @Nested
    class AddAndRetrieveRows {
        @Test
        @DisplayName("addInputRow creates bucket and accumulates rows per collection")
        void addInputRow_createsBucket_and_accumulates() {
            StarModelInput smi = new StarModelInput();

            StarModelInputRow r1 = new StarModelInputRow("colA", "blood_serum", "p1", "female", "45");
            r1.setHistLoc("C75"); // -> urn:miriam:icd:C75

            StarModelInputRow r2 = new StarModelInputRow("colA", "plasma-vital", "p2", "MALE", "38");
            r2.setHistLoc("E23.1"); // -> urn:miriam:icd:E23.1

            StarModelInputRow r3 = new StarModelInputRow("colB", "tissue", "p3", "female", "50");

            smi.addInputRow("colA", r1);
            smi.addInputRow("colA", r2);
            smi.addInputRow("colB", r3);

            // IDs present
            List<String> ids = smi.getInputCollectionIds();
            assertTrue(ids.containsAll(List.of("colA", "colB")));
            assertEquals(2, ids.size());

            // Rows for colA
            List<Map<String,String>> colA = smi.getInputRowsAsStringMaps("colA");
            assertEquals(2, colA.size());

            // Check conversions via StarModelInputRow.asMap()
            Map<String,String> first = colA.get(0);
            assertEquals("colA", first.get("collection"));
            assertEquals("SERUM", first.get("sample_material"));  // blood_serum -> SERUM
            assertEquals("p1", first.get("id"));
            assertEquals("FEMALE", first.get("sex"));             // female -> FEMALE
            assertEquals("45", first.get("age_at_primary_diagnosis"));
            assertEquals("urn:miriam:icd:C75", first.get("hist_loc"));

            Map<String,String> second = colA.get(1);
            assertEquals("PLASMA", second.get("sample_material")); // plasma-vital -> PLASMA
            assertEquals("MALE", second.get("sex"));
            assertEquals("urn:miriam:icd:E23.1", second.get("hist_loc"));

            // Rows for colB
            List<Map<String,String>> colB = smi.getInputRowsAsStringMaps("colB");
            assertEquals(1, colB.size());
            assertEquals("TISSUE_FROZEN", colB.get(0).get("sample_material")); // tissue -> TISSUE_FROZEN
        }

        @Test
        @DisplayName("getInputCollectionIds returns an unmodifiable copy")
        void getInputCollectionIds_unmodifiable() {
            StarModelInput smi = new StarModelInput();
            smi.addInputRow("X", new StarModelInputRow("X", "SERUM", "p", "F", "30"));

            List<String> ids = smi.getInputCollectionIds();
            assertThrows(UnsupportedOperationException.class, () -> ids.add("Y"));
        }

        @Test
        @DisplayName("getInputRowsAsStringMaps throws NPE for unknown collection (current behavior)")
        void getInputRowsAsStringMaps_unknownCollection_throwsNPE() {
            StarModelInput smi = new StarModelInput();
            assertThrows(NullPointerException.class, () -> smi.getInputRowsAsStringMaps("missing"));
        }
    }
}
