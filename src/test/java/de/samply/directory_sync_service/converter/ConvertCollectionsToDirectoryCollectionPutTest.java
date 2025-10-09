package de.samply.directory_sync_service.converter;

import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.Collection;
import de.samply.directory_sync_service.model.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConvertCollectionsToDirectoryCollectionPutTest {

    private static Collection mkCollection(
            String id,
            Integer size,
            Integer donors,
            List<String> sex,
            List<String> materials,
            List<String> temps,
            List<String> diagnoses
    ) {
        Collection c = new Collection();
        c.setId(id);
        if (size != null) c.setSize(size);
        if (donors != null) c.setNumberOfDonors(donors);
        if (sex != null) c.setSex(sex);
        if (materials != null) c.setMaterials(materials);
        if (temps != null) c.setStorageTemperatures(temps);
        if (diagnoses != null) c.setDiagnosisAvailable(diagnoses);

        // a few pass-through directory attributes (to ensure they are set, not null)
        c.setName("Name " + id);
        c.setCountry("DE");
        c.setType(List.of("SAMPLE"));
        c.setDataCategories(List.of("BIOLOGICAL_SAMPLES"));
        return c;
    }

    private static Collections wrap(Collection... items) {
        Collections cs = new Collections();
        for (Collection c : items) {
            cs.addCollection(c.getId(), c);
        }
        return cs;
    }

    @SuppressWarnings("unchecked")
    private static List<String> getList(Map<String, Object> entity, String key) {
        Object v = entity.get(key);
        assertNotNull(v, "Expected list under key: " + key);
        assertTrue(v instanceof List, "Expected list under key: " + key);
        return (List<String>) v;
    }

    private static Integer getInt(Map<String, Object> entity, String key) {
        Object v = entity.get(key);
        assertNotNull(v, "Expected integer under key: " + key);
        assertTrue(v instanceof Integer, "Expected integer under key: " + key);
        return (Integer) v;
    }

    private static String getString(Map<String, Object> entity, String key) {
        Object v = entity.get(key);
        assertNotNull(v, "Expected string under key: " + key);
        assertTrue(v instanceof String, "Expected string under key: " + key);
        return (String) v;
    }

    @Nested
    @DisplayName("Happy path conversions")
    class HappyPath {

        @Test
        @DisplayName("Converts + deduplicates; computes order-of-magnitude; miriamizes diagnoses")
        void fullConversionAndDerivations() {
            List<String> gender = new ArrayList<>();
            gender.add("male");
            gender.add("FEMALE");
            gender.add("male");
            List<String> type = new ArrayList<>();
            type.add("ffpe");
            type.add("FFPE");
            type.add("csf_liquor");
            type.add("blood-serum");
            type.add(null);
            List<String> temp = new ArrayList<>();
            temp.add("temperatureGN");
            temp.add("temperature4to8");
            temp.add("temperatureGN");
            List<String> diag = new ArrayList<>();
            diag.add("C10");
            diag.add("E23.1");
            diag.add("urn:miriam:icd:C10");
            diag.add("xyz"); // "xyz" invalid -> dropped
            // Inputs chosen to trigger conversion + distinct + miriamization
            Collection c = mkCollection(
                    "bbmri-eric:ID:DE_ABC_COL_1",
                    123,                       // size -> order_of_magnitude = 2
                    45,                        // donors -> order_of_magnitude_donors = 1
                    gender,
                    type,
                    temp,
                    diag
            );

            DirectoryCollectionPut put = ConvertCollectionsToDirectoryCollectionPut.convert(wrap(c));
            assertNotNull(put);

            String id = c.getId();
            Map<String, Object> ent = put.getEntity(id);

            // numeric fields
            assertEquals(123, getInt(ent, "size"));
            assertEquals(2, getInt(ent, "order_of_magnitude"));
            assertEquals(45, getInt(ent, "number_of_donors"));
            assertEquals(1, getInt(ent, "order_of_magnitude_donors"));

            // pass-through bits we set above
            assertEquals("Name " + id, getString(ent, "name"));
            assertEquals("DE", put.getCountryCode());

            // sex normalized & distinct
            List<String> sex = getList(ent, "sex");
            assertTrue(sex.containsAll(List.of("MALE", "FEMALE")));
            assertEquals(2, sex.size());

            // materials mapping
            // csf_liquor -> OTHER
            // blood-serum -> SERUM   (whatever your converter maps to; adjust if needed)
            List<String> materials = getList(ent, "materials");
            assertTrue(materials.contains("OTHER"));
            assertTrue(materials.contains("SERUM"));
            assertEquals(3, materials.size());

            // storage temperatures: temperatureGN -> temperatureOther; duplicates removed
            List<String> temps = getList(ent, "storage_temperatures");
            assertTrue(temps.contains("temperatureOther"));
            assertTrue(temps.contains("temperature4to8"));
            assertEquals(2, temps.size());

            // diagnoses miriamized & deduped; invalid removed
            List<String> dx = getList(ent, "diagnosis_available");
            assertTrue(dx.contains("urn:miriam:icd:C10"));
            assertTrue(dx.contains("urn:miriam:icd:E23.1"));
            assertEquals(3, dx.size());
        }
    }

    @Nested
    @DisplayName("Null/empty handling")
    class NullAndEmpty {

        @Test
        @DisplayName("Null lists become empty; null elements filtered; distinct preserved")
        void nullAndEmptyLists() {
            List<String> gender = new ArrayList<>();
            gender.add("female");

            Collection c = mkCollection(
                    "bbmri-eric:ID:DE_XYZ_COL_1",
                    10,
                    10,
                    gender,
                    /*materials*/ null,
                    /*temps*/ null,
                    /*diagnoses*/ null
            );

            DirectoryCollectionPut put = ConvertCollectionsToDirectoryCollectionPut.convert(wrap(c));
            assertNotNull(put);

            Map<String, Object> ent = put.getEntity(c.getId());

            // Sex -> FEMALE only
            List<String> sex = getList(ent, "sex");
            assertEquals(List.of("FEMALE"), sex);

            // Materials/temps/diagnoses: empty lists (not null)
            assertEquals(0, getList(ent, "materials").size());
            assertEquals(0, getList(ent, "storage_temperatures").size());
            assertEquals(0, getList(ent, "diagnosis_available").size());
        }
    }

    @Nested
    @DisplayName("Error handling")
    class ErrorHandling {
        @Test
        @DisplayName("If any collection conversion throws, overall convert returns null")
        void failingCollectionAbortsAll() {
            // Good + Bad (bad: null size triggers Math.log10(null) → NPE in convert)
            Collection good = mkCollection(
                    "bbmri-eric:ID:DE_GOOD_COL",
                    10, 10,
                    List.of("male"),
                    List.of("FFPE"),
                    List.of("temperatureGN"),
                    List.of("C10")
            );
            Collection bad = mkCollection(
                    "bbmri-eric:ID:DE_BAD_COL",
                    null, // triggers exception in order_of_magnitude
                    10,
                    List.of("male"),
                    List.of("FFPE"),
                    List.of("temperatureGN"),
                    List.of("C10")
            );

            DirectoryCollectionPut put = ConvertCollectionsToDirectoryCollectionPut.convert(wrap(good, bad));
            assertNull(put, "Any failure should nullify the overall DirectoryCollectionPut");
        }
    }

    @Nested
    @DisplayName("Multiple collections wiring")
    class MultipleCollections {
        @Test
        @DisplayName("Values are stored under the correct collection IDs")
        void perCollectionSeparation() {
            Collection c1 = mkCollection("bbmri-eric:ID:DE_A", 11, 5,
                    List.of("male"), List.of("FFPE"), List.of("temperatureGN"), List.of("C10"));
            Collection c2 = mkCollection("bbmri-eric:ID:DE_B", 101, 20,
                    List.of("female"), List.of("TISSUE"), List.of("temperature4to8"), List.of("E23.1"));

            DirectoryCollectionPut put = ConvertCollectionsToDirectoryCollectionPut.convert(wrap(c1, c2));
            assertNotNull(put);

            // sanity: both IDs present
            assertTrue(put.getCollectionIds().containsAll(List.of(c1.getId(), c2.getId())));

            Map<String, Object> e1 = put.getEntity(c1.getId());
            Map<String, Object> e2 = put.getEntity(c2.getId());

            assertEquals(11, getInt(e1, "size"));
            assertFalse(getList(e1, "materials").contains("TISSUE_PARAFFIN_EMBEDDED"));
            assertTrue(getList(e1, "materials").contains("FFPE"));

            assertEquals(101, getInt(e2, "size"));
            assertTrue(getList(e2, "materials").contains("TISSUE_FROZEN"));
        }
    }
}
