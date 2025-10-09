package de.samply.directory_sync_service.directory.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DirectoryCollectionPutTest {

    @Test
    @DisplayName("Constructor initializes 'entities' as an empty List")
    void ctor_initializesEntities() {
        DirectoryCollectionPut put = new DirectoryCollectionPut();

        Object entities = put.get("entities");
        assertNotNull(entities);
        assertTrue(entities instanceof List<?>);
        assertTrue(((List<?>) entities).isEmpty());
    }

    @Nested
    class GetEntityTests {
        @Test
        @DisplayName("getEntity creates and returns a new entity if ID not present")
        void getEntity_createsWhenMissing() {
            DirectoryCollectionPut put = new DirectoryCollectionPut();

            DirectoryCollectionPut.Entity e = put.getEntity("COLL-1");
            assertNotNull(e);
            assertEquals("COLL-1", e.getId());

            // Ensure it was added to the entities list
            List<?> entities = (List<?>) put.get("entities");
            assertEquals(1, entities.size());
            assertSame(e, entities.get(0));
        }

        @Test
        @DisplayName("getEntity returns existing entity for same ID (no duplicates)")
        void getEntity_returnsExisting() {
            DirectoryCollectionPut put = new DirectoryCollectionPut();

            DirectoryCollectionPut.Entity e1 = put.getEntity("COLL-1");
            DirectoryCollectionPut.Entity e2 = put.getEntity("COLL-1");

            assertSame(e1, e2);

            List<?> entities = (List<?>) put.get("entities");
            assertEquals(1, entities.size());
        }

        @Test
        @DisplayName("setId updates id and sets/updates timestamp")
        void setId_setsTimestamp() {
            DirectoryCollectionPut put = new DirectoryCollectionPut();
            DirectoryCollectionPut.Entity e = put.getEntity("A");

            // timestamp created on construction via setId
            assertNotNull(e.get("timestamp"));
            assertTrue(((String) e.get("timestamp")).length() >= 20);

            // change ID; timestamp should be refreshed
            String oldTs = (String) e.get("timestamp");
            try {
                Thread.sleep(1100); // wait more than a second, to stop old and new timestamps colliding
            } catch (java.lang.Exception ex) {
                throw new RuntimeException(ex);
            }
            e.setId("B");
            assertEquals("B", e.getId());
            String newTs = (String) e.get("timestamp");
            assertNotNull(newTs);
            assertNotEquals(oldTs, newTs);

            // basic shape check: "yyyy-MM-dd'T'HH:mm:ss'Z'"
            // We can't parse strictly because 'Z' is literal here, but check formatting anchors:
            assertTrue(newTs.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z"));
        }
    }

    @Nested
    class SetterGuards {
        @Test
        @DisplayName("String setters ignore null/empty")
        void stringSetters_ignoreNullEmpty() {
            DirectoryCollectionPut put = new DirectoryCollectionPut();
            DirectoryCollectionPut.Entity e = put.getEntity("X");

            e.setName(null);
            e.setName("");
            e.setCountry(null);
            e.setCountry("");
            e.setContact(null);
            e.setContact("");
            e.setBiobank(null);
            e.setBiobank("");
            e.setHead(null);
            e.setHead("");
            e.setLocation(null);
            e.setLocation("");
            e.setUrl(null);
            e.setUrl("");
            e.setDescription(null);
            e.setDescription("");

            // None of these keys should be present
            for (String k : List.of("name","country","contact","biobank","head","location","url","description")) {
                assertFalse(e.containsKey(k), "Key should not be present: " + k);
            }
        }

        @Test
        @DisplayName("List setters store empty list when given null")
        void listSetters_nullBecomesEmptyList() {
            DirectoryCollectionPut put = new DirectoryCollectionPut();
            DirectoryCollectionPut.Entity e = put.getEntity("X");

            e.setType(null);
            e.setMaterials(null);
            e.setStorageTemperatures(null);
            e.setDiagnosisAvailable(null);
            e.setSex(null);
            e.setDataCategories(null);
            e.setNetworks(null);

            Map<String, Object> m = e;
            // Keys should exist and point to (empty) lists
            for (String k : List.of("type","materials","storage_temperatures","diagnosis_available","sex","data_categories","network")) {
                assertTrue(m.containsKey(k), "Missing key: " + k);
                assertTrue(m.get(k) instanceof List<?>);
                assertTrue(((List<?>) m.get(k)).isEmpty(), "Expected empty list for: " + k);
            }
        }

        @Test
        @DisplayName("Integer setters ignore null where coded to do so")
        void integerSetters_ignoreNull() {
            DirectoryCollectionPut put = new DirectoryCollectionPut();
            DirectoryCollectionPut.Entity e = put.getEntity("X");

            e.setSize(null);
            e.setOrderOfMagnitude(null);
            e.setNumberOfDonors(null);
            e.setOrderOfMagnitudeDonors(null);

            for (String k : List.of("size","order_of_magnitude","number_of_donors","order_of_magnitude_donors")) {
                assertFalse(e.containsKey(k));
            }

            // Age low/high do not guard null (class currently puts value blindly)
            e.setAgeLow(null);
            e.setAgeHigh(null);
            assertTrue(e.containsKey("age_low"));
            assertTrue(e.containsKey("age_high"));
            assertNull(e.get("age_low"));
            assertNull(e.get("age_high"));
        }
    }

    @Test
    @DisplayName("getCollectionIds returns IDs of all entities in order")
    void getCollectionIds_returnsAll() {
        DirectoryCollectionPut put = new DirectoryCollectionPut();
        put.getEntity("A");
        put.getEntity("B");
        put.getEntity("C");

        assertEquals(List.of("A","B","C"), put.getCollectionIds());
    }

    @Nested
    class GetCountryCodeTests {
        @Test
        @DisplayName("Returns explicit country when present on first entity")
        void returnsExplicitCountry() {
            DirectoryCollectionPut put = new DirectoryCollectionPut();
            put.setCountry("A", "DE");
            put.getEntity("B"); // additional entities shouldn't matter

            assertEquals("DE", put.getCountryCode());
        }

        @Test
        @DisplayName("No entities -> getCountryCode returns null")
        void noEntities_returnsNull() {
            DirectoryCollectionPut put = new DirectoryCollectionPut();
            // entities list exists but is empty
            assertNull(put.getCountryCode());
        }

        @Test
        @DisplayName("No country on first entity and unparseable ID -> returns null (exception path handled)")
        void fallbackUnparseableId_returnsNull() {
            DirectoryCollectionPut put = new DirectoryCollectionPut();
            // First entity: no country, and some ID that BbmriEricId.valueOf likely can't parse
            put.getEntity("NOT-BBMRI-FORMAT");

            assertNull(put.getCountryCode());
        }

        // OPTIONAL: If you add Mockito-inline, you can mock BbmriEricId.valueOf to return a non-empty Optional
        // and assert the fallback returns that country code.
    }
}
