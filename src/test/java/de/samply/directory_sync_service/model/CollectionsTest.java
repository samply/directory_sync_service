package de.samply.directory_sync_service.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CollectionsTest {

    private static Collection collWithCountry(String id, String country) {
        Collection c = new Collection();
        c.setId(id);
        c.setCountry(country);
        return c;
    }

    private static Collection collWithName(String id, String name) {
        Collection c = new Collection();
        c.setId(id);
        c.setName(name);
        return c;
    }

    @Test
    @DisplayName("isEmpty/size reflect addCollection; null add is ignored")
    void sizeAndEmpty() {
        Collections cs = new Collections();
        assertTrue(cs.isEmpty());
        assertEquals(0, cs.size());

        cs.addCollection("A", collWithCountry("A", "DE"));
        assertFalse(cs.isEmpty());
        assertEquals(1, cs.size());

        cs.addCollection("B", null); // ignored
        assertEquals(1, cs.size());
    }

    @Test
    @DisplayName("addCollection merges into existing key via combineCollections")
    void addCollection_mergesDuplicates() {
        Collections cs = new Collections();

        // existing
        Collection existing = collWithName("K1", "OldName");
        cs.addCollection("K1", existing);

        // incoming should overwrite non-null fields (combineCollections)
        Collection incoming = collWithName("K1", "NewName");
        cs.addCollection("K1", incoming);

        Collection stored = cs.getCollection("K1");
        assertSame(existing, stored, "Existing instance should be retained");
        assertEquals("NewName", stored.getName(), "Name should be overwritten by combine");
        assertEquals(1, cs.size(), "No new entry should be created for same key");
    }

    @Test
    @DisplayName("getCollections returns snapshot/unmodifiable list")
    void getCollections_unmodifiable() {
        Collections cs = new Collections();
        cs.addCollection("A", collWithCountry("A", "DE"));
        cs.addCollection("B", collWithCountry("B", "DE"));

        java.util.List<Collection> list = cs.getCollections();
        assertEquals(2, list.size());
        assertThrows(UnsupportedOperationException.class, () -> list.add(new Collection()));
    }

    @Test
    @DisplayName("getCollectionIds returns keys in natural (sorted) order")
    void getCollectionIds_sorted() {
        Collections cs = new Collections();
        cs.addCollection("B", collWithCountry("B", "DE"));
        cs.addCollection("A", collWithCountry("A", "DE"));
        cs.addCollection("C", collWithCountry("C", "DE"));

        assertEquals(List.of("A", "B", "C"), cs.getCollectionIds());
    }

    @Test
    @DisplayName("getOrDefault returns existing if present, otherwise provided default (does not insert)")
    void getOrDefault_behavior() {
        Collections cs = new Collections();
        Collection def = new Collection();

        // absent -> default
        Collection got = cs.getOrDefault("X", def);
        assertSame(def, got);
        assertEquals(0, cs.size(), "getOrDefault should not insert the default");

        // present -> ignore default
        Collection c = collWithName("Y", "Name");
        cs.addCollection("Y", c);
        Collection got2 = cs.getOrDefault("Y", def);
        assertSame(c, got2);
    }

    @Nested
    class GetCountryCode {

        @Test
        @DisplayName("Returns first non-empty country from contained collections")
        void fromCollectionCountry() {
            Collections cs = new Collections();
            cs.addCollection("X1", collWithCountry("X1", ""));       // empty
            cs.addCollection("X2", collWithCountry("X2", "DE"));     // found
            cs.addCollection("X3", collWithCountry("X3", "FR"));     // not consulted

            assertEquals("DE", cs.getCountryCode());
        }

        @Test
        @DisplayName("Falls back to deriving from collection ID if countries are empty")
        void fromCollectionId() {
            Collections cs = new Collections();
            // No country set, derive from ID "bbmri-eric:ID:AT_foo"
            Collection c = new Collection();
            c.setId("bbmri-eric:ID:AT_foo");
            c.setCountry(""); // empty, so fallback used
            cs.addCollection(c.getId(), c);

            assertEquals("AT", cs.getCountryCode());
        }

        @Test
        @DisplayName("Empty collection set -> null")
        void emptyReturnsNull() {
            Collections cs = new Collections();
            assertNull(cs.getCountryCode());
        }

        @Test
        @DisplayName("Unparseable ID -> method catches and returns null")
        void badIdReturnsNull() {
            Collections cs = new Collections();
            Collection c = new Collection();
            c.setId("NOT-BBMRI-FORMAT");
            c.setCountry(""); // forces fallback to ID
            cs.addCollection(c.getId(), c);

            // Current implementation logs an exception internally and returns null
            assertNull(cs.getCountryCode());
        }
    }

    @Nested
    class ApplyDiagnosisCorrections {

        @Test
        @DisplayName("Null map -> no changes on any collection")
        void nullMap_noop() {
            Collections cs = new Collections();
            Collection c1 = new Collection();
            c1.setDiagnosisAvailable(new ArrayList<>(List.of("A00")));
            cs.addCollection("K1", c1);

            cs.applyDiagnosisCorrections(null);

            assertEquals(List.of("A00"), c1.getDiagnosisAvailable());
        }

        @Test
        @DisplayName("Delegates to each collection: applies corrections and deduplicates")
        void appliesToAllCollections() {
            Collections cs = new Collections();

            Collection c1 = new Collection();
            c1.setDiagnosisAvailable(new ArrayList<>(List.of("A00", "urn:miriam:icd:A00")));
            cs.addCollection("K1", c1);

            Collection c2 = new Collection();
            c2.setDiagnosisAvailable(new ArrayList<>(List.of("B01")));
            cs.addCollection("K2", c2);

            Map<String, String> corrections = new HashMap<>();
            corrections.put("urn:miriam:icd:A00", "urn:miriam:icd:C10");
            corrections.put("urn:miriam:icd:B01", "urn:miriam:icd:D20");

            cs.applyDiagnosisCorrections(corrections);

            // After corrections, each collection list should contain stripped corrected codes
            assertEquals(List.of("C10"), c1.getDiagnosisAvailable());
            assertEquals(List.of("D20"), c2.getDiagnosisAvailable());
        }
    }
}
