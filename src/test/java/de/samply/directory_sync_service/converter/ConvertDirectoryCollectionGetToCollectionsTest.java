package de.samply.directory_sync_service.converter;

import de.samply.directory_sync_service.model.Collection;
import de.samply.directory_sync_service.model.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ConvertDirectoryCollectionGetToCollectionsTest {

    private static Map<String, Object> str(String value) {
        // convenience for scalar string attributes passed as raw strings
        return Map.of("value", value); // not used; kept in case you want variants
    }

    private static Map<String, Object> id(String id) {
        return Map.of("id", id);
    }

    private static Map<String, Object> name(String name) {
        return Map.of("name", name);
    }

    private static Map<String, Object> idName(String id, String nameVal) {
        return Map.of("id", id, "name", nameVal);
    }

    private static Map<String, Object> entityMap(
            Object biobank, Object contact, Object country, Object dataCategories,
            Object description, Object head, Object id, Object location, Object name,
            Object network, Object type, Object url) {

        Map<String, Object> m = new HashMap<>();
        if (biobank != null)       m.put("biobank", biobank);
        if (contact != null)       m.put("contact", contact);
        if (country != null)       m.put("country", country);
        if (dataCategories != null)m.put("data_categories", dataCategories);
        if (description != null)   m.put("description", description);
        if (head != null)          m.put("head", head);
        if (id != null)            m.put("id", id);
        if (location != null)      m.put("location", location);
        if (name != null)          m.put("name", name);
        if (network != null)       m.put("network", network);
        if (type != null)          m.put("type", type);
        if (url != null)           m.put("url", url);
        return m;
    }

    private static Collection only(Collections cols) {
        assertEquals(1, cols.getCollections().size(), "expected exactly one collection");
        return cols.getCollections().get(0);
    }

    @Nested
    @DisplayName("Happy path")
    class HappyPath {
        @Test
        @DisplayName("Maps all supported fields (strings + list-of-maps id/name)")
        void mapsAllFields() {
            Collections cols = new Collections();

            Map<String, Object> source = entityMap(
                    id("biobank-123"),                // biobank map -> id
                    id("contact-456"),                // contact map -> id
                    name("DE"),                       // country map -> name
                    List.of(id("BIOLOGICAL_SAMPLES"), name("IMAGING")), // data_categories list-of-maps
                    name("Desc"),                     // description -> name
                    id("Dr. Head"),                   // head -> id
                    id("bbmri-eric:ID:DE_ABC_COL_001"), // id map -> id
                    name("Berlin"),                   // location -> name
                    idName("ignored", "CRC"),         // name prefers id; here both given → id wins per code (id first), but getStringFromAttributeMap returns "id" if present
                    List.of(name("NetworkA"), id("NetworkB")), // network
                    List.of(name("SAMPLE")),          // type
                    "https://example.org"             // url as raw string
            );

            ConvertDirectoryCollectionGetToCollections.addCollectionFromMap(
                    cols, "bbmri-eric:ID:DE_ABC_COL_001", source);

            Collection c = only(cols);
            assertEquals("biobank-123", c.getBiobank());
            assertEquals("contact-456", c.getContact());
            assertEquals("DE", c.getCountry());
            assertEquals(List.of("BIOLOGICAL_SAMPLES", "IMAGING"), c.getDataCategories());
            assertEquals("Desc", c.getDescription());
            assertEquals("Dr. Head", c.getHead());
            assertEquals("bbmri-eric:ID:DE_ABC_COL_001", c.getId());
            assertEquals("Berlin", c.getLocation());
            // getStringFromAttributeMap prefers "id" if present
            assertEquals("ignored", c.getName());
            assertEquals(List.of("NetworkA", "NetworkB"), c.getNetwork());
            assertEquals(List.of("SAMPLE"), c.getType());
            assertEquals("https://example.org", c.getUrl());
        }

        @Test
        @DisplayName("List attributes accept mixtures of strings and maps")
        void listCanMixStringsAndMaps() {
            Collections cols = new Collections();
            Map<String, Object> source = entityMap(
                    null, null, null,
                    List.of("BIOLOGICAL_SAMPLES", name("IMAGING"), id("GENOMICS")),
                    null, null, id("X"), null, null,
                    List.of("Net1", id("Net2")), List.of("SAMPLE"), null
            );

            ConvertDirectoryCollectionGetToCollections.addCollectionFromMap(cols, "X", source);
            Collection c = only(cols);

            assertEquals(List.of("BIOLOGICAL_SAMPLES", "IMAGING", "GENOMICS"), c.getDataCategories());
            assertEquals(List.of("Net1", "Net2"), c.getNetwork());
            assertEquals(List.of("SAMPLE"), c.getType());
        }
    }

    @Nested
    @DisplayName("Invalid shapes & edge cases")
    class InvalidShapes {
        @Test
        @DisplayName("Non-list for list attributes → ignored (null)")
        void nonListListAttributesIgnored() {
            Collections cols = new Collections();
            Map<String, Object> source = entityMap(
                    null, null, null,
                    "not-a-list",    // data_categories (invalid shape)
                    null, null, id("Y"), null, null,
                    12345,           // network (invalid shape)
                    Map.of("oops", "nope"), // type (invalid shape)
                    null
            );
            ConvertDirectoryCollectionGetToCollections.addCollectionFromMap(cols, "Y", source);
            Collection c = only(cols);

            assertNull(c.getDataCategories(), "invalid non-list should result in null");
            assertNull(c.getNetwork(), "invalid non-list should result in null");
            assertNull(c.getType(), "invalid non-list should result in null");
        }

        @Test
        @DisplayName("Scalar attributes accept either String or Map(id/name); other types → null")
        void scalarAttributeTypes() {
            Collections cols = new Collections();
            Map<String, Object> source = entityMap(
                    "biobank-raw",              // String accepted
                    name("contact-name"),       // Map(name)
                    id("DE"),                   // Map(id)
                    null,
                    Map.of("unexpected", "x"),  // description: map without id/name → null
                    42,                         // head: invalid type → null
                    "COL_ID",                   // id: String (accepted)
                    List.of(),                  // location: invalid type (list) → null
                    idName("NID", "NameVal"),   // name: id wins
                    null, null,
                    name("https://x.test")      // url via map(name)
            );

            ConvertDirectoryCollectionGetToCollections.addCollectionFromMap(cols, "COL_ID", source);
            Collection c = only(cols);

            assertEquals("biobank-raw", c.getBiobank());
            assertEquals("contact-name", c.getContact());
            assertEquals("DE", c.getCountry());
            assertNull(c.getDescription());
            assertNull(c.getHead());
            assertEquals("COL_ID", c.getId()); // from String
            assertNull(c.getLocation());
            assertEquals("NID", c.getName());
            assertEquals("https://x.test", c.getUrl());
        }

        @Test
        @DisplayName("Empty map → still creates a (mostly empty) Collection")
        void emptyMapCreatesEmptyCollection() {
            Collections cols = new Collections();
            ConvertDirectoryCollectionGetToCollections.addCollectionFromMap(cols, "Z", new HashMap<>());

            Collection c = only(cols);
            // ID is only set if "id" key exists in the map; here it doesn't.
            assertNull(c.getId());
            assertNull(c.getBiobank());
            assertNull(c.getContact());
            assertNull(c.getCountry());
            assertNull(c.getDataCategories());
            assertNull(c.getDescription());
            assertNull(c.getHead());
            assertNull(c.getLocation());
            assertNull(c.getName());
            assertNull(c.getNetwork());
            assertNull(c.getType());
            assertNull(c.getUrl());
        }

        @Test
        @DisplayName("Adds a collection to Collections under the provided collectionId")
        void addsToCollections() {
            Collections cols = new Collections();
            Map<String, Object> source = entityMap(null, null, null, null, null, null, id("ID_IN_MAP"), null, null, null, null, null);

            ConvertDirectoryCollectionGetToCollections.addCollectionFromMap(cols, "KEY_123", source);

            assertEquals(1, cols.getCollections().size());
            // We can’t assert the map-key directly without knowing Collections' internal structure,
            // but we can assert the collection object exists and carries the mapped ID.
            assertEquals("ID_IN_MAP", cols.getCollections().get(0).getId());
        }
    }
}

