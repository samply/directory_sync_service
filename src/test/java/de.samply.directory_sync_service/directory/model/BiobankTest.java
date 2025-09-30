package de.samply.directory_sync_service.directory.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

class BiobankTest {

    @Test
    void defaultConstructor_allFieldsNull() {
        Biobank b = new Biobank();

        assertNull(b.getAcronym());
        assertNull(b.getCapabilities());
        assertNull(b.getContact());
        assertNull(b.getCountry());
        assertNull(b.getDescription());
        assertNull(b.getHead());
        assertNull(b.getId());
        assertNull(b.getJuridicalPerson());
        assertNull(b.getLatitude());
        assertNull(b.getLocation());
        assertNull(b.getLongitude());
        assertNull(b.getName());
        assertNull(b.getNetwork());
        assertNull(b.getUrl());
    }

    @Test
    void settersAndGetters_roundTrip_primitivesAndStrings() {
        Biobank b = new Biobank();

        b.setAcronym("GBN");
        b.setDescription("German Biobank Node");
        b.setId("bbmri-eric:ID:DE_123");
        b.setJuridicalPerson("Charité");
        b.setLatitude("52.5200");
        b.setLongitude("13.4050");
        b.setLocation("Berlin");
        b.setName("GBN Biobank");
        b.setUrl("https://example.org/gbn");

        assertEquals("GBN", b.getAcronym());
        assertEquals("German Biobank Node", b.getDescription());
        assertEquals("bbmri-eric:ID:DE_123", b.getId());
        assertEquals("Charité", b.getJuridicalPerson());
        assertEquals("52.5200", b.getLatitude());
        assertEquals("13.4050", b.getLongitude());
        assertEquals("Berlin", b.getLocation());
        assertEquals("GBN Biobank", b.getName());
        assertEquals("https://example.org/gbn", b.getUrl());
    }

    @Test
    void settersAndGetters_roundTrip_mapsAndLists() {
        Biobank b = new Biobank();

        Map<String, Object> contact = new HashMap<>();
        contact.put("name", "Dr. Head of Biobank");
        contact.put("email", "head@example.org");

        Map<String, Object> country = new HashMap<>();
        country.put("code", "DE");
        country.put("name", "Germany");

        Map<String, Object> head = new HashMap<>();
        head.put("name", "Prof. X");

        List<Map> capabilities = new ArrayList<>();
        capabilities.add(new HashMap<>(Map.of("name", "Blood processing")));
        capabilities.add(new HashMap<>(Map.of("name", "Sample storage")));

        List<Map> network = new ArrayList<>();
        network.add(new HashMap<>(Map.of("name", "BBMRI-ERIC")));
        network.add(new HashMap<>(Map.of("name", "National Network")));

        b.setContact(contact);
        b.setCountry(country);
        b.setHead(head);
        b.setCapabilities(capabilities);
        b.setNetwork(network);

        // reference & content checks
        assertSame(contact, b.getContact());
        assertSame(country, b.getCountry());
        assertSame(head, b.getHead());
        assertSame(capabilities, b.getCapabilities());
        assertSame(network, b.getNetwork());
        assertEquals("Dr. Head of Biobank", b.getContact().get("name"));
        assertEquals("DE", b.getCountry().get("code"));
        assertEquals("BBMRI-ERIC", ((Map<?, ?>) b.getNetwork().get(0)).get("name"));
    }

    @Test
    void toString_emptyBiobank_serializesToEmptyJsonObject() {
        Biobank b = new Biobank();
        String json = b.toString();

        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
        // Gson (without serializeNulls) omits null fields → {}
        assertTrue(obj.entrySet().isEmpty(), "Expected no properties for an all-null object");
    }

    @Test
    void toString_populatedBiobank_containsAllNonNullFields() {
        Biobank b = new Biobank();
        b.setId("bbmri-eric:ID:DE_123");
        b.setName("GBN Biobank");
        b.setAcronym("GBN");
        b.setLatitude("52.5200");
        b.setLongitude("13.4050");
        b.setLocation("Berlin");
        b.setUrl("https://example.org/gbn");

        Map<String, Object> country = new HashMap<>();
        country.put("code", "DE");
        country.put("name", "Germany");
        b.setCountry(country);

        List<Map> network = new ArrayList<>();
        network.add(new HashMap<>(Map.of("name", "BBMRI-ERIC")));
        b.setNetwork(network);

        String json = b.toString();
        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();

        // basic scalar fields
        assertEquals("bbmri-eric:ID:DE_123", obj.get("id").getAsString());
        assertEquals("GBN Biobank", obj.get("name").getAsString());
        assertEquals("GBN", obj.get("acronym").getAsString());
        assertEquals("52.5200", obj.get("latitude").getAsString());
        assertEquals("13.4050", obj.get("longitude").getAsString());
        assertEquals("Berlin", obj.get("location").getAsString());
        assertEquals("https://example.org/gbn", obj.get("url").getAsString());

        // nested objects / arrays
        assertEquals("DE", obj.getAsJsonObject("country").get("code").getAsString());
        assertEquals("Germany", obj.getAsJsonObject("country").get("name").getAsString());

        assertTrue(obj.has("network"));
        assertEquals(1, obj.getAsJsonArray("network").size());
        assertEquals("BBMRI-ERIC",
                obj.getAsJsonArray("network").get(0).getAsJsonObject().get("name").getAsString());
    }
}
