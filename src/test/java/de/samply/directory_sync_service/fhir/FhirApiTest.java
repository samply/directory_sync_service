package de.samply.directory_sync_service.fhir;

import static org.junit.jupiter.api.Assertions.*;

import org.hl7.fhir.r4.model.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

class FhirApiTest {

    // ----------------- helpers -----------------

    /** Set a private field on the FhirApi under test (used to preload caches). */
    @SuppressWarnings("SameParameterValue")
    private static void setPrivateField(Object target, String fieldName, Object value) {
        try {
            Field f = FhirApi.class.getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set field " + fieldName, e);
        }
    }

    /** Make a Specimen with a simple type Coding code (no system required for our logic). */
    private static Specimen specimenWithTypeCode(String id, String code) {
        Specimen s = new Specimen();
        s.setId(id);
        CodeableConcept cc = new CodeableConcept();
        cc.addCoding(new Coding().setCode(code));
        s.setType(cc);
        return s;
    }

    /** Make a Specimen with type text (no codings). */
    private static Specimen specimenWithTypeText(String id, String text) {
        Specimen s = new Specimen();
        s.setId(id);
        s.setType(new CodeableConcept().setText(text));
        return s;
    }

    /** Add a CodeableConcept-valued extension to a Specimen. */
    private static Specimen addCodeableConceptExtension(Specimen s, String url, String code) {
        CodeableConcept cc = new CodeableConcept();
        cc.addCoding(new Coding().setCode(code));
        s.addExtension(new Extension(url, cc));
        return s;
    }

    // ----------------- tests -----------------

    @Nested
    class BbmriEricIdTests {
        @Test
        @DisplayName("bbmriEricId: extracts and parses a valid identifier from Organization")
        void bbmriEricId_valid() {
            Organization org = new Organization();
            org.addIdentifier()
                    .setSystem("http://www.bbmri-eric.eu/")
                    .setValue("bbmri-eric:ID:DE_ABC");

            var opt = FhirApi.bbmriEricId(org);
            assertTrue(opt.isPresent());
            assertEquals("DE", opt.get().getCountryCode());
            assertEquals("bbmri-eric:ID:DE_ABC", opt.get().toString());
        }

        @Test
        @DisplayName("bbmriEricId: returns empty when identifier missing or wrong system")
        void bbmriEricId_missingOrWrongSystem() {
            Organization org = new Organization();
            org.addIdentifier().setSystem("not-bbmri").setValue("bbmri-eric:ID:DE_ABC");
            assertTrue(FhirApi.bbmriEricId(org).isEmpty());

            Organization org2 = new Organization(); // no identifiers
            assertTrue(FhirApi.bbmriEricId(org2).isEmpty());
        }
    }

    @Test
    @DisplayName("resourceToJsonString: pretty JSON contains resourceType and id")
    void resourceToJsonString_basic() {
        FhirApi api = new FhirApi("http://example.org/fhir");
        Patient p = new Patient();
        p.setId("pat-1");
        String json = api.resourceToJsonString(p);
        assertTrue(json.contains("\"resourceType\""));
        assertTrue(json.contains("\"Patient\""));
        assertTrue(json.contains("\"id\""));
        assertTrue(json.contains("pat-1"));
    }

    @Nested
    class SpecimenCacheDrivenLogic {

        @Test
        @DisplayName("calculateTotalSpecimenCount: sums sizes across cached map")
        void calculateTotalSpecimenCount_sums() {
            FhirApi api = new FhirApi("http://example.org/fhir");

            Map<String, List<Specimen>> fake = new HashMap<>();
            fake.put("bbmri-eric:ID:DE_A", List.of(new Specimen(), new Specimen())); // 2
            fake.put("bbmri-eric:ID:DE_B", List.of(new Specimen()));                 // 1
            setPrivateField(api, "specimensByCollection", fake);

            int total = api.calculateTotalSpecimenCount(null);
            assertEquals(3, total);
        }

        @Test
        @DisplayName("getSampleMaterials: collects unique materials from Specimen.type (coding and text)")
        void getSampleMaterials_collectsUnique() {
            FhirApi api = new FhirApi("http://example.org/fhir");

            // Build specimens: one with Coding code, one with text, one with duplicate code
            Specimen s1 = specimenWithTypeCode("s1", "SERUM");
            Specimen s2 = specimenWithTypeText("s2", "PLASMA");
            Specimen s3 = specimenWithTypeCode("s3", "SERUM"); // duplicate should be deduped

            Map<String, List<Specimen>> fake = new HashMap<>();
            fake.put("bbmri-eric:ID:DE_A", List.of(s1, s2, s3));
            setPrivateField(api, "specimensByCollection", fake);

            Map<String, String> materials = api.getSampleMaterials(null);

            // Keys and values are the same in current implementation
            assertEquals(Set.of("SERUM", "PLASMA"),
                    new HashSet<>(materials.keySet()));
            assertEquals(materials.keySet(), new HashSet<>(materials.values()));
        }

        @Test
        @DisplayName("extractDiagnosesFromSpecimen: reads codes from SAMPLE_DIAGNOSIS_URI extensions")
        void extractDiagnosesFromSpecimen_readsExtensions() {
            FhirApi api = new FhirApi("http://example.org/fhir");
            Specimen s = new Specimen();
            s.setId("spec-1");

            // Add two diagnosis extensions with different codes
            addCodeableConceptExtension(s,
                    "https://fhir.bbmri.de/StructureDefinition/SampleDiagnosis", "A00");
            addCodeableConceptExtension(s,
                    "https://fhir.bbmri.de/StructureDefinition/SampleDiagnosis", "B01");

            List<String> codes = api.extractDiagnosesFromSpecimen(s);
            assertEquals(Set.of("A00", "B01"), new HashSet<>(codes));
        }
    }

    @Test
    @DisplayName("distinctBy: returns predicate that filters duplicates by key")
    void distinctBy_filtersDuplicates() {
        // given a list with duplicate keys
        record Item(String key, int value) {}
        List<Item> items = List.of(
                new Item("K", 1),
                new Item("K", 2),
                new Item("L", 3),
                new Item("L", 4)
        );

        // when filtering by 'key'
        var distinct = items.stream()
                .filter(FhirApi.distinctBy((Function<Item, ?>) Item::key))
                .collect(Collectors.toList());

        // then only first occurrence per key remains
        assertEquals(2, distinct.size());
        assertEquals("K", distinct.get(0).key());
        assertEquals("L", distinct.get(1).key());
    }
}
