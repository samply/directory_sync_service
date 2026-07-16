package de.samply.directory_sync_service.fhir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Specimen;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FhirApiBbmriDeTest {

    private static final String CUSTODIAN_URI =
            "https://fhir.bbmri.de/StructureDefinition/Custodian";

    private FhirApiBbmriDe fhirApi;

    @BeforeEach
    void setUp() {
        /*
         * Creating the HAPI FHIR client does not contact the server.
         * A connection is only made when a FHIR operation is executed.
         */
        fhirApi = new FhirApiBbmriDe("http://localhost:8080/fhir");
    }

    @Test
    void getBiobankProfileUriReturnsBbmriDeBiobankProfile() {
        assertEquals(
                "https://fhir.bbmri.de/StructureDefinition/Biobank",
                fhirApi.getBiobankProfileUri());
    }

    @Test
    void getCollectionProfileUriReturnsBbmriDeCollectionProfile() {
        assertEquals(
                "https://fhir.bbmri.de/StructureDefinition/Collection",
                fhirApi.getCollectionProfileUri());
    }

    @Test
    void getSampleDiagnosisUriReturnsBbmriDeSampleDiagnosisExtension() {
        assertEquals(
                "https://fhir.bbmri.de/StructureDefinition/SampleDiagnosis",
                fhirApi.getSampleDiagnosisUri());
    }

    @Test
    void extractLocalCollectionIdFromSpecimenReturnsIdFromRelativeReference() {
        Specimen specimen = specimenWithCustodianReference(
                "Organization/local-collection-123");

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertEquals("local-collection-123", result);
    }

    @Test
    void extractLocalCollectionIdFromSpecimenReturnsIdFromAbsoluteReference() {
        Specimen specimen = specimenWithCustodianReference(
                "https://example.org/fhir/Organization/local-collection-123");

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertEquals("local-collection-123", result);
    }

    @Test
    void extractLocalCollectionIdFromSpecimenReturnsPlainReferenceUnchanged() {
        Specimen specimen =
                specimenWithCustodianReference("local-collection-123");

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertEquals("local-collection-123", result);
    }

    @Test
    void extractLocalCollectionIdFromSpecimenReturnsNullWithoutExtensions() {
        Specimen specimen = new Specimen();

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertNull(result);
    }

    @Test
    void extractLocalCollectionIdFromSpecimenReturnsNullWithoutCustodianExtension() {
        Specimen specimen = new Specimen();

        specimen.addExtension(
                new Extension()
                        .setUrl("https://example.org/another-extension")
                        .setValue(new Reference("Organization/collection-123")));

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertNull(result);
    }

    @Test
    void extractLocalCollectionIdFromSpecimenReturnsNullForNullReference() {
        Specimen specimen = new Specimen();

        specimen.addExtension(
                new Extension()
                        .setUrl(CUSTODIAN_URI)
                        .setValue(new Reference()));

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertNull(result);
    }

    @Test
    void extractStorageTemperaturesFromSpecimenListReturnsTemperatureCodes() {
        Specimen minusEighty = specimenWithStorageTemperature("-80");
        Specimen minusTwenty = specimenWithStorageTemperature("-20");

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(minusEighty, minusTwenty));

        assertEquals(2, result.size());
        assertTrue(result.contains("-80"));
        assertTrue(result.contains("-20"));
    }

    @Test
    void extractStorageTemperaturesFromSpecimenListRemovesDuplicates() {
        Specimen first = specimenWithStorageTemperature("-80");
        Specimen second = specimenWithStorageTemperature("-80");

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(first, second));

        assertEquals(List.of("-80"), result);
    }

    @Test
    void extractStorageTemperaturesFromSpecimenListIgnoresOtherExtensions() {
        Specimen specimen = new Specimen();

        specimen.addExtension(
                new Extension()
                        .setUrl("https://example.org/other-extension")
                        .setValue(codeableConcept("ROOM_TEMPERATURE")));

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(specimen));

        assertTrue(result.isEmpty());
    }

    @Test
    void extractStorageTemperaturesFromSpecimenListReturnsEmptyListForEmptyInput() {
        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(List.of());

        assertTrue(result.isEmpty());
    }

    private Specimen specimenWithCustodianReference(String reference) {
        Specimen specimen = new Specimen();

        specimen.addExtension(
                new Extension()
                        .setUrl(CUSTODIAN_URI)
                        .setValue(new Reference(reference)));

        return specimen;
    }

    private Specimen specimenWithStorageTemperature(String code) {
        Specimen specimen = new Specimen();

        specimen.addExtension(
                new Extension()
                        .setUrl(FhirApiBbmriDe.STORAGE_TEMPERATURE_URI)
                        .setValue(codeableConcept(code)));

        return specimen;
    }

    private CodeableConcept codeableConcept(String code) {
        return new CodeableConcept()
                .addCoding(new Coding().setCode(code));
    }
}