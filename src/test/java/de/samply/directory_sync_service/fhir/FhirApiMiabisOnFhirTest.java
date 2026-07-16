package de.samply.directory_sync_service.fhir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Specimen;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FhirApiMiabisOnFhirTest {

    private static final String COLLECTION_EXTENSION_URL =
            "https://fhir.bbmri-eric.eu/StructureDefinition/"
                    + "miabis-sample-collection-extension";

    private static final String STORAGE_TEMP_EXTENSION =
            "https://fhir.bbmri-eric.eu/StructureDefinition/"
                    + "miabis-sample-storage-temperature-extension";

    private static final String STORAGE_TEMP_SYSTEM =
            "https://fhir.bbmri-eric.eu/CodeSystem/"
                    + "miabis-storage-temperature-cs";

    private FhirApiMiabisOnFhir fhirApi;

    @BeforeEach
    void setUp() {
        /*
         * Constructing the HAPI client does not contact the FHIR server.
         * A connection is made only when an operation is executed.
         */
        fhirApi = new FhirApiMiabisOnFhir("http://localhost:8080/fhir");
    }

    @Test
    void getBiobankProfileUriReturnsMiabisBiobankProfile() {
        assertEquals(
                "https://fhir.bbmri-eric.eu/StructureDefinition/miabis-biobank",
                fhirApi.getBiobankProfileUri());
    }

    @Test
    void getCollectionProfileUriReturnsMiabisCollectionProfile() {
        assertEquals(
                "https://fhir.bbmri-eric.eu/StructureDefinition/"
                        + "miabis-collection-organization",
                fhirApi.getCollectionProfileUri());
    }

    @Test
    void getSampleDiagnosisUriReturnsMiabisSampleDiagnosisProfile() {
        assertEquals(
                "https://fhir.bbmri-eric.eu/StructureDefinition/"
                        + "miabis-sample-diagnosis-extension",
                fhirApi.getSampleDiagnosisUri());
    }

    @Test
    void extractLocalCollectionIdReturnsIdentifierValue() {
        Specimen specimen = specimenWithCollectionIdentifier(
                "local-collection-123");

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertEquals("local-collection-123", result);
    }

    @Test
    void extractLocalCollectionIdRemovesOrganizationPrefix() {
        Specimen specimen = specimenWithCollectionIdentifier(
                "Organization/local-collection-123");

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertEquals("local-collection-123", result);
    }

    @Test
    void extractLocalCollectionIdExtractsIdFromAbsoluteUrl() {
        Specimen specimen = specimenWithCollectionIdentifier(
                "https://example.org/fhir/Organization/local-collection-123");

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertEquals("local-collection-123", result);
    }

    @Test
    void extractLocalCollectionIdReturnsNullWhenSpecimenHasNoExtensions() {
        Specimen specimen = new Specimen();

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertNull(result);
    }

    @Test
    void extractLocalCollectionIdReturnsNullWhenCollectionExtensionIsMissing() {
        Specimen specimen = new Specimen();

        specimen.addExtension(
                new Extension()
                        .setUrl("https://example.org/other-extension")
                        .setValue(new Identifier().setValue("collection-123")));

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertNull(result);
    }

    @Test
    void extractLocalCollectionIdReturnsNullWhenExtensionValueIsNotIdentifier() {
        Specimen specimen = new Specimen();

        specimen.addExtension(
                new Extension()
                        .setUrl(COLLECTION_EXTENSION_URL)
                        .setValue(new Reference("Organization/collection-123")));

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertNull(result);
    }

    @Test
    void extractLocalCollectionIdReturnsNullWhenIdentifierHasNoValue() {
        Specimen specimen = new Specimen();

        specimen.addExtension(
                new Extension()
                        .setUrl(COLLECTION_EXTENSION_URL)
                        .setValue(new Identifier()));

        String result =
                fhirApi.extractLocalCollectionIdFromSpecimen(specimen);

        assertNull(result);
    }

    @Test
    void extractMaterialMapsBloodToWholeBlood() {
        Specimen specimen = specimenWithMaterial("Blood");

        String result = fhirApi.extractMaterialFromSpecimen(specimen);

        assertEquals("whole-blood", result);
    }

    @Test
    void extractMaterialMapsPlasmaToBloodPlasma() {
        Specimen specimen = specimenWithMaterial("Plasma");

        String result = fhirApi.extractMaterialFromSpecimen(specimen);

        assertEquals("blood-plasma", result);
    }

    @Test
    void extractMaterialMapsSerumToBloodSerum() {
        Specimen specimen = specimenWithMaterial("Serum");

        String result = fhirApi.extractMaterialFromSpecimen(specimen);

        assertEquals("blood-serum", result);
    }

    @Test
    void extractMaterialMapsTissueFreshFrozenToTissueFrozen() {
        Specimen specimen = specimenWithMaterial("TissueFreshFrozen");

        String result = fhirApi.extractMaterialFromSpecimen(specimen);

        assertEquals("tissue-frozen", result);
    }

    @Test
    void extractMaterialMapsOtherToEmptyString() {
        Specimen specimen = specimenWithMaterial("Other");

        String result = fhirApi.extractMaterialFromSpecimen(specimen);

        assertEquals("", result);
    }

    @Test
    void extractMaterialUsesLowercaseFallbackForUnknownMaterial() {
        Specimen specimen = specimenWithMaterial("UnknownMaterial");

        String result = fhirApi.extractMaterialFromSpecimen(specimen);

        assertEquals("unknownmaterial", result);
    }

    @Test
    void extractMaterialReturnsNullWhenSpecimenContainsNoMaterial() {
        Specimen specimen = new Specimen();

        String result = fhirApi.extractMaterialFromSpecimen(specimen);

        assertNull(result);
    }

    @Test
    void extractStorageTemperaturesMapsKnownCodes() {
        Specimen roomTemperature = specimenWithStorageTemperature(
                STORAGE_TEMP_EXTENSION,
                STORAGE_TEMP_SYSTEM,
                "RT");

        Specimen frozen = specimenWithStorageTemperature(
                STORAGE_TEMP_EXTENSION,
                STORAGE_TEMP_SYSTEM,
                "-60to-85");

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(roomTemperature, frozen));

        assertEquals(2, result.size());
        assertTrue(result.contains("temperatureRoom"));
        assertTrue(result.contains("temperature-60to-85"));
    }

    @Test
    void extractStorageTemperaturesMapsAllKnownCodes() {
        List<Specimen> specimens = List.of(
                specimenWithStorageTemperature(
                        STORAGE_TEMP_EXTENSION, STORAGE_TEMP_SYSTEM, "RT"),
                specimenWithStorageTemperature(
                        STORAGE_TEMP_EXTENSION, STORAGE_TEMP_SYSTEM, "2to10"),
                specimenWithStorageTemperature(
                        STORAGE_TEMP_EXTENSION, STORAGE_TEMP_SYSTEM, "-18to-35"),
                specimenWithStorageTemperature(
                        STORAGE_TEMP_EXTENSION, STORAGE_TEMP_SYSTEM, "-60to-85"),
                specimenWithStorageTemperature(
                        STORAGE_TEMP_EXTENSION, STORAGE_TEMP_SYSTEM, "LN"),
                specimenWithStorageTemperature(
                        STORAGE_TEMP_EXTENSION, STORAGE_TEMP_SYSTEM, "Other"));

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(specimens);

        assertEquals(6, result.size());
        assertTrue(result.contains("temperatureRoom"));
        assertTrue(result.contains("temperature2to10"));
        assertTrue(result.contains("temperature-18to-35"));
        assertTrue(result.contains("temperature-60to-85"));
        assertTrue(result.contains("temperatureLN"));
        assertTrue(result.contains("temperatureOther"));
    }

    @Test
    void extractStorageTemperaturesRemovesDuplicateMiabisCodes() {
        Specimen first = specimenWithStorageTemperature(
                STORAGE_TEMP_EXTENSION,
                STORAGE_TEMP_SYSTEM,
                "RT");

        Specimen second = specimenWithStorageTemperature(
                STORAGE_TEMP_EXTENSION,
                STORAGE_TEMP_SYSTEM,
                "RT");

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(first, second));

        assertEquals(List.of("temperatureRoom"), result);
    }

    @Test
    void extractStorageTemperaturesUsesFallbackForUnknownCode() {
        Specimen specimen = specimenWithStorageTemperature(
                STORAGE_TEMP_EXTENSION,
                STORAGE_TEMP_SYSTEM,
                "UltraCold");

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(specimen));

        assertEquals(List.of("temperatureUltraCold"), result);
    }

    @Test
    void extractStorageTemperaturesIgnoresWrongExtensionUrl() {
        Specimen specimen = specimenWithStorageTemperature(
                "https://example.org/other-extension",
                STORAGE_TEMP_SYSTEM,
                "RT");

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(specimen));

        assertTrue(result.isEmpty());
    }

    @Test
    void extractStorageTemperaturesIgnoresWrongCodingSystem() {
        Specimen specimen = specimenWithStorageTemperature(
                STORAGE_TEMP_EXTENSION,
                "https://example.org/other-code-system",
                "RT");

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(specimen));

        assertTrue(result.isEmpty());
    }

    @Test
    void extractStorageTemperaturesIgnoresNullCode() {
        Specimen specimen = specimenWithStorageTemperature(
                STORAGE_TEMP_EXTENSION,
                STORAGE_TEMP_SYSTEM,
                null);

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(specimen));

        assertTrue(result.isEmpty());
    }

    @Test
    void extractStorageTemperaturesIgnoresEmptyCode() {
        Specimen specimen = specimenWithStorageTemperature(
                STORAGE_TEMP_EXTENSION,
                STORAGE_TEMP_SYSTEM,
                "");

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(specimen));

        assertTrue(result.isEmpty());
    }

    @Test
    void extractStorageTemperaturesIgnoresNonCodeableConceptValue() {
        Specimen specimen = new Specimen();

        specimen.addProcessing()
                .addExtension(
                        new Extension()
                                .setUrl(STORAGE_TEMP_EXTENSION)
                                .setValue(new Identifier().setValue("RT")));

        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(
                        List.of(specimen));

        assertTrue(result.isEmpty());
    }

    @Test
    void extractStorageTemperaturesReturnsEmptyListForEmptyInput() {
        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(List.of());

        assertTrue(result.isEmpty());
    }

    @Test
    void extractStorageTemperaturesReturnsNullForNullInput() {
        List<String> result =
                fhirApi.extractStorageTemperaturesFromSpecimenList(null);

        /*
         * The production method catches the NullPointerException and returns
         * the initial null value.
         */
        assertNull(result);
    }

    private Specimen specimenWithCollectionIdentifier(String value) {
        Specimen specimen = new Specimen();

        specimen.addExtension(
                new Extension()
                        .setUrl(COLLECTION_EXTENSION_URL)
                        .setValue(new Identifier().setValue(value)));

        return specimen;
    }

    private Specimen specimenWithMaterial(String materialCode) {
        Specimen specimen = new Specimen();

        specimen.setType(
                new CodeableConcept()
                        .addCoding(
                                new Coding()
                                        .setCode(materialCode)));

        return specimen;
    }

    private Specimen specimenWithStorageTemperature(
            String extensionUrl,
            String codingSystem,
            String code) {

        Specimen specimen = new Specimen();

        CodeableConcept value = new CodeableConcept();
        value.addCoding(
                new Coding()
                        .setSystem(codingSystem)
                        .setCode(code));

        specimen.addProcessing()
                .addExtension(
                        new Extension()
                                .setUrl(extensionUrl)
                                .setValue(value));

        return specimen;
    }
}