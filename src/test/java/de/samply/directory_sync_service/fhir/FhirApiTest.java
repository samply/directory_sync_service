package de.samply.directory_sync_service.fhir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import de.samply.directory_sync_service.model.BbmriEricId;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Specimen;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FhirApiTest {

    private static final String PROFILE_URI =
            "https://example.org/fhir/StructureDefinition/test-organization";

    private static final String EXTENSION_URI =
            "https://example.org/fhir/StructureDefinition/test-extension";

    private FhirApi fhirApi;

    @BeforeEach
    void setUp() {
        /*
         * Constructing a HAPI client does not contact the server. A connection is
         * only attempted when operations such as search(), read(), or update()
         * are executed.
         */
        fhirApi = new FhirApi("http://localhost:8080/fhir");
    }

    @Test
    void extractDirectoryIdFromOrganizationIdentifierReturnsValidIdentifier() {
        Organization organization = new Organization();

        organization.addIdentifier()
                .setSystem("http://www.bbmri-eric.eu/")
                .setValue("bbmri-eric:ID:DE_DKFZ_TEST:collection:Test1");

        BbmriEricId result =
                FhirApi.extractDirectoryIdFromOrganizationIdentifier(organization);

        assertEquals(
                "bbmri-eric:ID:DE_DKFZ_TEST:collection:Test1",
                result.toString());
    }

    @Test
    void extractDirectoryIdFromOrganizationIdentifierIgnoresWrongSystem() {
        Organization organization = new Organization();

        organization.addIdentifier()
                .setSystem("https://example.org/identifier-system")
                .setValue("bbmri-eric:ID:DE_DKFZ_TEST:collection:Test1");

        BbmriEricId result =
                FhirApi.extractDirectoryIdFromOrganizationIdentifier(organization);

        assertNull(result);
    }

    @Test
    void extractDirectoryIdFromOrganizationIdentifierIgnoresEmptyValue() {
        Organization organization = new Organization();

        organization.addIdentifier()
                .setSystem("http://www.bbmri-eric.eu/")
                .setValue("");

        BbmriEricId result =
                FhirApi.extractDirectoryIdFromOrganizationIdentifier(organization);

        assertNull(result);
    }

    @Test
    void extractDirectoryIdFromOrganizationIdentifierSkipsInvalidIdentifier() {
        Organization organization = new Organization();

        organization.addIdentifier()
                .setSystem("http://www.bbmri-eric.eu/")
                .setValue("not-a-valid-directory-id");

        organization.addIdentifier()
                .setSystem("http://www.bbmri-eric.eu/")
                .setValue("bbmri-eric:ID:DE_DKFZ_TEST:collection:Test1");

        BbmriEricId result =
                FhirApi.extractDirectoryIdFromOrganizationIdentifier(organization);

        assertEquals(
                "bbmri-eric:ID:DE_DKFZ_TEST:collection:Test1",
                result.toString());
    }

    @Test
    void extractDirectoryIdFromOrganizationIdentifierReturnsNullWhenNoIdentifiersExist() {
        Organization organization = new Organization();

        BbmriEricId result =
                FhirApi.extractDirectoryIdFromOrganizationIdentifier(organization);

        assertNull(result);
    }

    @Test
    void extractOrganizationsReturnsOnlyOrganizationsWithRequestedProfile() {
        Organization matchingOrganization = new Organization();
        matchingOrganization.setId("matching");
        matchingOrganization.getMeta().addProfile(PROFILE_URI);

        Organization wrongProfileOrganization = new Organization();
        wrongProfileOrganization.setId("wrong-profile");
        wrongProfileOrganization.getMeta().addProfile(
                "https://example.org/fhir/StructureDefinition/other-profile");

        Patient patient = new Patient();
        patient.setId("patient");
        patient.getMeta().addProfile(PROFILE_URI);

        Bundle bundle = new Bundle();
        bundle.addEntry().setResource(matchingOrganization);
        bundle.addEntry().setResource(wrongProfileOrganization);
        bundle.addEntry().setResource(patient);

        List<Organization> result =
                FhirApi.extractOrganizations(bundle, PROFILE_URI);

        assertEquals(1, result.size());
        assertSame(matchingOrganization, result.get(0));
    }

    @Test
    void extractOrganizationsReturnsEmptyListForEmptyBundle() {
        Bundle bundle = new Bundle();

        List<Organization> result =
                FhirApi.extractOrganizations(bundle, PROFILE_URI);

        assertTrue(result.isEmpty());
    }

    @Test
    void extractOrganizationsReturnsEmptyListWhenNoProfileMatches() {
        Organization organization = new Organization();
        organization.getMeta().addProfile(
                "https://example.org/fhir/StructureDefinition/other-profile");

        Bundle bundle = new Bundle();
        bundle.addEntry().setResource(organization);

        List<Organization> result =
                FhirApi.extractOrganizations(bundle, PROFILE_URI);

        assertTrue(result.isEmpty());
    }

    @Test
    void extractCollectionIdFromReferenceHandlesRelativeOrganizationReference() {
        String result =
                fhirApi.extractCollectionIdFromReference("Organization/collection-123");

        assertEquals("collection-123", result);
    }

    @Test
    void extractCollectionIdFromReferenceHandlesAbsoluteUrl() {
        String result = fhirApi.extractCollectionIdFromReference(
                "https://example.org/fhir/Organization/collection-123");

        assertEquals("collection-123", result);
    }

    @Test
    void extractCollectionIdFromReferenceReturnsUnchangedPlainIdentifier() {
        String result =
                fhirApi.extractCollectionIdFromReference("collection-123");

        assertEquals("collection-123", result);
    }

    @Test
    void extractCollectionIdFromReferenceReturnsNullForNullInput() {
        assertNull(fhirApi.extractCollectionIdFromReference(null));
    }

    @Test
    void extractValidDirectoryIdentifierFromCollectionReturnsValidIdentifier() {
        Organization collection = new Organization();

        collection.addIdentifier()
                .setSystem("https://example.org/local-id")
                .setValue("local-collection");

        collection.addIdentifier()
                .setSystem("http://www.bbmri-eric.eu/")
                .setValue("bbmri-eric:ID:DE_DKFZ_TEST:collection:Test1");

        String result =
                fhirApi.extractValidDirectoryIdentifierFromCollection(collection);

        assertEquals(
                "bbmri-eric:ID:DE_DKFZ_TEST:collection:Test1",
                result);
    }

    @Test
    void extractValidDirectoryIdentifierFromCollectionReturnsDefaultForInvalidIdentifiers() {
        Organization collection = new Organization();

        collection.addIdentifier(
                new Identifier().setValue("invalid-collection-id"));

        String result =
                fhirApi.extractValidDirectoryIdentifierFromCollection(collection);

        assertEquals(FhirApi.DEFAULT_COLLECTION_ID, result);
    }

    @Test
    void extractMaterialFromSpecimenReturnsFirstCodingCode() {
        Specimen specimen = new Specimen();
        specimen.setId("specimen-1");

        specimen.setType(
                new CodeableConcept().addCoding(
                        new Coding()
                                .setSystem("https://example.org/material")
                                .setCode("PLASMA")));

        String result = fhirApi.extractMaterialFromSpecimen(specimen);

        assertEquals("PLASMA", result);
    }

    @Test
    void extractMaterialFromSpecimenFallsBackToText() {
        Specimen specimen = new Specimen();
        specimen.setId("specimen-1");
        specimen.setType(new CodeableConcept().setText("Whole blood"));

        String result = fhirApi.extractMaterialFromSpecimen(specimen);

        assertEquals("Whole blood", result);
    }

    @Test
    void extractMaterialFromSpecimenReturnsNullWhenTypeContainsNoMaterial() {
        Specimen specimen = new Specimen();
        specimen.setId("specimen-1");
        specimen.setType(new CodeableConcept());

        String result = fhirApi.extractMaterialFromSpecimen(specimen);

        assertNull(result);
    }

    @Test
    void extractMaterialsFromSpecimenListReturnsUniqueMaterials() {
        Specimen firstPlasma = specimenWithMaterial("specimen-1", "PLASMA");
        Specimen secondPlasma = specimenWithMaterial("specimen-2", "PLASMA");
        Specimen serum = specimenWithMaterial("specimen-3", "SERUM");

        List<String> result = fhirApi.extractMaterialsFromSpecimenList(
                List.of(firstPlasma, secondPlasma, serum));

        assertEquals(2, result.size());
        assertTrue(result.contains("PLASMA"));
        assertTrue(result.contains("SERUM"));
    }

    @Test
    void extractMaterialsFromSpecimenListReturnsNullForNullList() {
        assertNull(fhirApi.extractMaterialsFromSpecimenList(null));
    }

    @Test
    void extractExtensionElementValuesFromSpecimenReturnsCode() {
        Specimen specimen = new Specimen();

        CodeableConcept value = new CodeableConcept();
        value.addCoding(
                new Coding()
                        .setSystem("https://example.org/diagnosis")
                        .setCode("C50"));

        specimen.addExtension(
                new Extension()
                        .setUrl(EXTENSION_URI)
                        .setValue(value));

        List<String> result =
                fhirApi.extractExtensionElementValuesFromSpecimen(
                        specimen, EXTENSION_URI);

        assertEquals(List.of("C50"), result);
    }

    @Test
    void extractExtensionElementValuesFromSpecimenIgnoresOtherUrls() {
        Specimen specimen = new Specimen();

        CodeableConcept value = new CodeableConcept();
        value.addCoding(new Coding().setCode("C50"));

        specimen.addExtension(
                new Extension()
                        .setUrl("https://example.org/fhir/other-extension")
                        .setValue(value));

        List<String> result =
                fhirApi.extractExtensionElementValuesFromSpecimen(
                        specimen, EXTENSION_URI);

        assertTrue(result.isEmpty());
    }

    @Test
    void extractExtensionElementValuesFromSpecimenReturnsEmptyListForNullSpecimen() {
        List<String> result =
                fhirApi.extractExtensionElementValuesFromSpecimen(
                        null, EXTENSION_URI);

        assertTrue(result.isEmpty());
    }

    @Test
    void extractExtensionElementValuesFromSpecimensRemovesDuplicates() {
        Specimen first = specimenWithExtension("C50");
        Specimen second = specimenWithExtension("C50");
        Specimen third = specimenWithExtension("C34");

        List<String> result =
                fhirApi.extractExtensionElementValuesFromSpecimens(
                        List.of(first, second, third), EXTENSION_URI);

        assertEquals(2, result.size());
        assertTrue(result.contains("C50"));
        assertTrue(result.contains("C34"));
    }

    @Test
    void extractSexFromPatientListReturnsUniqueValues() {
        Patient maleOne = new Patient();
        maleOne.setGender(org.hl7.fhir.r4.model.Enumerations.AdministrativeGender.MALE);

        Patient maleTwo = new Patient();
        maleTwo.setGender(org.hl7.fhir.r4.model.Enumerations.AdministrativeGender.MALE);

        Patient female = new Patient();
        female.setGender(org.hl7.fhir.r4.model.Enumerations.AdministrativeGender.FEMALE);

        List<String> result =
                fhirApi.extractSexFromPatientList(
                        List.of(maleOne, maleTwo, female));

        assertEquals(2, result.size());
        assertTrue(result.contains("male"));
        assertTrue(result.contains("female"));
    }

    @Test
    void determinePatientAgeReturnsNullWithoutBirthDate() {
        Patient patient = new Patient();

        assertNull(fhirApi.determinePatientAge(patient));
    }

    @Test
    void determinePatientAgeReturnsExpectedAge() {
        Patient patient = new Patient();

        LocalDate birthDate = LocalDate.now().minusYears(40);
        Date date = Date.from(
                birthDate.atStartOfDay(ZoneId.systemDefault()).toInstant());

        patient.setBirthDate(date);

        assertEquals(40, fhirApi.determinePatientAge(patient));
    }

    @Test
    void extractAgeLowFromPatientListReturnsYoungestAge() {
        Patient ageTwenty = patientWithAge(20);
        Patient ageForty = patientWithAge(40);
        Patient ageThirty = patientWithAge(30);

        Integer result = fhirApi.extractAgeLowFromPatientList(
                List.of(ageTwenty, ageForty, ageThirty));

        assertEquals(20, result);
    }

    @Test
    void extractAgeHighFromPatientListReturnsOldestAge() {
        Patient ageTwenty = patientWithAge(20);
        Patient ageForty = patientWithAge(40);
        Patient ageThirty = patientWithAge(30);

        Integer result = fhirApi.extractAgeHighFromPatientList(
                List.of(ageTwenty, ageForty, ageThirty));

        assertEquals(40, result);
    }

    @Test
    void extractAgeMethodsReturnMinusOneWhenNoBirthDatesExist() {
        List<Patient> patients = List.of(new Patient(), new Patient());

        assertEquals(-1, fhirApi.extractAgeLowFromPatientList(patients));
        assertEquals(-1, fhirApi.extractAgeHighFromPatientList(patients));
    }

    private Specimen specimenWithMaterial(String id, String material) {
        Specimen specimen = new Specimen();
        specimen.setId(id);
        specimen.setType(
                new CodeableConcept().addCoding(
                        new Coding().setCode(material)));
        return specimen;
    }

    private Specimen specimenWithExtension(String code) {
        Specimen specimen = new Specimen();

        CodeableConcept value = new CodeableConcept();
        value.addCoding(new Coding().setCode(code));

        specimen.addExtension(
                new Extension()
                        .setUrl(EXTENSION_URI)
                        .setValue(value));

        return specimen;
    }

    private Patient patientWithAge(int age) {
        Patient patient = new Patient();

        LocalDate birthDate = LocalDate.now().minusYears(age);
        patient.setBirthDate(
                Date.from(
                        birthDate.atStartOfDay(ZoneId.systemDefault()).toInstant()));

        return patient;
    }
}
