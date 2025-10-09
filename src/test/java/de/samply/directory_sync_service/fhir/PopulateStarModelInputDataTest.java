package de.samply.directory_sync_service.fhir;

import de.samply.directory_sync_service.model.StarModelInput;
import org.hl7.fhir.r4.model.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for PopulateStarModelInputData.
 *
 * We treat private helpers (age, material, earliest date, etc.) as black-box
 * behaviors exercised indirectly via populate(...).
 */
@ExtendWith(MockitoExtension.class)
class PopulateStarModelInputDataTest {

    private static final String COLL = "bbmri-eric:ID:DE_TESTCOLL";

    /** Utility: java.util.Date from LocalDate at midnight system zone. */
    private static Date d(LocalDate ld) {
        return Date.from(ld.atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    /** Minimal Patient with id and gender and birthdate. */
    private static Patient patient(String id, Enumerations.AdministrativeGender gender, LocalDate birth) {
        Patient p = new Patient();
        p.setId(id);
        p.setGender(gender);
        p.setBirthDate(birth == null ? null : d(birth));
        return p;
    }

    /** Specimen with id, optional collected date, and optional type (text and/or coding code). */
    private static Specimen specimen(String id, LocalDate collected, String typeText, String codingCode) {
        Specimen s = new Specimen();
        s.setId(id);
        if (collected != null) {
            Specimen.SpecimenCollectionComponent coll = new Specimen.SpecimenCollectionComponent();
            coll.setCollected(new DateTimeType(d(collected)));
            s.setCollection(coll);
        }
        if (typeText != null || codingCode != null) {
            CodeableConcept cc = new CodeableConcept();
            if (typeText != null) cc.setText(typeText);
            if (codingCode != null) cc.addCoding(new Coding().setCode(codingCode));
            s.setType(cc);
        }
        return s;
    }

    /** Read rows for a collection as list of maps. */
    private static List<Map<String,String>> rowsFor(StarModelInput input, String collectionId) {
        return input.getInputRowsAsStringMaps(collectionId);
    }

    @Nested
    class PopulateBasics {

        @Test
        @DisplayName("Returns null if fetchSpecimensByCollection returns null (error path)")
        void returnsNullWhenSpecimenMapIsNull() {
            FhirApi api = mock(FhirApi.class);
            when(api.fetchSpecimensByCollection(any())).thenReturn(null);

            PopulateStarModelInputData svc = new PopulateStarModelInputData(api);
            assertNull(svc.populate(COLL));
        }

        @Test
        @DisplayName("Empty map → returns StarModelInput with no data")
        void emptyMapProducesEmptyInput() {
            FhirApi api = mock(FhirApi.class);
            when(api.fetchSpecimensByCollection(any())).thenReturn(Collections.emptyMap());

            PopulateStarModelInputData svc = new PopulateStarModelInputData(api);
            StarModelInput out = svc.populate(COLL);

            assertNotNull(out);
            assertTrue(out.getInputCollectionIds().isEmpty());
        }

        @Test
        @DisplayName("Skips specimens whose patient cannot be resolved")
        void skipsSpecimenWithNullPatient() {
            FhirApi api = mock(FhirApi.class);
            Specimen s1 = specimen("s1", LocalDate.now(), "FFPE", null);
            when(api.fetchSpecimensByCollection(any()))
                    .thenReturn(Map.of(COLL, List.of(s1)));
            when(api.extractPatientFromSpecimen(s1)).thenReturn(null);

            PopulateStarModelInputData svc = new PopulateStarModelInputData(api);
            StarModelInput out = svc.populate(COLL);

            assertTrue(out.getInputCollectionIds().isEmpty());
        }
    }

    @Nested
    class HappyPathAndMerging {

        @Test
        @DisplayName("Builds one row per distinct diagnosis (patient conditions + specimen diagnoses) with material & age")
        void buildsRowsAndMergesDiagnoses() {
            // Arrange
            FhirApi api = mock(FhirApi.class);

            LocalDate birth = LocalDate.of(1990, 1, 1);
            LocalDate collected1 = LocalDate.of(2010, 6, 15); // age 20
            Patient p = patient("p1", Enumerations.AdministrativeGender.FEMALE, birth);

            Specimen s = specimen("s1", collected1, "FFPE", null); // type text path

            // Specimen grouping
            when(api.fetchSpecimensByCollection(any()))
                    .thenReturn(Map.of(COLL, List.of(s)));

            // Resolve patient for specimen
            when(api.extractPatientFromSpecimen(s)).thenReturn(p);

            // Diagnoses: patient has C10; specimen has C10 and E23.1 → merged and deduped → {C10, E23.1}
            when(api.extractConditionCodesFromPatient(p)).thenReturn(List.of("C10"));
            when(api.extractDiagnosesFromSpecimen(s)).thenReturn(List.of("C10", "E23.1"));

            PopulateStarModelInputData svc = new PopulateStarModelInputData(api);

            // Act
            StarModelInput out = svc.populate(COLL);
            List<Map<String,String>> rows = rowsFor(out, COLL);

            // Assert
            // Expect 2 rows: one per distinct diagnosis
            assertEquals(2, rows.size());

            // All rows are for our collection
            assertTrue(rows.stream().allMatch(r -> COLL.equals(r.get("collection"))));

            // Material text "FFPE" is passed; StarModelInputRow converts to TISSUE_PARAFFIN_EMBEDDED in asMap()
            // So we need to build the row via Populate → StarModelInputRow → asMap.
            // The key in final map is "sample_material" at input stage; in fact rows for StarModelInput it is "sample_material".
            // Here we only check that the chosen text path was used (not null/empty).
            assertTrue(rows.stream().allMatch(r -> "FFPE".equals(r.get("sample_material"))));

            // Diagnoses are distinct and present
            Set<String> diagnoses = rows.stream().map(r -> r.get("hist_loc")).collect(Collectors.toSet());
            assertEquals(Set.of("urn:miriam:icd:C10", "urn:miriam:icd:E23.1"), diagnoses);
        }

        @Test
        @DisplayName("Material from coding when no text present")
        void materialFromCodingWhenNoText() {
            FhirApi api = mock(FhirApi.class);

            Patient p = patient("p2", Enumerations.AdministrativeGender.MALE, LocalDate.of(1980,1,1));
            Specimen s = specimen("sX", LocalDate.of(2000,1,1), null, "BLOOD_SERUM"); // coding path

            when(api.fetchSpecimensByCollection(any())).thenReturn(Map.of(COLL, List.of(s)));
            when(api.extractPatientFromSpecimen(s)).thenReturn(p);
            when(api.findAllSpecimensWithReferencesToPatient(p)).thenReturn(List.of(s));
            when(api.extractConditionCodesFromPatient(p)).thenReturn(List.of("C00"));
            when(api.extractDiagnosesFromSpecimen(s)).thenReturn(Collections.emptyList());

            PopulateStarModelInputData svc = new PopulateStarModelInputData(api);
            StarModelInput out = svc.populate(COLL);
            List<Map<String,String>> rows = rowsFor(out, COLL);

            assertEquals(1, rows.size());
            assertEquals("SERUM", rows.get(0).get("sample_material"));
        }
    }

    @Nested
    class AgeCornerCases {

        @Test
        @DisplayName("Age is null if earliest collection date is null")
        void ageNullIfNoCollectionDate() {
            FhirApi api = mock(FhirApi.class);

            Patient p = patient("p3", Enumerations.AdministrativeGender.MALE, LocalDate.of(2000,1,1));
            Specimen s = specimen("sNoDate", null, "FFPE", null); // no collected date

            when(api.fetchSpecimensByCollection(any())).thenReturn(Map.of(COLL, List.of(s)));
            when(api.extractPatientFromSpecimen(s)).thenReturn(p);
            when(api.findAllSpecimensWithReferencesToPatient(p)).thenReturn(List.of(s));
            when(api.extractConditionCodesFromPatient(p)).thenReturn(List.of("C10"));
            when(api.extractDiagnosesFromSpecimen(s)).thenReturn(Collections.emptyList());

            PopulateStarModelInputData svc = new PopulateStarModelInputData(api);
            StarModelInput out = svc.populate(COLL);
            List<Map<String,String>> rows = rowsFor(out, COLL);

            assertEquals(1, rows.size());
            assertNull(rows.get(0).get("age_at_primary_diagnosis"));
        }

        @Test
        @DisplayName("Age becomes null if earliest collection date is before birth (negative age guard)")
        void ageNullIfNegative() {
            FhirApi api = mock(FhirApi.class);

            LocalDate birth = LocalDate.of(2010, 1, 1);
            Patient p = patient("p4", Enumerations.AdministrativeGender.MALE, birth);
            Specimen sEarly = specimen("sEarly", LocalDate.of(2009, 1, 1), "FFPE", null);

            when(api.fetchSpecimensByCollection(any())).thenReturn(Map.of(COLL, List.of(sEarly)));
            when(api.extractPatientFromSpecimen(sEarly)).thenReturn(p);
            when(api.findAllSpecimensWithReferencesToPatient(p)).thenReturn(List.of(sEarly));
            when(api.extractConditionCodesFromPatient(p)).thenReturn(List.of("C10"));
            when(api.extractDiagnosesFromSpecimen(sEarly)).thenReturn(Collections.emptyList());

            PopulateStarModelInputData svc = new PopulateStarModelInputData(api);
            StarModelInput out = svc.populate(COLL);
            List<Map<String,String>> rows = rowsFor(out, COLL);

            assertEquals(1, rows.size());
            assertNull(rows.get(0).get("age_at_primary_diagnosis"));
        }

        @Test
        @DisplayName("Age null if patient birthdate missing")
        void ageNullIfBirthdateMissing() {
            FhirApi api = mock(FhirApi.class);

            Patient p = patient("p5", Enumerations.AdministrativeGender.FEMALE, null); // no birthdate
            Specimen s = specimen("sHasDate", LocalDate.of(2015, 1, 1), "FFPE", null);

            when(api.fetchSpecimensByCollection(any())).thenReturn(Map.of(COLL, List.of(s)));
            when(api.extractPatientFromSpecimen(s)).thenReturn(p);
//            when(api.findAllSpecimensWithReferencesToPatient(p)).thenReturn(List.of(s));
            when(api.extractConditionCodesFromPatient(p)).thenReturn(List.of("C10"));
            when(api.extractDiagnosesFromSpecimen(s)).thenReturn(Collections.emptyList());

            PopulateStarModelInputData svc = new PopulateStarModelInputData(api);
            StarModelInput out = svc.populate(COLL);
            List<Map<String,String>> rows = rowsFor(out, COLL);

            assertEquals(1, rows.size());
            assertNull(rows.get(0).get("age_at_primary_diagnosis"));
        }
    }

    @Nested
    class DedupingDiagnoses {

        @Test
        @DisplayName("Duplicate diagnoses across patient/specimen are deduped")
        void dedupDiagnoses() {
            FhirApi api = mock(FhirApi.class);

            Patient p = patient("p6", Enumerations.AdministrativeGender.FEMALE, LocalDate.of(1980,1,1));
            Specimen s = specimen("s6", LocalDate.of(2000,1,1), "FFPE", null);

            when(api.fetchSpecimensByCollection(any())).thenReturn(Map.of(COLL, List.of(s)));
            when(api.extractPatientFromSpecimen(s)).thenReturn(p);
            when(api.findAllSpecimensWithReferencesToPatient(p)).thenReturn(List.of(s));
            when(api.extractConditionCodesFromPatient(p)).thenReturn(List.of("C10", "E23.1"));
            when(api.extractDiagnosesFromSpecimen(s)).thenReturn(List.of("C10", "C10", "E23.1"));

            PopulateStarModelInputData svc = new PopulateStarModelInputData(api);
            StarModelInput out = svc.populate(COLL);
            List<Map<String,String>> rows = rowsFor(out, COLL);

            // Expect only 2 rows: C10 and E23.1 once each
            assertEquals(2, rows.size());
            Set<String> diagnoses = rows.stream().map(r -> r.get("hist_loc")).collect(Collectors.toSet());
            assertEquals(Set.of("urn:miriam:icd:C10", "urn:miriam:icd:E23.1"), diagnoses);
        }
    }
}
