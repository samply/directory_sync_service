package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import de.samply.directory_sync_service.converter.Icd10WhoNormalizer;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DiagnosisCorrectionsTest {

    @Mock
    private FhirApi fhirApi;

    @Mock
    private DirectoryApi directoryApi;

    @Test
    void returnsEmptyMapWhenFhirDiagnosesIsNull() {
        when(fhirApi.fetchDiagnoses("col")).thenReturn(null);

        Map<String, String> result =
                DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApi, "col");

        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(fhirApi).fetchDiagnoses("col");
        verifyNoInteractions(directoryApi);
    }

    @Test
    void returnsNullOnException() {
        when(fhirApi.fetchDiagnoses(any())).thenThrow(new RuntimeException("boom"));

        Map<String, String> result =
                DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApi, "col");

        assertNull(result);
    }

    @Test
    void leavesValidDiagnosesUntouched() {
        List<String> fhirDiagnoses = Arrays.asList("raw1", "raw2");
        when(fhirApi.fetchDiagnoses(null)).thenReturn(fhirDiagnoses);

        try (MockedStatic<FhirToDirectoryAttributeConverter> mocked = mockStatic(FhirToDirectoryAttributeConverter.class)) {
            mocked.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("raw1"))
                    .thenReturn("urn:miriam:icd:C75.4");
            mocked.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("raw2"))
                    .thenReturn("urn:miriam:icd:D62");

            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75.4")).thenReturn(true);
            when(directoryApi.isValidIcdValue("urn:miriam:icd:D62")).thenReturn(true);

            Map<String, String> result =
                    DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApi, null);

            assertEquals(2, result.size());
            assertEquals("urn:miriam:icd:C75.4", result.get("urn:miriam:icd:C75.4"));
            assertEquals("urn:miriam:icd:D62", result.get("urn:miriam:icd:D62"));

            verify(directoryApi).isValidIcdValue("urn:miriam:icd:C75.4");
            verify(directoryApi).isValidIcdValue("urn:miriam:icd:D62");
        }
    }

    @Test
    void invalidDiagnosis_correctedByNormalizedCode_whenNormalizedBecomesValid() {
        when(fhirApi.fetchDiagnoses(null)).thenReturn(List.of("raw"));

        try (MockedStatic<FhirToDirectoryAttributeConverter> conv = mockStatic(FhirToDirectoryAttributeConverter.class);
             MockedStatic<Icd10WhoNormalizer> norm = mockStatic(Icd10WhoNormalizer.class)) {

            // initial conversion
            conv.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("raw"))
                    .thenReturn("urn:miriam:icd:C75.5"); // invalid

            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75.5")).thenReturn(false);

            // normalization path
            norm.when(() -> Icd10WhoNormalizer.normalize("raw")).thenReturn("C75.4");
            conv.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("C75.4"))
                    .thenReturn("urn:miriam:icd:C75.4");

            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75.4")).thenReturn(true);

            Map<String, String> result =
                    DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApi, null);

            assertEquals(1, result.size());
            assertEquals("urn:miriam:icd:C75.4", result.get("urn:miriam:icd:C75.5"));
        }
    }

    @Test
    void invalidDiagnosis_correctedToNormalizedCategory_whenNormalizedInvalidButCategoryValid() {
        when(fhirApi.fetchDiagnoses(null)).thenReturn(List.of("raw"));

        try (MockedStatic<FhirToDirectoryAttributeConverter> conv = mockStatic(FhirToDirectoryAttributeConverter.class);
             MockedStatic<Icd10WhoNormalizer> norm = mockStatic(Icd10WhoNormalizer.class)) {

            conv.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("raw"))
                    .thenReturn("urn:miriam:icd:C75.5"); // invalid
            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75.5")).thenReturn(false);

            // normalization returns a code that still won't validate as full code
            norm.when(() -> Icd10WhoNormalizer.normalize("raw")).thenReturn("C75.51");
            conv.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("C75.51"))
                    .thenReturn("urn:miriam:icd:C75.51");

            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75.51")).thenReturn(false);
            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75")).thenReturn(true); // category valid

            Map<String, String> result =
                    DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApi, null);

            assertEquals("urn:miriam:icd:C75", result.get("urn:miriam:icd:C75.5"));
        }
    }

    @Test
    void invalidDiagnosis_correctedToOriginalCategory_whenNormalizedPathFailsButOriginalCategoryValid() {
        when(fhirApi.fetchDiagnoses(null)).thenReturn(List.of("raw"));

        try (MockedStatic<FhirToDirectoryAttributeConverter> conv = mockStatic(FhirToDirectoryAttributeConverter.class);
             MockedStatic<Icd10WhoNormalizer> norm = mockStatic(Icd10WhoNormalizer.class)) {

            conv.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("raw"))
                    .thenReturn("urn:miriam:icd:C75.5"); // invalid
            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75.5")).thenReturn(false);

            // normalized code -> still invalid, and its category invalid too
            norm.when(() -> Icd10WhoNormalizer.normalize("raw")).thenReturn("C75.51");
            conv.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("C75.51"))
                    .thenReturn("urn:miriam:icd:C75.51");

            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75.51")).thenReturn(false);
            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75")).thenReturn(false);

            // original category check succeeds
            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75")).thenReturn(true);

            Map<String, String> result =
                    DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApi, null);

            assertEquals("urn:miriam:icd:C75", result.get("urn:miriam:icd:C75.5"));
        }
    }

    @Test
    void invalidDiagnosis_becomesNull_whenNoCategoryValid() {
        when(fhirApi.fetchDiagnoses(null)).thenReturn(List.of("raw"));

        try (MockedStatic<FhirToDirectoryAttributeConverter> conv = mockStatic(FhirToDirectoryAttributeConverter.class);
             MockedStatic<Icd10WhoNormalizer> norm = mockStatic(Icd10WhoNormalizer.class)) {

            conv.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("raw"))
                    .thenReturn("urn:miriam:icd:C75.5"); // invalid
            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75.5")).thenReturn(false);

            norm.when(() -> Icd10WhoNormalizer.normalize("raw")).thenReturn("C75.51");
            conv.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("C75.51"))
                    .thenReturn("urn:miriam:icd:C75.51");

            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75.51")).thenReturn(false);
            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75")).thenReturn(false);

            // original category also invalid
            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75")).thenReturn(false);

            Map<String, String> result =
                    DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApi, null);

            assertTrue(result.containsKey("urn:miriam:icd:C75.5"));
            assertNull(result.get("urn:miriam:icd:C75.5"));
        }
    }

    @Test
    void duplicatesOverwriteInMap() {
        when(fhirApi.fetchDiagnoses(null)).thenReturn(List.of("raw1", "raw2"));

        try (MockedStatic<FhirToDirectoryAttributeConverter> conv = mockStatic(FhirToDirectoryAttributeConverter.class)) {
            // both raw values convert to same MIRIAM code
            conv.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("raw1"))
                    .thenReturn("urn:miriam:icd:C75.4");
            conv.when(() -> FhirToDirectoryAttributeConverter.convertDiagnosis("raw2"))
                    .thenReturn("urn:miriam:icd:C75.4");

            when(directoryApi.isValidIcdValue("urn:miriam:icd:C75.4")).thenReturn(true);

            Map<String, String> result =
                    DiagnosisCorrections.generateDiagnosisCorrections(fhirApi, directoryApi, null);

            // only one key remains
            assertEquals(1, result.size());
            assertEquals("urn:miriam:icd:C75.4", result.get("urn:miriam:icd:C75.4"));
        }
    }
}

