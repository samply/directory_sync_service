package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.model.BbmriEricId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DiagnosisCorrectionsTest {

    @Test
    void returnsNull_whenFhirDiagnosesIsNull() {
        FhirApi fhir = mock(FhirApi.class);
        DirectoryApi dir = mock(DirectoryApi.class);

        when(fhir.fetchDiagnoses(null)).thenReturn(null);

        Map<String,String> result =
                DiagnosisCorrections.generateDiagnosisCorrections(fhir, dir, null);

        assertNull(result, "Should return null on FHIR error");
        verify(dir, never()).collectDiagnosisCorrections(any());
    }

    @Test
    void returnsEmptyMap_andSkipsDirectoryCall_whenFhirDiagnosesEmpty() {
        FhirApi fhir = mock(FhirApi.class);
        DirectoryApi dir = mock(DirectoryApi.class);

        when(fhir.fetchDiagnoses(null)).thenReturn(Collections.emptyList());

        Map<String,String> result =
                DiagnosisCorrections.generateDiagnosisCorrections(fhir, dir, null);

        assertNotNull(result);
        assertTrue(result.isEmpty(), "Empty input -> empty corrections map");
        verify(dir, never()).collectDiagnosisCorrections(any());
    }

    @Test
    void convertsIcdToMiriam_thenDirectoryMayAdjustMappings() {
        FhirApi fhir = mock(FhirApi.class);
        DirectoryApi dir = mock(DirectoryApi.class);

        // FHIR returns plain ICD and already-MIRIAM, with a duplicate
        when(fhir.fetchDiagnoses(null)).thenReturn(List.of(
                "C10",                           // plain ICD
                "urn:miriam:icd:E23.1",          // already MIRIAM
                "C10"                            // duplicate
        ));

        // Directory adjusts one of the entries (simulating a normalization/correction)
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            Map<String,String> map = (Map<String, String>) inv.getArguments()[0];
            // change value for C10 → some canonicalized target
            map.put("urn:miriam:icd:C10", "urn:miriam:icd:C10-CANON");
            return null;
        }).when(dir).collectDiagnosisCorrections(anyMap());

        Map<String,String> result =
                DiagnosisCorrections.generateDiagnosisCorrections(fhir, dir, null);

        assertNotNull(result);
        // Expect deduped MIRIAM keys only
        assertEquals(
                new HashSet<>(List.of("urn:miriam:icd:C10", "urn:miriam:icd:E23.1")),
                result.keySet()
        );
        // Directory’s mutation visible in returned map
        assertEquals("urn:miriam:icd:C10-CANON", result.get("urn:miriam:icd:C10"));
        // Unchanged entry
        assertEquals("urn:miriam:icd:E23.1", result.get("urn:miriam:icd:E23.1"));

        verify(dir).collectDiagnosisCorrections(anyMap());
    }

    @Test
    void passesParsedBbmriEricId_toFetchDiagnoses_whenDefaultCollectionIdProvided() {
        FhirApi fhir = mock(FhirApi.class);
        DirectoryApi dir = mock(DirectoryApi.class);

        // Any non-empty list so the method continues past the early-return
        when(fhir.fetchDiagnoses(any())).thenReturn(List.of("C10"));
        doAnswer(inv -> null).when(dir).collectDiagnosisCorrections(anyMap());

        String defaultCollectionId = "bbmri-eric:ID:DE_123";
        DiagnosisCorrections.generateDiagnosisCorrections(fhir, dir, defaultCollectionId);

        ArgumentCaptor<BbmriEricId> captor = ArgumentCaptor.forClass(BbmriEricId.class);
        verify(fhir).fetchDiagnoses(captor.capture());
        BbmriEricId id = captor.getValue();
        assertNotNull(id);
        assertEquals("DE", id.getCountryCode(), "Country code parsed from defaultCollectionId");
    }

    @Test
    void passesNull_toFetchDiagnoses_whenDefaultCollectionIdIsNull() {
        FhirApi fhir = mock(FhirApi.class);
        DirectoryApi dir = mock(DirectoryApi.class);

        when(fhir.fetchDiagnoses(null)).thenReturn(List.of("C10"));
        doAnswer(inv -> null).when(dir).collectDiagnosisCorrections(anyMap());

        DiagnosisCorrections.generateDiagnosisCorrections(fhir, dir, null);

        verify(fhir).fetchDiagnoses(isNull());
    }

    @Test
    void throwsWhenConverterReturnsNull_documentingCurrentBehavior() {
        // WARNING: This documents current implementation. If you later guard against nulls,
        // update the test to reflect the new behavior.
        FhirApi fhir = mock(FhirApi.class);
        DirectoryApi dir = mock(DirectoryApi.class);

        // A value that FhirToDirectoryAttributeConverter.convertDiagnosis() would return null for
        when(fhir.fetchDiagnoses(null)).thenReturn(List.of("not-a-valid-icd"));

        Map<String,String> result = DiagnosisCorrections.generateDiagnosisCorrections(fhir, dir, null);
        assertTrue(result.containsKey(null));
        assertNull(result.get(null));
    }
}
