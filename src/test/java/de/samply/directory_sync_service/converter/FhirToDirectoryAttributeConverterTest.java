package de.samply.directory_sync_service.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class FhirToDirectoryAttributeConverterTest {

    // ---------- convertSex ----------

    @Test
    @DisplayName("convertSex uppercases non-null input")
    void convertSex_uppercases() {
        assertEquals("MALE", FhirToDirectoryAttributeConverter.convertSex("male"));
        assertEquals("F", FhirToDirectoryAttributeConverter.convertSex("f"));
        assertEquals("OTHER", FhirToDirectoryAttributeConverter.convertSex("Other"));
    }

    @Test
    @DisplayName("convertSex(null) throws NullPointerException (documents current behavior)")
    void convertSex_null_throwsNpe() {
        assertThrows(NullPointerException.class, () -> FhirToDirectoryAttributeConverter.convertSex(null));
    }

    // ---------- convertMaterial ----------

    static Stream<String[]> materialMappings() {
        return Stream.of(
                // basic normalization: uppercase + '-' -> '_'
                new String[]{"whole-blood", "WHOLE_BLOOD"},
                // drops _VITAL suffix
                new String[]{"serum_vital", "SERUM"},
                new String[]{"plasma-vital", "PLASMA"}, // note: not in explicit map; still normalizes
                // specific renames
                new String[]{"tissue_formalin", "TISSUE_PARAFFIN_EMBEDDED"},
                new String[]{"tissue", "TISSUE_FROZEN"},
                new String[]{"cf-dna", "CDNA"},
                new String[]{"blood_serum", "SERUM"},
                new String[]{"stool_faeces", "FECES"},
                new String[]{"blood_plasma", "SERUM"},
                // map to OTHER
                new String[]{"something_other", "OTHER"},
                new String[]{"derivative", "OTHER"},
                new String[]{"csf_liquor", "OTHER"},
                new String[]{"liquid", "OTHER"},
                new String[]{"ascites", "OTHER"},
                new String[]{"bone_marrow", "OTHER"},
                new String[]{"tissue_paxgene_or_else", "OTHER"}
        );
    }

    @ParameterizedTest(name = "convertMaterial(\"{0}\") -> \"{1}\"")
    @MethodSource("materialMappings")
    void convertMaterial_maps(String input, String expected) {
        assertEquals(expected, FhirToDirectoryAttributeConverter.convertMaterial(input));
    }

    @Test
    @DisplayName("convertMaterial returns null on null input")
    void convertMaterial_null_returnsNull() {
        assertNull(FhirToDirectoryAttributeConverter.convertMaterial(null));
    }

    // ---------- convertStorageTemperature ----------

    @Test
    @DisplayName("convertStorageTemperature replaces temperatureGN with temperatureOther")
    void convertStorageTemperature_replacesGN() {
        assertEquals("temperatureOther",
                FhirToDirectoryAttributeConverter.convertStorageTemperature("temperatureGN"));
    }

    @Test
    @DisplayName("convertStorageTemperature leaves unrelated values untouched")
    void convertStorageTemperature_passthrough() {
        assertEquals("temperatureLN",
                FhirToDirectoryAttributeConverter.convertStorageTemperature("temperatureLN"));
        assertEquals("roomTemp",
                FhirToDirectoryAttributeConverter.convertStorageTemperature("roomTemp"));
    }

    @Test
    @DisplayName("convertStorageTemperature returns null on null input")
    void convertStorageTemperature_null_returnsNull() {
        assertNull(FhirToDirectoryAttributeConverter.convertStorageTemperature(null));
    }

    // ---------- convertDiagnosis ----------

    @Test
    @DisplayName("convertDiagnosis passes through already-miriam codes")
    void convertDiagnosis_miriam_passthrough() {
        String in = "urn:miriam:icd:C75";
        assertEquals(in, FhirToDirectoryAttributeConverter.convertDiagnosis(in));
    }

    @Test
    @DisplayName("convertDiagnosis prefixes 3-char ICD like C75")
    void convertDiagnosis_prefixes3() {
        assertEquals("urn:miriam:icd:C75",
                FhirToDirectoryAttributeConverter.convertDiagnosis("C75"));
    }

    @Test
    @DisplayName("convertDiagnosis prefixes 5-char ICD with dot like E23.1")
    void convertDiagnosis_prefixes5() {
        assertEquals("urn:miriam:icd:E23.1",
                FhirToDirectoryAttributeConverter.convertDiagnosis("E23.1"));
    }

    @Test
    @DisplayName("convertDiagnosis invalid formats return null (and log a warning)")
    void convertDiagnosis_invalid_returnsNull() {
        assertNull(FhirToDirectoryAttributeConverter.convertDiagnosis("C7"));     // 2 chars
        assertNull(FhirToDirectoryAttributeConverter.convertDiagnosis("E231"));   // 4 chars
        assertNull(FhirToDirectoryAttributeConverter.convertDiagnosis("E23.12")); // 6 incl dot
        assertNull(FhirToDirectoryAttributeConverter.convertDiagnosis("C75X"));   // 4 chars
    }

    @Test
    @DisplayName("convertDiagnosis returns null on null input")
    void convertDiagnosis_null_returnsNull() {
        assertNull(FhirToDirectoryAttributeConverter.convertDiagnosis(null));
    }
}
