package de.samply.directory_sync_service.converter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class Icd10WhoNormalizerTest {

    // ---- normalize(String) tests ----

    @Test
    void normalize_null_returnsR69() {
        assertEquals("R69", Icd10WhoNormalizer.normalize(null));
    }

    @Test
    void normalize_emptyOrGarbage_returnsR69() {
        assertEquals("R69", Icd10WhoNormalizer.normalize(""));
        assertEquals("R69", Icd10WhoNormalizer.normalize("   "));
        assertEquals("R69", Icd10WhoNormalizer.normalize("???###"));
        assertEquals("R69", Icd10WhoNormalizer.normalize("..,,"));
    }

    @Test
    void normalize_acceptsCategoryWithoutDecimal() {
        assertEquals("C75", Icd10WhoNormalizer.normalize("C75"));
        assertEquals("C75", Icd10WhoNormalizer.normalize("c75"));
        assertEquals("C75", Icd10WhoNormalizer.normalize("  c75  "));
    }

    @Test
    void normalize_handlesCommonSeparatorsAndWhitespace() {
        assertEquals("E11.9", Icd10WhoNormalizer.normalize("E11-9"));
        assertEquals("E11.9", Icd10WhoNormalizer.normalize(" E11  . 9 "));
        assertEquals("E11.9", Icd10WhoNormalizer.normalize("E11,9"));
    }

    @Test
    void normalize_stripsTrailingAlphabeticCharactersAfterDecimal() {
        assertEquals("C85.9", Icd10WhoNormalizer.normalize("C85.9B"));
        assertEquals("A04.7", Icd10WhoNormalizer.normalize("A04.7XYZ"));
        assertEquals("C75", Icd10WhoNormalizer.normalize("C75ABC")); // no decimal -> becomes category only
    }

    @Test
    void normalize_truncatesAfterTwoDecimalDigits() {
        assertEquals("C85.97", Icd10WhoNormalizer.normalize("C85.9723124B"));
        assertEquals("C85.12", Icd10WhoNormalizer.normalize("C85.123"));
        assertEquals("C85.1", Icd10WhoNormalizer.normalize("C85.1B"));
    }

    @Test
    void normalize_skipsLeadingJunkBeforeFirstLetter() {
        assertEquals("C85.9", Icd10WhoNormalizer.normalize("12??c85.9B"));
        assertEquals("A00", Icd10WhoNormalizer.normalize("###A00###"));
    }

    @Test
    void normalize_requiresTwoDigitsAfterLetter_otherwiseR69() {
        assertEquals("R69", Icd10WhoNormalizer.normalize("C7"));
        assertEquals("R69", Icd10WhoNormalizer.normalize("C."));
        assertEquals("R69", Icd10WhoNormalizer.normalize("C"));
    }

    @Test
    void normalize_doesNotInventExtraDigits() {
        // "C750" could be interpreted as "C75.0", but this normalizer deliberately does not invent a dot
        // and will drop trailing digits because it only uses 2 digits for the category and only uses decimals when a dot exists.
        assertEquals("C75", Icd10WhoNormalizer.normalize("C750"));
    }

    // ---- isValid(String) tests ----

    @Test
    void isValid_null_false() {
        assertFalse(Icd10WhoNormalizer.isValid(null));
    }

    @Test
    void isValid_acceptsValidWhoCodes() {
        assertTrue(Icd10WhoNormalizer.isValid("C75"));
        assertTrue(Icd10WhoNormalizer.isValid("C85.9"));
        assertTrue(Icd10WhoNormalizer.isValid("E11.99"));
        assertTrue(Icd10WhoNormalizer.isValid("A00"));
    }

    @Test
    void isValid_rejectsLowercaseOrWhitespaceBecauseStrict() {
        assertFalse(Icd10WhoNormalizer.isValid("c75"));
        assertFalse(Icd10WhoNormalizer.isValid(" C75 "));
        assertFalse(Icd10WhoNormalizer.isValid("C75 "));
    }

    @Test
    void isValid_rejectsInvalidForms() {
        assertFalse(Icd10WhoNormalizer.isValid("C7"));       // too short
        assertFalse(Icd10WhoNormalizer.isValid("C750"));     // missing dot
        assertFalse(Icd10WhoNormalizer.isValid("C75."));     // empty decimal
        assertFalse(Icd10WhoNormalizer.isValid("C75.A"));    // alpha decimal
        assertFalse(Icd10WhoNormalizer.isValid("C75.123"));  // too many decimal digits
        assertFalse(Icd10WhoNormalizer.isValid(""));         // empty
        assertFalse(Icd10WhoNormalizer.isValid("R69X"));     // extra junk
    }
}

