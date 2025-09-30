package de.samply.directory_sync_service.model;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BbmriEricIdTest {

    @Nested
    class ValueOf_ValidCases {
        @Test
        @DisplayName("Parses lowercase country and normalizes to uppercase; preserves suffix")
        void parsesAndNormalizes() {
            Optional<BbmriEricId> idOpt = BbmriEricId.valueOf("bbmri-eric:ID:de_ABC-123");
            assertTrue(idOpt.isPresent());
            BbmriEricId id = idOpt.get();
            assertEquals("DE", id.getCountryCode());
            // Suffix must start with underscore per pattern "([a-zA-Z]{2})(_.+)"
            // toString should reconstruct exactly
            assertEquals("bbmri-eric:ID:DE_ABC-123", id.toString());
        }

        @Test
        @DisplayName("Supports arbitrary non-empty suffix after underscore")
        void arbitrarySuffixAfterUnderscore() {
            Optional<BbmriEricId> idOpt = BbmriEricId.valueOf("bbmri-eric:ID:FR__weird_suffix.42");
            assertTrue(idOpt.isPresent());
            assertEquals("FR", idOpt.get().getCountryCode());
            assertEquals("bbmri-eric:ID:FR__weird_suffix.42", idOpt.get().toString());
        }
    }

    @Nested
    class ValueOf_InvalidCases {
        @Test
        @DisplayName("Null input -> Optional.empty")
        void nullInput() {
            assertTrue(BbmriEricId.valueOf(null).isEmpty());
        }

        @Test
        @DisplayName("Wrong prefix -> Optional.empty")
        void wrongPrefix() {
            assertTrue(BbmriEricId.valueOf("BBMRI-ERIC:ID:DE_123").isEmpty());
            assertTrue(BbmriEricId.valueOf("bbmri-eric:id:DE_123").isEmpty());
            assertTrue(BbmriEricId.valueOf("something:else").isEmpty());
        }

        @Test
        @DisplayName("Missing underscore between country and suffix -> Optional.empty")
        void missingUnderscore() {
            assertTrue(BbmriEricId.valueOf("bbmri-eric:ID:DE123").isEmpty());
        }

        @Test
        @DisplayName("Country must be exactly two letters A-Z/a-z")
        void badCountry() {
            assertTrue(BbmriEricId.valueOf("bbmri-eric:ID:D_123").isEmpty());     // 1 letter
            assertTrue(BbmriEricId.valueOf("bbmri-eric:ID:DEU_123").isEmpty());   // 3 letters
            assertTrue(BbmriEricId.valueOf("bbmri-eric:ID:D1_123").isEmpty());    // contains digit
            assertTrue(BbmriEricId.valueOf("bbmri-eric:ID:1D_123").isEmpty());    // starts with digit
        }

        @Test
        @DisplayName("Empty/short suffix after underscore -> Optional.empty")
        void emptySuffix() {
            // pattern requires "_.+" → at least one char after underscore
            assertTrue(BbmriEricId.valueOf("bbmri-eric:ID:DE_").isEmpty());
        }
    }

    @Nested
    class IsValid {
        @Test
        void returnsTrueForValidIds() {
            assertTrue(BbmriEricId.isValidDirectoryCollectionIdentifier("bbmri-eric:ID:AT_foo"));
            assertTrue(BbmriEricId.isValidDirectoryCollectionIdentifier("bbmri-eric:ID:it_BAR-42"));
        }

        @Test
        void returnsFalseForInvalidIds() {
            assertFalse(BbmriEricId.isValidDirectoryCollectionIdentifier(null));
            assertFalse(BbmriEricId.isValidDirectoryCollectionIdentifier("bbmri-eric:ID:DE123"));
            assertFalse(BbmriEricId.isValidDirectoryCollectionIdentifier("bbmri-eric:ID:DN_")); // empty suffix
        }
    }

    @Nested
    class EqualityHashcodeToString {
        @Test
        @DisplayName("equals/hashCode: same components -> equal; different -> not equal")
        void equalsHashCode() {
            BbmriEricId a = BbmriEricId.valueOf("bbmri-eric:ID:DE_123").get();
            BbmriEricId b = BbmriEricId.valueOf("bbmri-eric:ID:de_123").get(); // lowercase country normalizes
            BbmriEricId c = BbmriEricId.valueOf("bbmri-eric:ID:DE_124").get(); // different suffix
            BbmriEricId d = BbmriEricId.valueOf("bbmri-eric:ID:AT_123").get(); // different country

            assertEquals(a, b);
            assertEquals(a.hashCode(), b.hashCode());

            assertNotEquals(a, c);
            assertNotEquals(a, d);
        }

        @Test
        @DisplayName("toString reconstructs canonical form bbmri-eric:ID:<CC><suffix>")
        void toStringFormat() {
            BbmriEricId id = BbmriEricId.valueOf("bbmri-eric:ID:nl__x").get();
            assertEquals("bbmri-eric:ID:NL__x", id.toString());
        }
    }
}
