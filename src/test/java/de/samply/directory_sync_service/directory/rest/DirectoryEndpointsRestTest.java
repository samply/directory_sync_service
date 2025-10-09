package de.samply.directory_sync_service.directory.rest;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DirectoryEndpointsRestTest {

    private final DirectoryEndpointsRest endpoints = new DirectoryEndpointsRest();

    @Nested
    @DisplayName("Fixed endpoints")
    class FixedEndpoints {
        @Test
        void loginEndpoint_isConstant() {
            assertEquals("/api/v1/login", endpoints.getLoginEndpoint());
        }

        @Test
        void diseaseTypeEndpoint_isConstant() {
            assertEquals("/api/v2/eu_bbmri_eric_disease_types", endpoints.getDiseaseTypeEndpoint());
        }
    }

    @Nested
    @DisplayName("Country-scoped endpoints – biobanks")
    class BiobankEndpoints {
        @ParameterizedTest(name = "country=\"{0}\" -> {1}")
        @CsvSource({
                "DE,   /api/v2/eu_bbmri_eric_DE_biobanks",
                "de,   /api/v2/eu_bbmri_eric_de_biobanks",   // case is preserved
                "'',   /api/v2/eu_bbmri_eric_biobanks",      // empty -> no country prefix
                ",     /api/v2/eu_bbmri_eric_biobanks"       // null -> no country prefix
        })
        void biobankEndpoint_variousCountryCodes(String country, String expected) {
            assertEquals(expected, endpoints.getBiobankEndpoint(country));
        }
    }

    @Nested
    @DisplayName("Country-scoped endpoints – collections")
    class CollectionEndpoints {
        @ParameterizedTest(name = "country=\"{0}\" -> {1}")
        @CsvSource({
                "DE,   /api/v2/eu_bbmri_eric_DE_collections",
                "'',   /api/v2/eu_bbmri_eric_collections",
                ",     /api/v2/eu_bbmri_eric_collections"
        })
        void collectionEndpoint_variousCountryCodes(String country, String expected) {
            assertEquals(expected, endpoints.getCollectionEndpoint(country));
        }
    }

    @Nested
    @DisplayName("Country-scoped endpoints – facts")
    class FactEndpoints {
        @ParameterizedTest(name = "country=\"{0}\" -> {1}")
        @CsvSource({
                "DE,   /api/v2/eu_bbmri_eric_DE_facts",
                "'',   /api/v2/eu_bbmri_eric_facts",
                ",     /api/v2/eu_bbmri_eric_facts"
        })
        void factEndpoint_variousCountryCodes(String country, String expected) {
            assertEquals(expected, endpoints.getFactEndpoint(country));
        }
    }
}
