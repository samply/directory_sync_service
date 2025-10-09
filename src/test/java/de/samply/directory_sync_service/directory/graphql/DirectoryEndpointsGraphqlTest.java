package de.samply.directory_sync_service.directory.graphql;

import de.samply.directory_sync_service.directory.DirectoryEndpoints;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DirectoryEndpointsGraphqlTest {

    @Nested
    @DisplayName("Endpoint values")
    class EndpointValues {
        @Test
        @DisplayName("API endpoint is /api/graphql")
        void apiEndpoint() {
            DirectoryEndpointsGraphql ep = new DirectoryEndpointsGraphql();
            assertEquals("/api/graphql", ep.getApiEndpoint());
        }

        @Test
        @DisplayName("DirectoryOntologies endpoint is /DirectoryOntologies/api/graphql")
        void directoryOntologiesEndpoint() {
            DirectoryEndpointsGraphql ep = new DirectoryEndpointsGraphql();
            assertEquals("/DirectoryOntologies/api/graphql", ep.getDatabaseDirectoryOntologiesEndpoint());
        }

        @Test
        @DisplayName("Login endpoint equals API endpoint")
        void loginEqualsApiEndpoint() {
            DirectoryEndpointsGraphql ep = new DirectoryEndpointsGraphql();
            assertEquals(ep.getApiEndpoint(), ep.getLoginEndpoint());
            assertEquals("/api/graphql", ep.getLoginEndpoint());
        }
    }

    @Nested
    @DisplayName("Type/Inheritance")
    class TypeChecks {
        @Test
        @DisplayName("Implements DirectoryEndpoints contract")
        void isDirectoryEndpoints() {
            DirectoryEndpoints ep = new DirectoryEndpointsGraphql();
            // Polymorphic call should compile and return the same value as concrete
            assertEquals("/api/graphql", ep.getLoginEndpoint());
        }
    }
}
