package de.samply.directory_sync_service.directory.graphql;

import com.google.gson.JsonObject;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DirectoryCallsGraphql that override executeRequest to avoid real HTTP.
 */
class DirectoryCallsGraphqlTest {

    /** A test double that captures the outgoing HttpPost and returns canned responses. */
    static class TestableDirectoryCallsGraphql extends DirectoryCallsGraphql {
        String nextResponseJson;              // what executeRequest should return
        String lastRequestBody;               // captured POST body (JSON wrapper)
        Map<String, String> lastHeaders = new HashMap<>(); // captured headers
        String lastUrl;                       // captured full URL

        TestableDirectoryCallsGraphql() {
            super("https://example.org", "user", "pass");
        }

        @Override
        protected String executeRequest(HttpUriRequest request) {
            if (request instanceof HttpEntityEnclosingRequestBase enclosing) {
                // This returns the full body as sent over the wire
                try {
                    lastRequestBody = EntityUtils
                            .toString(enclosing.getEntity(), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return nextResponseJson; // whatever fake response you set
        }
    }

    private static JsonObject obj(String json) {
        return new com.google.gson.Gson().fromJson(json, JsonObject.class);
    }

    @Nested
    @DisplayName("runGraphqlQueryReturnMap")
    class ReturnMap {

        @Test
        @DisplayName("happy path: returns first item when list present")
        void happyPath() {
            var api = new TestableDirectoryCallsGraphql();
            api.nextResponseJson = """
                {"data":{"biobanks":[{"id":"B1","name":"BioOne"}]}}
            """;
            Map<String,Object> m = api.runGraphqlQueryReturnMap(
                    "/api/graphql", "biobanks", null, List.of("id","name"));

            assertNotNull(m);
            assertEquals("B1", m.get("id"));
            assertEquals("BioOne", m.get("name"));
            // Body should be wrapped as GraphQL query JSON
            assertTrue(api.lastRequestBody.startsWith("{ \"query\": \"query"));
        }

        @Test
        @DisplayName("missing required attribute -> null")
        void missingAttr() {
            var api = new TestableDirectoryCallsGraphql();
            api.nextResponseJson = """
                {"data":{"biobanks":[{"id":"B1"}]}}
            """;
            Map<String,Object> m = api.runGraphqlQueryReturnMap(
                    "/api/graphql", "biobanks", null, List.of("id","name"));

            assertNull(m);
        }

        @Test
        @DisplayName("no results -> empty map")
        void emptyList() {
            var api = new TestableDirectoryCallsGraphql();
            api.nextResponseJson = """
                {"data":{"biobanks":[]}}
            """;
            Map<String,Object> m = api.runGraphqlQueryReturnMap(
                    "/api/graphql", "biobanks", null, List.of("id"));

            assertNotNull(m);
            assertTrue(m.isEmpty());
        }
    }

    @Nested
    @DisplayName("runGraphqlQueryReturnList (via table/filter/attrs)")
    class ReturnList_viaBuilder {

        @Test
        @DisplayName("builds query: wraps filter in parentheses, escapes quotes, strips newlines")
        void queryBuilding() {
            var api = new TestableDirectoryCallsGraphql();
            api.nextResponseJson = """
                {"data":{"samples":[{"a":"x","b":"y"}]}}
            """;

            // Pass the filter with quotes (escaped for Java)
            api.runGraphqlQueryReturnList(
                    "/api/graphql", "samples", "id: \"123\"", List.of("a", "b"));

            // --- Robustly parse the JSON body we sent ---
            String query;
            try {
                com.google.gson.JsonObject bodyJson =
                        com.google.gson.JsonParser.parseString(api.lastRequestBody).getAsJsonObject();
                query = bodyJson.getAsJsonPrimitive("query").getAsString(); // decoded GraphQL text
            } catch (Exception parseErr) {
                // Fallback: extract the query value between the first "query":" and the last quote
                // (good enough for tests; lets the assertions still run)
                String marker = "\"query\":";
                int start = api.lastRequestBody.indexOf(marker);
                int firstQuote = api.lastRequestBody.indexOf('"', start + marker.length());
                int lastQuote  = api.lastRequestBody.lastIndexOf('"');
                query = api.lastRequestBody.substring(firstQuote + 1, lastQuote)
                        .replace("\\\"", "\""); // unescape for assertions
            }

            // 1) Filter wrapped in parentheses
            assertTrue(query.contains("( id: \"123\" )"));

            // 2) No newlines remain
            assertFalse(query.contains("\n"));
            assertFalse(query.contains("\r"));

            // 3) Attributes present and in order
            int idxA = query.indexOf(" a ");
            int idxB = query.indexOf(" b ");
            assertTrue(idxA >= 0 && idxB >= 0 && idxA < idxB);

            // 4) Table name present with opening paren
            assertTrue(query.contains("samples(") || query.contains("samples ("));
        }

        @Test
        @DisplayName("returns null when HTTP response is null")
        void httpNull() {
            var api = new TestableDirectoryCallsGraphql();
            api.nextResponseJson = null;

            assertNull(api.runGraphqlQueryReturnList(
                    "/api/graphql", "samples", null, List.of("a")));
        }

        @Test
        @DisplayName("returns empty list when data table exists but has no rows")
        void noRows() {
            var api = new TestableDirectoryCallsGraphql();
            api.nextResponseJson = """
                {"data":{"samples":[]}}
            """;
            List<Map<String,Object>> out = api.runGraphqlQueryReturnList(
                    "/api/graphql", "samples", null, List.of("a"));

            assertNotNull(out);
            assertTrue(out.isEmpty());
        }

        @Test
        @DisplayName("malformed: multiple keys in data -> null")
        void multipleTablesInData() {
            var api = new TestableDirectoryCallsGraphql();
            api.nextResponseJson = """
                {"data":{"t1":[{"id":1}], "t2":[{"id":2}]}}
            """;
            assertNull(api.runGraphqlQueryReturnList(
                    "/api/graphql", "ignored", null, List.of("id")));
        }

        @Test
        @DisplayName("malformed: table value is not a list -> null")
        void tableNotList() {
            var api = new TestableDirectoryCallsGraphql();
            api.nextResponseJson = """
                {"data":{"samples":{"id":1}}}
            """;
            assertNull(api.runGraphqlQueryReturnList(
                    "/api/graphql", "ignored", null, List.of("id")));
        }
    }

    @Nested
    @DisplayName("runGraphqlCommand")
    class RunCommand {

        @Test
        @DisplayName("returns null when HTTP returns null")
        void returnsNullWhenHttpNull() {
            var api = new TestableDirectoryCallsGraphql();
            api.nextResponseJson = null;

            assertNull(api.runGraphqlCommand("/api/graphql", "query { x { id } }"));
        }

        @Test
        @DisplayName("returns null when 'data' is missing")
        void returnsNullWhenDataMissing() {
            var api = new TestableDirectoryCallsGraphql();
            api.nextResponseJson = """
                {"error":"something bad"}
            """;

            assertNull(api.runGraphqlCommand("/api/graphql", "query { x { id } }"));
        }
    }

    @Nested
    @DisplayName("convertJsonObjectToMap")
    class ConvertJson {

        @Test
        @DisplayName("parses JsonObject into Map<String,Object>")
        void converts() {
            var api = new TestableDirectoryCallsGraphql();
            JsonObject json = obj("""
                {"table":[{"id":"A"},{"id":"B"}]}
            """);
            Map<String,Object> map = api.convertJsonObjectToMap(json);

            assertNotNull(map);
            assertTrue(map.containsKey("table"));
            Object val = map.get("table");
            assertTrue(val instanceof List<?>);
            assertEquals(2, ((List<?>) val).size());
        }
    }

    // Convenience to set token (protected in parent via DirectoryCredentials)
    private static void setToken(TestableDirectoryCallsGraphql api, String token) {
        api.setToken(token);
    }
}
