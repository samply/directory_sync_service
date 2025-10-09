package de.samply.directory_sync_service.directory.graphql;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.model.BbmriEricId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DirectoryApiGraphql using a simple FakeDirectoryCallsGraphql.
 * No network; assertions are on behavior & generated GraphQL.
 */
class DirectoryApiGraphqlTest {

    private TestableApi api;
    private FakeDirectoryCallsGraphql fake;

    @BeforeEach
    void setUp() throws Exception {
        // Construct the real API (it creates its own DirectoryCallsGraphql)
        api = new TestableApi("https://example.org", false, "user@x", "secret");

        // Swap in our fake via reflection
        fake = new FakeDirectoryCallsGraphql();
        setPrivateFinal(api, "directoryCallsGraphql", fake);

        // By default, make /api/graphql _schemas contain both ERIC-DE and BBMRI-ERIC
        fake.schemas = List.of("ERIC-DE", "BBMRI-ERIC", "DirectoryOntologies");
    }

    // ---------- helpers ----------

    private static void setPrivateFinal(Object target, String fieldName, Object value) throws Exception {
        Field f = DirectoryApiGraphql.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }

    /** Subclass only to expose constructor; production logic unchanged. */
    static class TestableApi extends DirectoryApiGraphql {
        TestableApi(String baseUrl, boolean mock, String user, String pass) {
            super(baseUrl, mock, user, pass);
        }
    }

    /** Minimal fake for DirectoryCallsGraphql: returns scripted Json and records calls. */
    static class FakeDirectoryCallsGraphql extends DirectoryCallsGraphql {
        FakeDirectoryCallsGraphql() { super("https://example.org", "u", "p"); }

        String token;
        List<String> schemas = List.of();                      // returned by {_schemas {label}}
        Map<String, JsonObject> nextByCommand = new HashMap<>();// keyed by command substring
        List<String> seenCommands = new ArrayList<>();         // all graphql cmds seen
        List<String> seenEndpoints = new ArrayList<>();        // endpoints used
        AtomicInteger schemaCalls = new AtomicInteger();

        @Override public void setToken(String token) { this.token = token; }

        @Override
        public JsonObject runGraphqlCommand(String endpoint, String graphqlCommand) {
            seenEndpoints.add(endpoint);
            seenCommands.add(graphqlCommand);

            // Simulate {_schemas { label }}
            if (graphqlCommand.contains("_schemas")) {
                schemaCalls.incrementAndGet();
                var data = new JsonObject();
                var arr = new com.google.gson.JsonArray();
                for (var label : schemas) {
                    var obj = new JsonObject();
                    obj.addProperty("label", label);
                    arr.add(obj);
                }
                data.add("_schemas", arr);
                return data;
            }

            // Simulate _schema { tables { name columns { name } } } for column introspection
            if (graphqlCommand.contains("_schema") && graphqlCommand.contains("tables")) {
                JsonObject data = nextByCommand.getOrDefault("_schema_tables",
                        JsonParser.parseString("""
                          { "_schema": { "tables": [
                              { "name":"Collections", "columns":[{"name":"id"},{"name":"name"}] },
                              { "name":"CollectionFacts", "columns":[{"name":"id"},{"name":"national_node"}] }
                          ]}}
                        """).getAsJsonObject());
                return data;
            }

            // Generic: return any specifically registered stub by substring key
            for (var key : nextByCommand.keySet()) {
                if (graphqlCommand.contains(key)) return nextByCommand.get(key);
            }

            // Default: a success envelope
            return JsonParser.parseString("{\"ok\":true}").getAsJsonObject();
        }

        @Override
        public List<Map<String, Object>> runGraphqlQueryReturnList(String endpoint, String dataTableName, String filter, List<String> attributeNames) {
            seenEndpoints.add(endpoint);
            seenCommands.add("Q:" + dataTableName + "|" + String.valueOf(filter));

            // keyed by table name
            if ("Biobanks".equals(dataTableName)) {
                // The map-return path uses runGraphqlQueryReturnMap; not needed here
                return List.of();
            }
            if ("CollectionFacts".equals(dataTableName)) {
                // Provide a few IDs to drive getNextPageOfFactIdsForCollection
                return List.of(
                        Map.of("id", "F1"),
                        Map.of("id", "F2"),
                        Map.of("id", "F3")
                );
            }
            if ("DiseaseTypes".equals(dataTableName)) {
                // Most tests stub via nextByCommand in runGraphqlCommand(), but allow default empty
                return List.of();
            }
            // Default
            return List.of();
        }

        @Override
        public Map<String, Object> runGraphqlQueryReturnMap(String endpoint, String table, String filter, List<String> attrs) {
            seenEndpoints.add(endpoint);
            seenCommands.add("QM:" + table + "|" + filter);

            // Provide a minimal Biobank row with id + name
            if ("Biobanks".equals(table)) {
                var m = new HashMap<String,Object>();
                // pull id value out of filter if possible
                String id = "bbmri-eric:ID:XX";
                int i = filter.indexOf("equals: \"");
                if (i >= 0) {
                    int j = filter.indexOf('"', i + 9);
                    if (j > i) id = filter.substring(i + 9, j);
                }
                m.put("id", id);
                m.put("name", "My Bio");
                return m;
            }
            return null;
        }
    }

    // ---------- tests ----------

    @Nested
    class Login {
        @Test
        void login_success_setsToken_andReturnsTrue() {
            // stub signin result with token
            fake.nextByCommand.put("signin", JsonParser.parseString("""
              {"signin":{"message":"OK","token":"T0K3N"}}
            """).getAsJsonObject());

            assertTrue(api.login());
            assertEquals("T0K3N", fake.token);
        }

        @Test
        void login_nullData_returnsFalse() {
            fake.nextByCommand.put("signin", null); // runGraphqlCommand returns default -> not containing "signin"
            assertFalse(api.login());
        }

        @Test
        void login_missingToken_returnsFalse() {
            fake.nextByCommand.put("signin", JsonParser.parseString("""
              {"signin":{"message":"OK"}}
            """).getAsJsonObject());
            assertFalse(api.login());
        }
    }

    @Nested
    class DatabaseEndpointHeuristics {
        @Test
        void picks_exact_ERIC_country_first_and_caches() throws Exception {
            // default schemas include ERIC-DE
            var endpoint1 = invokeGetDatabaseEricEndpoint("DE");
            var endpoint2 = invokeGetDatabaseEricEndpoint("DE"); // cached
            assertEquals("ERIC-DE/api/graphql", endpoint1);
            assertEquals(endpoint1, endpoint2);
            // Only one _schemas call despite two invocations
            assertEquals(1, fake.schemaCalls.get());
        }

        @Test
        void falls_back_to_BBMRI_ERIC_if_no_country_specific() throws Exception {
            fake.schemas = List.of("BBMRI-ERIC", "DirectoryOntologies");
            var endpoint = invokeGetDatabaseEricEndpoint("CY");
            assertEquals("BBMRI-ERIC/api/graphql", endpoint);
        }

        // reflectively call private getDatabaseEricEndpoint
        private String invokeGetDatabaseEricEndpoint(String cc) throws Exception {
            var m = DirectoryApiGraphql.class.getDeclaredMethod("getDatabaseEricEndpoint", String.class);
            m.setAccessible(true);
            return (String) m.invoke(api, cc);
        }
    }

    @Nested
    class ColumnIntrospection {
        @Test
        void isColumnInTable_true_and_cached() throws Exception {
            // First call hits schema; second is cached
            boolean first = invokeIsColumnInTable("DE", "CollectionFacts", "national_node");
            boolean second = invokeIsColumnInTable("DE", "CollectionFacts", "national_node");
            assertTrue(first);
            assertTrue(second);
            assertEquals(1, fake.schemaCalls.get());
        }

        @Test
        void isColumnInTable_false_when_absent() throws Exception {
            // Remove national_node from fake schema reply
            fake.nextByCommand.put("_schema_tables", JsonParser.parseString("""
              {"_schema":{"tables":[
                {"name":"CollectionFacts","columns":[{"name":"id"}]}
              ]}}
            """).getAsJsonObject());
            assertFalse(invokeIsColumnInTable("DE", "CollectionFacts", "national_node"));
        }

        private boolean invokeIsColumnInTable(String cc, String t, String c) throws Exception {
            var m = DirectoryApiGraphql.class.getDeclaredMethod("isColumnInTable", String.class, String.class, String.class);
            m.setAccessible(true);
            return (boolean) m.invoke(api, cc, t, c);
        }
    }

    @Nested
    class FactsUpdateAndDelete {
        @Test
        void updateFacts_excludes_national_node_when_schema_lacks_it() {
            // Schema without national_node
            fake.nextByCommand.put("_schema_tables", JsonParser.parseString("""
              {"_schema":{"tables":[
                {"name":"CollectionFacts","columns":[{"name":"id"}]}
              ]}}
            """).getAsJsonObject());

            String last = fake.seenCommands.stream().filter(s -> s.contains("insert(")).reduce((a,b) -> b).orElse("");
            assertFalse(last.contains("national_node"));
        }

        @Test
        void deleteFacts_deletes_each_id() {
            assertTrue(api.deleteFactsByIds("DE", List.of("F1","F2","F3")));
            long deletes = fake.seenCommands.stream().filter(s -> s.contains("delete( CollectionFacts")).count();
            assertEquals(3, deletes);
        }
    }

    @Nested
    class FactPagingToggle {
        @Test
        void getNextPageOfFactIds_returnsOnce_thenEmpty() {
            List<String> first = api.getNextPageOfFactIdsForCollection("bbmri-eric:ID:DE_1");
            List<String> second = api.getNextPageOfFactIdsForCollection("bbmri-eric:ID:DE_1");
            assertEquals(List.of("F1","F2","F3"), first);
            assertTrue(second.isEmpty());
        }
    }

    @Nested
    class IcdValidation {
        @Test
        void invalid_when_empty_list() throws Exception {
            fake.nextByCommand.put("DiseaseTypes",
                    JsonParser.parseString("{\"data\":{\"DiseaseTypes\":[]}}").getAsJsonObject());
            assertFalse(invokeIsValid("C10"));
        }

        private boolean invokeIsValid(String code) throws Exception {
            var m = DirectoryApiGraphql.class.getDeclaredMethod("isValidIcdValue", String.class);
            m.setAccessible(true);
            return (boolean) m.invoke(api, code);
        }
    }

    @Nested
    class BiobankFetch {
        @Test
        void fetchBiobank_minimal_happyPath() {
            // _schemas includes ERIC-DE so endpoint will be ERIC-DE/api/graphql
            var id = BbmriEricId.valueOf("bbmri-eric:ID:DE_BBK").orElseThrow();
            Biobank b = api.fetchBiobank(id);
            assertNotNull(b);
            assertEquals("bbmri-eric:ID:DE_BBK", b.getId());
        }
    }
}
