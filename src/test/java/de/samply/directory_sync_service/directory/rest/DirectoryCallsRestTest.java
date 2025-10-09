package de.samply.directory_sync_service.directory.rest;

import com.google.gson.Gson;
import de.samply.directory_sync_service.directory.DirectoryCalls;
import de.samply.directory_sync_service.directory.DirectoryCredentials;
import org.apache.http.Header;
import org.apache.http.client.methods.*;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DirectoryCallsRestTest {

    private static final Gson GSON = new Gson();

    /** Testable SUT that intercepts HTTP. */
    static class TestableDirectoryCallsRest extends DirectoryCallsRest {
        String cannedResponse;
        HttpUriRequest lastRequest;

        TestableDirectoryCallsRest(String baseUrl, String username, String password) {
            super(baseUrl, username, password);
        }

        @Override
        protected String executeRequest(HttpUriRequest request) {
            this.lastRequest = request;
            return cannedResponse;
        }
    }

    // ---------- login() ----------

    @Test
    void login_nullResponse_false_andNoToken() {
        TestableDirectoryCallsRest rest = new TestableDirectoryCallsRest("https://host", "u", "p");
        rest.cannedResponse = null;

        assertFalse(rest.login());
        assertNull(rest.directoryCredentials.getToken());
    }

    @Test
    void login_emptyToken_false_andNoToken() {
        TestableDirectoryCallsRest rest = new TestableDirectoryCallsRest("https://host", "u", "p");
        DirectoryCredentials.LoginResponse lr = new DirectoryCredentials.LoginResponse();
        lr.username = "u";
        lr.token = "";
        rest.cannedResponse = GSON.toJson(lr);

        assertFalse(rest.login());
        assertNull(rest.directoryCredentials.getToken());
    }

    // ---------- get() ----------

    @Test
    void get_setsAcceptHeader_addsTokenWhenPresent_parsesJson_andBuildsUrl() {
        TestableDirectoryCallsRest rest = new TestableDirectoryCallsRest("https://host/base/", "u", "p");
        rest.setToken("TKN");
        rest.cannedResponse = "{\"a\":1}";

        @SuppressWarnings("unchecked")
        Map<String,Object> out = (Map<String,Object>) rest.get("/x", Map.class);

        assertEquals(1.0, out.get("a")); // Gson → double in raw Map
        assertTrue(rest.lastRequest instanceof HttpGet);
        assertEquals("https://host/base/x", rest.lastRequest.getURI().toString());
        assertHeader(rest.lastRequest, "Accept", "application/json");
        assertHeader(rest.lastRequest, "x-molgenis-token", "TKN");
    }

    @Test
    void get_nullResponse_returnsNull() {
        TestableDirectoryCallsRest rest = new TestableDirectoryCallsRest("https://host/api", "u", "p");
        rest.cannedResponse = null;

        assertNull(rest.get("any", Map.class));
    }

    @Test
    void get_noToken_doesNotSetTokenHeader() {
        TestableDirectoryCallsRest rest = new TestableDirectoryCallsRest("https://host/api", "u", "p");
        rest.cannedResponse = "{}";

        rest.get("x", Map.class);
        assertNull(headerValue(rest.lastRequest, "x-molgenis-token"));
    }

    // ---------- post() ----------

    @Test
    void post_object_and_parse_toType_setsHeaders_andBodyJson() throws Exception {
        TestableDirectoryCallsRest rest = new TestableDirectoryCallsRest("https://h/api", "u", "p");
        rest.setToken("TK1");
        rest.cannedResponse = GSON.toJson("OK");

        String out = (String) rest.post("/do", String.class, Map.of("k","v"));
        assertEquals("OK", out);

        assertTrue(rest.lastRequest instanceof HttpPost);
        assertEquals("https://h/api/do", rest.lastRequest.getURI().toString());
        assertHeader(rest.lastRequest, "Accept", "application/json");
        assertHeader(rest.lastRequest, "Content-type", "application/json");
        assertHeader(rest.lastRequest, "x-molgenis-token", "TK1");

        String body = EntityUtils.toString(((HttpPost) rest.lastRequest).getEntity(), StandardCharsets.UTF_8);
        assertEquals(GSON.toJson(Map.of("k","v")), body);
    }

    @Test
    void post_raw_nullResponse_returnsNull() {
        TestableDirectoryCallsRest rest = new TestableDirectoryCallsRest("https://h/api", "u", "p");
        rest.cannedResponse = null;

        assertNull(rest.post("/x", Map.of("a",1)));
    }

    // ---------- put() ----------

    @Test
    void put_buildsJsonBody_setsHeaders_andReturnsRaw() throws Exception {
        TestableDirectoryCallsRest rest = new TestableDirectoryCallsRest("https://h/base", "u", "p");
        rest.setToken("TK2");
        rest.cannedResponse = "{\"status\":\"saved\"}";

        String resp = rest.put("/save", Map.of("id",7));
        assertEquals("{\"status\":\"saved\"}", resp);

        assertTrue(rest.lastRequest instanceof HttpPut);
        assertHeader(rest.lastRequest, "Content-type", "application/json");
        assertHeader(rest.lastRequest, "x-molgenis-token", "TK2");

        String body = EntityUtils.toString(((HttpPut) rest.lastRequest).getEntity(), StandardCharsets.UTF_8);
        assertEquals(GSON.toJson(Map.of("id",7)), body);
    }

    // ---------- delete() with body ----------

    @Test
    void delete_withBody_usesCustomMethod_setsHeaders_andJsonBody() throws Exception {
        TestableDirectoryCallsRest rest = new TestableDirectoryCallsRest("https://h", "u", "p");
        rest.setToken("TOK");
        rest.cannedResponse = "\"deleted\"";

        String resp = rest.delete("/del", Map.of("x",9));
        assertEquals("\"deleted\"", resp);

        assertEquals("DELETE", ((HttpRequestBase) rest.lastRequest).getMethod());
        assertEquals("https://h/del", rest.lastRequest.getURI().toString());
        assertHeader(rest.lastRequest, "Accept", "application/json");
        assertHeader(rest.lastRequest, "Content-type", "application/json");
        assertHeader(rest.lastRequest, "x-molgenis-token", "TOK");

        String body = EntityUtils.toString(
                ((HttpEntityEnclosingRequestBase) rest.lastRequest).getEntity(), StandardCharsets.UTF_8);
        assertEquals(GSON.toJson(Map.of("x",9)), body);
    }

    // ---------- parent helper urlCombine() smoke ----------

    @Test
    void urlCombine_singleSlashBetweenParts() {
        assertEquals("a/b", DirectoryCalls.urlCombine("a", "b"));
        assertEquals("a/b", DirectoryCalls.urlCombine("a/", "b"));
        assertEquals("a/b", DirectoryCalls.urlCombine("a", "/b"));
        assertEquals("a/b", DirectoryCalls.urlCombine("a/", "/b"));
    }

    // ------------ helpers ------------

    private static void assertHeader(HttpUriRequest req, String name, String expectedValue) {
        assertEquals(expectedValue, headerValue(req, name), "Header " + name + " mismatch");
    }

    private static String headerValue(HttpUriRequest req, String name) {
        Header h = req.getFirstHeader(name);
        return h == null ? null : h.getValue();
    }
}
