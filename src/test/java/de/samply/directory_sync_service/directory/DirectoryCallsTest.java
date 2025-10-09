package de.samply.directory_sync_service.directory;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for DirectoryCalls (abstract) via a tiny concrete test subclass.
 */
class DirectoryCallsTest {

    /** Minimal concrete subclass we can instantiate in tests. */
    static class TestableDirectoryCalls extends DirectoryCalls {
        TestableDirectoryCalls(String baseUrl) {
            super(baseUrl, "user", "pass");
        }
        String base() { return baseUrl; }
        String token() { return directoryCredentials.getToken(); }
        /** Allow tests to inject a mock HttpClient despite the final field. */
        void injectHttpClient(CloseableHttpClient client) throws Exception {
            Field f = DirectoryCalls.class.getDeclaredField("httpClient");
            f.setAccessible(true);
            f.set(this, client);
        }
        /** Expose protected method for direct testing. */
        String callExecute(HttpUriRequest req) { return executeRequest(req); }
    }

    @Nested
    @DisplayName("urlCombine")
    class UrlCombine {
        @Test
        @DisplayName("joins with exactly one slash")
        void joinsWithOneSlash() {
            assertEquals("a/b", DirectoryCalls.urlCombine("a", "b"));
            assertEquals("a/b", DirectoryCalls.urlCombine("a/", "b"));
            assertEquals("a/b", DirectoryCalls.urlCombine("a", "/b"));
            assertEquals("a/b", DirectoryCalls.urlCombine("a/", "/b"));
        }
    }

    @Nested
    @DisplayName("Constructor & token")
    class CtorAndToken {
        @Test
        @DisplayName("strips trailing slashes from baseUrl")
        void stripsTrailingSlashes() {
            TestableDirectoryCalls dc = new TestableDirectoryCalls("https://example.org/api///");
            assertEquals("https://example.org/api", dc.base());
        }

        @Test
        @DisplayName("setToken stores token in credentials")
        void setTokenStores() {
            TestableDirectoryCalls dc = new TestableDirectoryCalls("http://x");
            dc.setToken("abc123");
            assertEquals("abc123", dc.token());
        }
    }

    @Nested
    @DisplayName("executeRequest() behavior")
    class ExecuteRequestBehavior {

        private CloseableHttpClient mockClient() {
            return Mockito.mock(CloseableHttpClient.class);
        }
        @Test
        @DisplayName("returns body on 200 with entity")
        void okWithBody() throws Exception {
            TestableDirectoryCalls dc = new TestableDirectoryCalls("http://x");
            CloseableHttpClient client = mockClient();
            when(client.execute(any(HttpUriRequest.class)))
                    .thenReturn(new FakeCloseableHttpResponse(200, "OK", "payload"));
            dc.injectHttpClient(client);

            String res = dc.callExecute(new HttpGet("http://x/ping"));
            assertEquals("payload", res);
        }

        @Test
        @DisplayName("returns null on 204 with null entity")
        void okNoEntity() throws Exception {
            TestableDirectoryCalls dc = new TestableDirectoryCalls("http://x");
            CloseableHttpClient client = mockClient();
            when(client.execute(any(HttpUriRequest.class)))
                    .thenReturn(new FakeCloseableHttpResponse(204, "No Content", null));
            dc.injectHttpClient(client);

            String res = dc.callExecute(new HttpGet("http://x/ping"));
            assertNull(res);
        }

        @Test
        @DisplayName("returns null on 404")
        void notFound() throws Exception {
            TestableDirectoryCalls dc = new TestableDirectoryCalls("http://x");
            CloseableHttpClient client = mockClient();
            when(client.execute(any(HttpUriRequest.class)))
                    .thenReturn(new FakeCloseableHttpResponse(404, "Not Found", null));
            dc.injectHttpClient(client);

            String res = dc.callExecute(new HttpGet("http://x/absent"));
            assertNull(res);
        }

        @Test
        @DisplayName("returns null on 500+")
        void serverError() throws Exception {
            TestableDirectoryCalls dc = new TestableDirectoryCalls("http://x");
            CloseableHttpClient client = mockClient();
            when(client.execute(any(HttpUriRequest.class)))
                    .thenReturn(new FakeCloseableHttpResponse(500, "Server Error", "oops"));
            dc.injectHttpClient(client);

            String res = dc.callExecute(new HttpGet("http://x/fail"));
            assertNull(res);
        }

        @Test
        @DisplayName("returns null on IOException during execute")
        void ioExceptionPath() throws Exception {
            TestableDirectoryCalls dc = new TestableDirectoryCalls("http://x");
            CloseableHttpClient client = mockClient();
            when(client.execute(any(HttpUriRequest.class)))
                    .thenThrow(new java.io.IOException("boom"));
            dc.injectHttpClient(client);

            String res = dc.callExecute(new HttpGet("http://x/ioe"));
            assertNull(res);
        }
    }
}
