package de.samply.directory_sync_service.directory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DirectoryCredentialsTest {

    @Nested
    class GenerateLoginCredentials {

        @Test
        @DisplayName("generateLoginCredentials returns non-null inner type bound to outer")
        void returnsInnerTypeAndBindsValues() {
            DirectoryCredentials dc = new DirectoryCredentials("alice", "s3cr3t");
            Object o = dc.generateLoginCredentials();

            assertNotNull(o, "Returned object must not be null");
            assertTrue(o instanceof DirectoryCredentials.LoginCredentials,
                    "Return type should be DirectoryCredentials.LoginCredentials");

            DirectoryCredentials.LoginCredentials creds = (DirectoryCredentials.LoginCredentials) o;
            // Fields are package-private; test is in same package so we can access them.
            assertEquals("alice", creds.username);
            assertEquals("s3cr3t", creds.password);
        }

        @Test
        @DisplayName("Different outer instances yield independent login credentials")
        void independentPerOuterInstance() {
            DirectoryCredentials dc1 = new DirectoryCredentials("alice", "a1");
            DirectoryCredentials dc2 = new DirectoryCredentials("bob", "b2");

            DirectoryCredentials.LoginCredentials c1 =
                    (DirectoryCredentials.LoginCredentials) dc1.generateLoginCredentials();
            DirectoryCredentials.LoginCredentials c2 =
                    (DirectoryCredentials.LoginCredentials) dc2.generateLoginCredentials();

            assertEquals("alice", c1.username);
            assertEquals("a1", c1.password);
            assertEquals("bob", c2.username);
            assertEquals("b2", c2.password);
        }
    }

    @Nested
    class TokenBehavior {

        @Test
        @DisplayName("Token defaults to null and is settable")
        void tokenDefaultNullAndSettable() {
            DirectoryCredentials dc = new DirectoryCredentials("u", "p");
            assertNull(dc.getToken(), "Token should default to null");

            dc.setToken("jwt-123");
            assertEquals("jwt-123", dc.getToken());

            dc.setToken(null);
            assertNull(dc.getToken(), "Token should accept being cleared to null");
        }
    }

    @Nested
    class LoginResponsePojo {

        @Test
        @DisplayName("LoginResponse is a mutable DTO with public fields")
        void loginResponseFields() {
            DirectoryCredentials.LoginResponse lr = new DirectoryCredentials.LoginResponse();
            assertNull(lr.username);
            assertNull(lr.token);

            lr.username = "carol";
            lr.token = "tok-456";

            assertEquals("carol", lr.username);
            assertEquals("tok-456", lr.token);
        }
    }

    @Test
    @DisplayName("generateLoginCredentials returns the expected concrete class despite Object signature")
    void objectReturnTypeIsExpected() {
        DirectoryCredentials dc = new DirectoryCredentials("x", "y");
        Object any = dc.generateLoginCredentials();
        assertInstanceOf(DirectoryCredentials.LoginCredentials.class, any);
    }
}
