package de.samply.directory_sync_service.service;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DirectorySyncService.
 */
class DirectorySyncServiceTest {

    @Test
    void run_delegatesToLauncher() {
        DirectorySyncLauncher launcher = mock(DirectorySyncLauncher.class);
        DirectorySyncService svc = new DirectorySyncService(launcher);

        svc.run("arg1", "arg2");

        verify(launcher, times(1)).run();
        verifyNoMoreInteractions(launcher);
    }

    @Test
    void main_invokesSpringApplicationRun_withClassAndArgs() {
        String[] args = {"--foo=bar"};

        try (MockedStatic<SpringApplication> app = Mockito.mockStatic(SpringApplication.class)) {
            // We don't care about the returned context; just ensure the call is made.
            app.when(() -> SpringApplication.run(DirectorySyncService.class, args)).thenReturn(null);

            DirectorySyncService.main(args);

            app.verify(() -> SpringApplication.run(DirectorySyncService.class, args), times(1));
        }
    }

    @Test
    void main_handlesEmptyArgs() {
        String[] args = new String[0];

        try (MockedStatic<SpringApplication> app = Mockito.mockStatic(SpringApplication.class)) {
            app.when(() -> SpringApplication.run(DirectorySyncService.class, args)).thenReturn(null);

            assertDoesNotThrow(() -> DirectorySyncService.main(args));
            app.verify(() -> SpringApplication.run(DirectorySyncService.class, args), times(1));
        }
    }

    @Test
    void class_isAnnotatedWithSpringBootApplication() {
        SpringBootApplication ann = DirectorySyncService.class.getAnnotation(SpringBootApplication.class);
        assertNotNull(ann, "DirectorySyncService should be annotated with @SpringBootApplication");
    }
}
