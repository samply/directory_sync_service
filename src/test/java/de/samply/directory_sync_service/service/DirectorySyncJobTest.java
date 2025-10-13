package de.samply.directory_sync_service.service;

import de.samply.directory_sync_service.sync.Sync;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for DirectorySyncJob.
 */
class DirectorySyncJobTest {

    // Helper to build a configuration mock with explicit values that will be parsed in execute(Configuration)
    private Configuration configMockWithValues() {
        Configuration cfg = mock(Configuration.class);

        // Strings that will be parsed inside execute(...)
        when(cfg.getRetryMax()).thenReturn("3");
        when(cfg.getRetryInterval()).thenReturn("1000");
        when(cfg.getDirectoryUrl()).thenReturn("https://dir.example");
        when(cfg.getFhirStoreUrl()).thenReturn("https://fhir.example");
        when(cfg.getDirectoryUserName()).thenReturn("user");
        when(cfg.getDirectoryUserPass()).thenReturn("pass");
        when(cfg.getDirectoryUserToken()).thenReturn("token");
        when(cfg.getDirectoryDefaultCollectionId()).thenReturn("bbmri-eric:ID:DE_COLL1");
        when(cfg.getDirectoryAllowStarModel()).thenReturn("true");
        when(cfg.getDirectoryMinDonors()).thenReturn("5");
        when(cfg.getDirectoryMaxFacts()).thenReturn("250");
        when(cfg.getDirectoryMock()).thenReturn("false");
        when(cfg.getDirectoryOnlyLogin()).thenReturn("false");
        when(cfg.getDirectoryWriteToFile()).thenReturn("true");
        when(cfg.getDirectoryOutputDirectory()).thenReturn("/tmp/out");
        when(cfg.getImportBiobanks()).thenReturn("true");
        when(cfg.getImportCollections()).thenReturn("false");

        return cfg;
    }

    @Nested
    @DisplayName("execute(Configuration)")
    class ExecuteSingleRun {

        @Test
        @DisplayName("Calls Sync.syncWithDirectoryFailover with parsed args from Configuration")
        void callsSyncWithParsedArgs() {
            DirectorySyncJob job = new DirectorySyncJob();
            Configuration cfg = configMockWithValues();

            try (MockedStatic<Sync> sync = Mockito.mockStatic(Sync.class)) {
                sync.when(() -> Sync.syncWithDirectoryFailover(
                        anyString(), anyString(),
                        anyString(), anyString(),
                        anyString(), anyString(),
                        anyString(), anyString(),
                        anyBoolean(), anyInt(), anyInt(),
                        anyBoolean(), anyBoolean(),
                        anyBoolean(), anyString(),
                        anyBoolean(), anyBoolean()
                )).thenReturn(true);

                job.execute(cfg);

                // Verify exact argument values and types as parsed inside execute(...)
                sync.verify(() -> Sync.syncWithDirectoryFailover(
                        eq("3"),                       // retryMax (string)
                        eq("1000"),                    // retryInterval (string)
                        eq("https://fhir.example"),
                        eq("https://dir.example"),
                        eq("user"),
                        eq("pass"),
                        eq("token"),
                        eq("bbmri-eric:ID:DE_COLL1"),
                        eq(true),                      // directoryAllowStarModel
                        eq(5),                         // directoryMinDonors
                        eq(250),                       // directoryMaxFacts
                        eq(false),                     // directoryMock
                        eq(false),                     // directoryOnlyLogin
                        eq(true),                      // directoryWriteToFile
                        eq("/tmp/out"),
                        eq(true),                      // importBiobanks
                        eq(false)                      // importCollections
                ));
            }
        }

        @Test
        @DisplayName("Increments failure path without throwing when Sync returns false")
        void handlesFailureWithoutThrow() {
            DirectorySyncJob job = new DirectorySyncJob();
            Configuration cfg = configMockWithValues();

            try (MockedStatic<Sync> sync = Mockito.mockStatic(Sync.class)) {
                sync.when(() -> Sync.syncWithDirectoryFailover(
                        anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(),
                        anyBoolean(), anyInt(), anyInt(),
                        anyBoolean(), anyBoolean(), anyBoolean(), anyString(),
                        anyBoolean(), anyBoolean()
                )).thenReturn(false);

                assertDoesNotThrow(() -> job.execute(cfg));
            }
        }
    }

    @Nested
    @DisplayName("execute(JobExecutionContext)")
    class QuartzEntrypoint {

        @Test
        @DisplayName("Delegates to execute(Configuration) using data from JobDataMap")
        void delegatesToSingleRunExecute() {
            // Spy so we can verify delegation into execute(Configuration)
            DirectorySyncJob jobSpy = spy(new DirectorySyncJob());
            Configuration cfg = configMockWithValues();

            // Build a JobDetail with a JobDataMap carrying our Configuration
            JobDetailImpl detail = new JobDetailImpl();
            detail.setKey(JobKey.jobKey("testJob", "grp"));
            JobDataMap map = new JobDataMap(Map.of("configuration", cfg));
            detail.setJobDataMap(map);

            JobExecutionContext ctx = mock(JobExecutionContext.class);
            when(ctx.getJobDetail()).thenReturn(detail);

            // Stub the single-run to avoid calling Sync
            doNothing().when(jobSpy).execute(cfg);

            jobSpy.execute(ctx);

            verify(jobSpy, times(1)).execute(cfg);
        }
    }

    @Nested
    @DisplayName("Preconditions & metadata")
    class PreconditionsAndMeta {

        @Test
        @DisplayName("isExecutable false when username empty")
        void isExecutable_usernameEmpty() {
            DirectorySyncJob job = new DirectorySyncJob();
            Configuration cfg = mock(Configuration.class);
            when(cfg.getDirectoryUserName()).thenReturn("");
            when(cfg.getDirectoryUserPass()).thenReturn("pass");
            assertFalse(job.isExecutable(cfg));
        }

        @Test
        @DisplayName("isExecutable false when password empty")
        void isExecutable_passwordEmpty() {
            DirectorySyncJob job = new DirectorySyncJob();
            Configuration cfg = mock(Configuration.class);
            when(cfg.getDirectoryUserName()).thenReturn("user");
            when(cfg.getDirectoryUserPass()).thenReturn("");
            assertFalse(job.isExecutable(cfg));
        }

        @Test
        @DisplayName("isExecutable true when both present")
        void isExecutable_bothPresent() {
            DirectorySyncJob job = new DirectorySyncJob();
            Configuration cfg = mock(Configuration.class);
            when(cfg.getDirectoryUserName()).thenReturn("user");
            when(cfg.getDirectoryUserPass()).thenReturn("pass");
            assertTrue(job.isExecutable(cfg));
        }

        @Test
        @DisplayName("getJobType returns directorySync")
        void getJobType_value() {
            assertEquals("directorySync", new DirectorySyncJob().getJobType());
        }

        @Test
        @DisplayName("@DisallowConcurrentExecution is present on class")
        void hasDisallowConcurrentExecutionAnnotation() {
            assertTrue(
                    de.samply.directory_sync_service.service.DirectorySyncJob.class
                            .isAnnotationPresent(DisallowConcurrentExecution.class)
            );
        }
    }

    // Minimal concrete subclass to allow instantiation (no behavior changed)
    private static class DirectorySyncJob extends de.samply.directory_sync_service.service.DirectorySyncJob {
        // Expose a public no-arg constructor for test convenience
        public DirectorySyncJob() { super(); }
        // Make execute(Configuration) visible to spy in tests
        @Override
        public void execute(Configuration configuration) { super.execute(configuration); }
    }
}
