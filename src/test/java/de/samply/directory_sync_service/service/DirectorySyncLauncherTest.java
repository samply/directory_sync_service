package de.samply.directory_sync_service.service;

import org.junit.jupiter.api.*;
import org.mockito.MockedConstruction;
import org.mockito.ArgumentCaptor;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DirectorySyncLauncherTest {

    DirectorySyncLauncher launcher;
    Configuration cfg; // simple bean, we set fields via setters Lombok generated

    @BeforeEach
    void setUp() {
        launcher = new DirectorySyncLauncher();
        cfg = new Configuration();
        // inject Configuration (field has package visibility; keep test in same package)
        launcher.configuration = cfg;
    }

    @Nested
    class OneShotMode {

        @Test
        void runsExecuteOnce_whenNoCron_andDoesNotTouchQuartz() throws Exception {
            cfg.setTimerCron(null); // one-shot

            try (MockedConstruction<DirectorySyncJob> jobCtor = mockConstruction(
                    DirectorySyncJob.class,
                    (mock, context) -> when(mock.isExecutable(cfg)).thenReturn(true)
            );
                 MockedConstruction<StdSchedulerFactory> factoryCtor = mockConstruction(StdSchedulerFactory.class)
            ) {
                launcher.run();

                // verify one-shot execution
                DirectorySyncJob job = jobCtor.constructed().get(0);
                verify(job, times(1)).isExecutable(cfg);
                verify(job, times(1)).execute(cfg);

                // Quartz should not be touched at all: no factories constructed
                var constructed = factoryCtor.constructed();
                // Prefer a clear assertion:
                assertTrue(constructed.isEmpty(), "StdSchedulerFactory should not be constructed in one-shot mode");
                // If you still want to be defensive and verify, guard the call:
                if (!constructed.isEmpty()) {
                    verifyNoInteractions(constructed.toArray());
                }
            }
        }

        @Test
        void doesNothing_whenJobNotExecutable() {
            cfg.setTimerCron(null); // one-shot

            try (MockedConstruction<DirectorySyncJob> jobCtor = mockConstruction(
                    DirectorySyncJob.class,
                    (mock, context) -> when(mock.isExecutable(cfg)).thenReturn(false)
            );
                 MockedConstruction<StdSchedulerFactory> factoryCtor = mockConstruction(StdSchedulerFactory.class)
            ) {
                launcher.run();

                DirectorySyncJob job = jobCtor.constructed().get(0);
                verify(job, times(1)).isExecutable(cfg);
                // Disambiguate overloaded execute(...) if needed:
                verify(job, never()).execute(eq(cfg));

                var constructed = factoryCtor.constructed();
                assertTrue(constructed.isEmpty(), "StdSchedulerFactory should not be constructed when job not executable");
                if (!constructed.isEmpty()) {
                    verifyNoInteractions(constructed.toArray());
                }
            }
        }
    }

    @Nested
    class CronMode {

        @Test
        void schedulesWithCron_andAppendsQuestionMark_ifMissing_andPassesConfigurationToJobData() throws Exception {
            cfg.setTimerCron("0 0/5 * * *"); // no trailing " ?"

            // mock Quartz scheduler
            Scheduler scheduler = mock(Scheduler.class);

            try (MockedConstruction<StdSchedulerFactory> factoryCtor =
                         mockConstruction(StdSchedulerFactory.class,
                                 (factory, ctx) -> when(factory.getScheduler()).thenReturn(scheduler));
                 MockedConstruction<DirectorySyncJob> jobCtor =
                         mockConstruction(DirectorySyncJob.class,
                                 (mock, ctx) -> {
                                     when(mock.isExecutable(cfg)).thenReturn(true);
                                     when(mock.getJobType()).thenReturn("DirSync");
                                 })) {

                launcher.run();

                // We scheduled once
                verify(scheduler, times(1)).start();
                // capture arguments to inspect details
                ArgumentCaptor<JobDetail> jdCap = ArgumentCaptor.forClass(JobDetail.class);
                ArgumentCaptor<Trigger> trCap = ArgumentCaptor.forClass(Trigger.class);
                verify(scheduler).scheduleJob(jdCap.capture(), trCap.capture());

                JobDetail jd = jdCap.getValue();
                Trigger tr = trCap.getValue();

                // Job key matches "{jobType}Job" in group "{jobType}"
                assertEquals(new JobKey("DirSyncJob", "DirSync"), jd.getKey());

                // Configuration passed through JobDataMap
                assertSame(cfg, jd.getJobDataMap().get("configuration"));

                // Cron expression got the trailing " ?"
                assertTrue(tr instanceof CronTrigger);
                assertEquals("0 0 0/5 ? * *", ((CronTrigger) tr).getCronExpression());
            }
        }

        @Test
        void swallowsSchedulerException_withoutCrashing() throws Exception {
            cfg.setTimerCron("0 0/1 * * *"); // cron mode

            try (MockedConstruction<StdSchedulerFactory> factoryCtor =
                         mockConstruction(StdSchedulerFactory.class,
                                 (factory, ctx) -> when(factory.getScheduler()).thenThrow(new SchedulerException("boom")));
                 MockedConstruction<DirectorySyncJob> jobCtor =
                         mockConstruction(DirectorySyncJob.class,
                                 (mock, ctx) -> {
                                     when(mock.isExecutable(cfg)).thenReturn(true);
                                     when(mock.getJobType()).thenReturn("DirSync");
                                 })) {

                assertDoesNotThrow(() -> launcher.run());
                // If exception thrown earlier, this would fail the test.
            }
        }
    }
}
