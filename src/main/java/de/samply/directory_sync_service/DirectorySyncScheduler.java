package de.samply.directory_sync_service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * Use the Quartz scheduler to start synchronization at regular intervals. These are specified
 * using cron syntax.
 */
public class DirectorySyncScheduler {
    private static Logger logger = LogManager.getLogger(DirectorySyncScheduler.class);

    /**
     * Initiates the Quartz job running service and passes relevant parameters to
     * Directory sync jobs.
     *
     * @param directoryUrl       Base URL of the Directory
     * @param directoryUserName  User name for logging in to Directory
     * @param directoryPassCode  Password for logging in to Directory
     * @param fhirStoreUrl       URL for Bridgehead Blaze store
     * @param timerCron          Repetition schedule for Quartz job, cron format
     * @param retryMax           Max number of times Directory sync will be retried on failure
     * @param retryInterval      Interval (seconds) between retries
     */
    public void directorySyncStart( String directoryUserName, String directoryPassCode, String directoryUrl,String fhirStoreUrl, String timerCron, String retryMax, String retryInterval)
    {
        JobKey directorySyncJobKey = new JobKey("directorySyncJob", "directorySync");
        JobDetail directorySyncJob = JobBuilder.newJob(DirectorySyncJob.class)
                .withIdentity(directorySyncJobKey).build();

        Trigger directorySyncTrigger = TriggerBuilder
                .newTrigger()
                .withIdentity("directorySyncTrigger", "directorySync")
                .withSchedule(
                        CronScheduleBuilder.cronSchedule(timerCron))
                .build();

        // Pass parameters to job
        directorySyncJob.getJobDataMap().put("directoryUrl",directoryUrl);
        directorySyncJob.getJobDataMap().put("directoryUserName",directoryUserName);
        directorySyncJob.getJobDataMap().put("directoryPassCode",directoryPassCode);
        directorySyncJob.getJobDataMap().put("fhirStoreUrl",fhirStoreUrl);
        directorySyncJob.getJobDataMap().put("retryMax",retryMax);
        directorySyncJob.getJobDataMap().put("retryInterval",retryInterval);

        try {
            Scheduler scheduler = new StdSchedulerFactory().getScheduler();

            scheduler.start();
            scheduler.scheduleJob(directorySyncJob, directorySyncTrigger);
        } catch (SchedulerException e) {
            logger.error(e.toString());
        }
    }
}
