package de.samply.directory_sync_service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobKey;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * Use the Quartz scheduler to start synchronization of a job at regular intervals. These are specified
 * using cron syntax.
 */
public class JobScheduler {
    private static Logger logger = LogManager.getLogger(JobScheduler.class);
    private String jobName;
    private String jobTrigger;
    private String jobGroup;

    /**
     * Initialize the job scheduler with a somewhat arbitrary string.
     *
     * @param jobType The type of the job, e.g. "directorySync". This is used primarily in diagnostics.
     */
    public JobScheduler(String jobType) {
        jobName = jobType + "Job";
        jobTrigger = jobType + "Trigger";
        jobGroup = jobType;
    }

    /**
     * Initiates the Quartz job running service.
     *
     * You will need to wrap the job you want to run in a class that inherits from
     * org.quartz.Job.
     *
     * Two operational modes are possible, depending on the value of timerCron.
     *
     * If timerCron is not specified (null), then the job will be performed
     * immediately. It will only be carried out once.
     *
     * If timerCron contains a valid cron expression, then the job will be
     * repeated indefinitely, at the times specified in the cron expression.
     *
     * @param timerCron Cron expression, specifying repeat schedule for job. Null for one-off.
     * @param jobClass The class of the Quartz job that you want to run. Needs to inherit from
     *                 org.quartz.Job.
     */
    public void jobStart(String timerCron, Class jobClass)
    {
        if (timerCron == null || timerCron.isEmpty()) {
            logger.info("Running job just once");
            try {
                ((Job) jobClass.getDeclaredConstructor().newInstance()).execute(null);
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (JobExecutionException e) {
                e.printStackTrace();
            }
            return;
        }

        // Quartz likes to have a " ?" at the end of its cron definition.
        if (!timerCron.endsWith(" ?"))
            timerCron = timerCron + " ?";

        logger.info("Running job repeatedly, according to following cron schedule: " + timerCron);
        JobKey quartzJobKey = new JobKey(jobName, jobGroup);
        JobDetail quartzJob = JobBuilder.newJob(jobClass)
                .withIdentity(quartzJobKey).build();

        Trigger quartzTrigger = TriggerBuilder
                .newTrigger()
                .withIdentity(jobTrigger, jobGroup)
                .withSchedule(
                        CronScheduleBuilder.cronSchedule(timerCron))
                .build();

        try {
            Scheduler scheduler = new StdSchedulerFactory().getScheduler();

            scheduler.start();
            scheduler.scheduleJob(quartzJob, quartzTrigger);
        } catch (SchedulerException e) {
            logger.error(e.toString());
        }
    }
}