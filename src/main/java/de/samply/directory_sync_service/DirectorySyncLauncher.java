package de.samply.directory_sync_service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Pull the configuration for this service from environment variables and trigger the
 * job.
 *
 * The Spring Boot configuration mechanism is used to get the environment variables,
 * which need to follow the pattern laid down in the application.yml file.
 *
 * Two types of job scheduling are possible: one-shot and repeated.
 *
 * If you don't explicitly configure it via the environment variables, the default
 * is one-shot, which means that the job will only run once and the process will
 * then terminate.
 *
 * If you set the timer-cron, then the job will be repeated forever, according to
 * the cron settings. A Quartz job manager will be used for this purpose.
 */
@Service
public class DirectorySyncLauncher {
  private static Logger logger = LogManager.getLogger(DirectorySyncLauncher.class);

  @Autowired
  Configuration configuration;

  public void run() throws Exception {
    DirectorySyncJob directorySyncJob = new DirectorySyncJob();

    if (!directorySyncJob.isExecutable(configuration))
      return;

    String timerCron = configuration.getTimerCron();

    if (timerCron == null || timerCron.isEmpty()) {
      logger.info("Running job just once");
      directorySyncJob.execute(configuration);
      return;
    }

    // Quartz likes to have a " ?" at the end of its cron definition.
    if (!timerCron.endsWith(" ?"))
      timerCron = timerCron + " ?";

    String jobType = directorySyncJob.getJobType();

    logger.info("Running job repeatedly, according to following cron schedule: " + timerCron);
    JobKey quartzJobKey = new JobKey(jobType + "Job", jobType);
    JobDetail quartzJob = JobBuilder.newJob(DirectorySyncJob.class)
            .withIdentity(quartzJobKey).build();

    // Pass configuration to job
    quartzJob.getJobDataMap().put("configuration", configuration);

    Trigger quartzTrigger = TriggerBuilder
            .newTrigger()
            .withIdentity(jobType + "Trigger", jobType)
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
