package de.samply.directory_sync_service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/** Contains the default mappings. */
@Service
public class DirectorySyncLauncher {
  private static Logger logger = LogManager.getLogger(DirectorySyncLauncher.class);
  private static final String jobType = "directorySync"; // The type of the job. This is used primarily in diagnostics.

  @Autowired
  Configuration configuration;

  public void run() throws Exception {
    if (!(new DirectorySyncJob().isExecutable(configuration)))
      return;

    String timerCron = configuration.getTimerCron();

    if (timerCron == null || timerCron.isEmpty()) {
      logger.info("Running job just once");
      try {
        (new DirectorySyncJob()).execute(configuration);
      } catch (JobExecutionException e) {
        e.printStackTrace();
      }
      return;
    }

    // Quartz likes to have a " ?" at the end of its cron definition.
    if (!timerCron.endsWith(" ?"))
      timerCron = timerCron + " ?";

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
