package de.samply.directory_sync_service;

import org.quartz.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.quartz.impl.StdSchedulerFactory;

/**
 * Runs the Directory sync service at a biobank site.
 *
 * This service keeps the BBMRI Directory up to date with the number of samples, etc.
 * kept in the biobank.
 *
 * It uses Quartz to start synchronization at regular intervals. These are specified
 * using cron syntax.
 *
 * You need to provide URL, user and password for the Directory.
 *
 * Additionally, you will need to supply a URL for the FHIR store that supplies the
 * information for the synchronization at the Bridgehead end.
 *
 * These parameters are supplied to the program via a file:
 *
 * /etc/bridgehead/directory_sync.conf
 *
 * The contents of the file should look something like this:
 *
 * directory_sync.directory.url=https://bbmritestnn.gcc.rug.nl
 * directory_sync.directory.user_name=testuser@gmail.com
 * directory_sync.directory.pass_code=KJNJFZTIUZBUZbzubzubzbfdeswsqaq
 * directory_sync.fhir_store_url=http://store:8080
 * directory_sync.timer_cron=
 */
@SpringBootApplication
public class DirectorySyncServiceApplication {
    private static Logger logger = LogManager.getLogger(DirectorySyncServiceApplication.class);
    private String configFilename = "/etc/bridgehead/directory_sync.conf";
    private String directoryUrl;
    private String directoryUserName;
    private String directoryPassCode;
    private String fhirStoreUrl;
    private String timerCron;

    /**
     * Constructor. Loads parameters relating to FHIR store and Directory from
     * a file.
     */
    public DirectorySyncServiceApplication() {
        loadProperties();

        logger.info("DirectorySyncServiceApplication initialized, repeat interval: " + timerCron);
    }

    /**
     * Main method, used by Spring to start the Directory sync service.
     *
     * @param args No arguments required.
     */
    public static void main(String[] args) {
        SpringApplication.run(DirectorySyncServiceApplication.class, args);

        new DirectorySyncServiceApplication().directorySyncStart();
    }

    /**
     * Initiates the Quartz job running service and passes relevant parameters to
     * Directory sync jobs.
     */
    private void directorySyncStart()
    {
        logger.info("Starting Directory sync");

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

        try {
            Scheduler scheduler = new StdSchedulerFactory().getScheduler();

            scheduler.start();
            scheduler.scheduleJob(directorySyncJob, directorySyncTrigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
    }

    /**
     * Pulls the parameters needed by Directory sync from a Java parameter file.
     */
    private void loadProperties() {
        try (InputStream input = new FileInputStream(configFilename)) {
            Properties prop = new Properties();

            prop.load(input);

            // Stash the properties
            directoryUrl = prop.getProperty("directory_sync.directory.url");
            directoryUserName = prop.getProperty("directory_sync.directory.user_name");
            directoryPassCode = prop.getProperty("directory_sync.directory.pass_code");
            fhirStoreUrl = prop.getProperty("directory_sync.fhir_store_url");
            timerCron = prop.getProperty("directory_sync.timer_cron");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
