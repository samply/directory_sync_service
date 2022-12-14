package de.samply.directory_sync_service;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.validator.routines.UrlValidator;

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
 * directory_sync.fhir_store_url=http://store:8080/fhir
 * directory_sync.timer_cron=0/20 * * * *
 */
@SpringBootApplication
public class DirectorySyncServiceApplication {
    private static Logger logger = LogManager.getLogger(DirectorySyncServiceApplication.class);
    private String configFilename = "/etc/bridgehead/directory_sync.conf";
    private String directoryUrl = "https://bbmritestnn.gcc.rug.nl"; // default: Directory test site
    private String directoryUserName = null; // User MUST supply this value
    private String directoryPassCode = null; // User MUST supply this value
    private String fhirStoreUrl = "http://store:8989/fhir"; // default: Blaze in Bridgehead
    private String timerCron = "0 22 * * *"; // Default: every evening at 10pm

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

            // Give advanced warning if there are problems with the properties
            if (directoryUrl == null || directoryUrl == "")
                logger.warn("Direcory URL is empty");
            else if (new UrlValidator().isValid(directoryUrl))
                logger.warn("Direcory URL is invalid");
            if (directoryUserName == null || directoryUserName == "")
                logger.warn("Direcory user name is empty");
            if (directoryPassCode == null || directoryPassCode == "")
                logger.warn("Direcory pass code is empty");
            if (fhirStoreUrl == null || fhirStoreUrl == "")
                logger.warn("FHIR store URL is empty");
            else if (new UrlValidator().isValid(fhirStoreUrl))
                logger.warn("FHIR store URL is invalid");
            if (timerCron == null || timerCron == "")
                logger.warn("Cron expression for repeated execution of Directory sync is empty");
            else if (!CronExpression.isValidExpression(timerCron))
                logger.warn("Cron expression for repeated execution of Directory sync is invalid: " + timerCron);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
