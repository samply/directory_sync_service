package de.samply.directory_sync_service;

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
 * It can run Directory sync just once, or use Quartz to start synchronization at regular
 * intervals. These are specified with the cron syntax. If you don't supply any intervals,
 * Directiory sync will only be run once and the service will then terminate.
 *
 * You need to provide URL, user and password for the Directory. If these are not
 * provided, Directory sync will not be performed. You can use this behavior as
 * a switch for turning synchronization on or off.
 *
 * Additionally, you will need to specify a URL for the FHIR store that supplies the
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
public class DirectorySyncService {
    private static Logger logger = LogManager.getLogger(DirectorySyncService.class);
    private String configFilename = "/etc/bridgehead/directory_sync.conf";
    private String directoryUrl;
    private String directoryUserName;
    private String directoryPassCode;
    private String fhirStoreUrl;
    private String timerCron;
    private String retryMax;
    private String retryInterval;

    /**
     * Main method, used by Spring to start the Directory sync service.
     *
     * @param args No arguments required.
     */
    public static void main(String[] args) {
        SpringApplication.run(DirectorySyncService.class, args);

        DirectorySyncService d = new DirectorySyncService();
        d.loadProperties();
        d.directorySyncStart();
    }

    /**
     * Starts Directory synchronization.
     *
     * If Directory login credentials (name or password) are missing, no Synchronization
     * will be performed.
     *
     * Two operational modes are possible, depending on the value of timerCron.
     *
     * If timerCron is not specified (null), then Directory sync will be performed
     * immediately. It will only be carried out once.
     *
     * If timerCron contains a valid cron expression, then Directory sync will be
     * repeated indefinitely, at the times specified in the cron expression.
     */
    private void directorySyncStart() {
        if (directoryUserName == null || directoryUserName.isEmpty() || directoryPassCode == null || directoryPassCode.isEmpty()) {
            logger.info("Directory user name or pass code is empty, will *not* perform Directory sync");
            return;
        }

        if (timerCron == null || timerCron.isEmpty())
            new DirectorySync().syncWithDirectoryFailover(directoryUserName, directoryPassCode, directoryUrl, fhirStoreUrl, retryMax, retryInterval);
        else
            new DirectorySyncScheduler().directorySyncStart(directoryUserName, directoryPassCode, directoryUrl, fhirStoreUrl, timerCron, retryMax, retryInterval);
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
            retryMax = prop.getProperty("directory_sync.retry_max");
            retryInterval = prop.getProperty("directory_sync.retry_interval");

            // Give advanced warning if there are problems with the properties
            if (directoryUrl != null && !new UrlValidator().isValid(directoryUrl))
                logger.warn("Direcory URL is invalid: " + directoryUrl);
            if (fhirStoreUrl != null && !new UrlValidator().isValid(fhirStoreUrl))
                logger.warn("FHIR store URL is invalid: " + fhirStoreUrl);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
