package de.samply.directory_sync_service;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class DirectorySyncService implements CommandLineRunner {
    private static Logger logger = LogManager.getLogger(DirectorySyncService.class);
    private static final String configFilename = "/etc/bridgehead/directory_sync.conf";

    private DirectorySyncLauncher directorySyncLauncher;

    /** Loads the Directory sync launcher. */
    DirectorySyncService(DirectorySyncLauncher directorySyncLauncher) {
        this.directorySyncLauncher = directorySyncLauncher;
    }

    /**
     * Main method, used by Spring to start the Directory sync service.
     *
     * If Directory login credentials (name or password) are missing, no synchronization
     * will be performed.
     *
     * @param args No arguments required.
     */
    public static void main(String[] args) {
        SpringApplication.run(DirectorySyncService.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        directorySyncLauncher.run();
    }
}
