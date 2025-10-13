package de.samply.directory_sync_service.service;

import de.samply.directory_sync_service.sync.Sync;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.StatefulJob;
import org.quartz.DisallowConcurrentExecution;

/**
 * Job for starting a synchronization with the Directory.
 * <p>
 * Can handle single-run or repeated run operations.
 * <p>
 * Will not allow multiple instances of this job to run at the same time.
 */
@DisallowConcurrentExecution
public class DirectorySyncJob implements StatefulJob  {
    private static final Logger logger = LogManager.getLogger(DirectorySyncJob.class);
    private int successCounter = 0;
    private int failureCounter = 0;

    /**
     * Method used by Quartz to start a job.
     *
     * @param jobExecutionContext Not used, null value allowed.
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        // Get parameters
        JobDataMap data = jobExecutionContext.getJobDetail().getJobDataMap();
        Configuration configuration = (Configuration) data.get("configuration");

        logger.info("execute: ------------------ executing as part of a repeated run");
        execute(configuration);
        logger.info("execute: ------------------ finished executing as part of a repeated run\n\n\n\n");
    }

    /**
     * Method to launch a single run of the job.
     *
     * @param configuration Spring Boot configuration to be used for parameters.
     */
    public void execute(Configuration configuration) {
        String retryMax = configuration.getRetryMax();
        String retryInterval = configuration.getRetryInterval();
        String directoryUrl = configuration.getDirectoryUrl();
        String fhirStoreUrl = configuration.getFhirStoreUrl();
        String directoryUserName = configuration.getDirectoryUserName();
        String directoryUserPass = configuration.getDirectoryUserPass();
        String directoryUserToken = configuration.getDirectoryUserToken();
        String directoryDefaultCollectionId = configuration.getDirectoryDefaultCollectionId();
        boolean directoryAllowStarModel = Boolean.parseBoolean(configuration.getDirectoryAllowStarModel());
        int directoryMinDonors = Integer.parseInt(configuration.getDirectoryMinDonors());
        int directoryMaxFacts = Integer.parseInt(configuration.getDirectoryMaxFacts());
        boolean directoryMock = Boolean.parseBoolean(configuration.getDirectoryMock());
        boolean directoryOnlyLogin = Boolean.parseBoolean(configuration.getDirectoryOnlyLogin());
        boolean directoryWriteToFile = Boolean.parseBoolean(configuration.getDirectoryWriteToFile());
        String directoryOutputDirectory = configuration.getDirectoryOutputDirectory();
        boolean importBiobanks = Boolean.parseBoolean(configuration.getImportBiobanks());
        boolean importCollections = Boolean.parseBoolean(configuration.getImportCollections());

        boolean success = Sync.syncWithDirectoryFailover(retryMax, retryInterval, fhirStoreUrl, directoryUrl, directoryUserName, directoryUserPass, directoryUserToken, directoryDefaultCollectionId, directoryAllowStarModel, directoryMinDonors, directoryMaxFacts, directoryMock, directoryOnlyLogin, directoryWriteToFile, directoryOutputDirectory, importBiobanks, importCollections);

        if (success) {
            logger.info("execute: Directory sync succeeded");
            successCounter++;
        } else {
            logger.info("execute: Directory sync failed");
            failureCounter++;
        }
        // Print a warning message once a week if we never have any successful Directory sync runs.
        if (successCounter == 0 && failureCounter >= 7 && failureCounter%7 == 0)
            logger.warn("execute: Directory sync appears to be consistently failing, failureCounter=" + failureCounter);
    }

    /**
     * Test to see if the preconditions for executing a job are available.
     *
     * @param configuration Spring Boot configuration for this service.
     * @return True, if the preconditions are satidfied, false otherwise.
     */
    public boolean isExecutable(Configuration configuration) {
        String directoryUserName = configuration.getDirectoryUserName();
        String directoryUserPass = configuration.getDirectoryUserPass();
        String directoryUserToken = configuration.getDirectoryUserToken();
        boolean noName = directoryUserName == null || directoryUserName.isEmpty();
        boolean noPass = directoryUserPass == null || directoryUserPass.isEmpty();
        boolean noToken = directoryUserToken == null || directoryUserToken.isEmpty();

        if (noToken && (noName || noPass)) {
            logger.warn("Directory user name or pass code is empty, will *not* perform Directory sync");
            return false;
        }

        return true;
    }

    /**
     * Provide the type of the job being run. This is a string, that is essentially arbitrary,
     * but it should have some relevance to the task being executed, because it appears in
     * some of the diagnostics generated by Quartz.
     *
     * @return Job type.
     */
    public String getJobType() {
        return "directorySync";
    }
}
