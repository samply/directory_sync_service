package de.samply.directory_sync_service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Job for starting a synchronization with the Directory.
 *
 * Can handle single-run or repeated run operations.
 */
public class DirectorySyncJob implements Job {
    private static Logger logger = LogManager.getLogger(DirectorySyncLauncher.class);

    /**
     * Method used by Quartz to start a job.
     *
     * @param jobExecutionContext Not used, null value allowed.
     * @throws JobExecutionException
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        // Get parameters
        JobDataMap data = jobExecutionContext.getJobDetail().getJobDataMap();
        Configuration configuration = (Configuration) data.get("configuration");

        execute(configuration);
    }

    /**
     * Method to launch a single run of the job.
     *
     * @param configuration Spring Boot configuration to be used for parameters.
     * @throws JobExecutionException
     */
    public void execute(Configuration configuration) throws JobExecutionException {
        new DirectorySync().syncWithDirectoryFailover(configuration);
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
        if (directoryUserName == null || directoryUserName.isEmpty() || directoryUserPass == null || directoryUserPass.isEmpty()) {
            logger.warn("Directory user name or pass code is empty, will *not* perform Directory sync");
            return false;
        }

        return true;
    }
}
