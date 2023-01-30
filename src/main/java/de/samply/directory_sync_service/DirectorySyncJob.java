package de.samply.directory_sync_service;

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
}
