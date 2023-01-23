package de.samply.directory_sync_service;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Quartz job for starting a synchronization with the Directory.
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
        new DirectorySync().syncWithDirectoryFailover();
    }
}
