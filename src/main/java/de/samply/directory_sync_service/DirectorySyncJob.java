package de.samply.directory_sync_service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import ca.uhn.fhir.context.FhirContext;

/**
 * Quartz job for starting a synchronization with the Directory.
 */
public class DirectorySyncJob implements Job {
    private static Logger logger = LogManager.getLogger(DirectorySyncJob.class);
    private final FhirContext ctx = FhirContext.forR4();

    /**
     * Method used by Quartz to start a job.
     *
     * The job execution context is used to transfer parameters to the job.
     * The following parameters need to be set:
     *
     * * directoryUrl       Base URL of the Directory
     * * directoryUserName  User name for logging in to Directory
     * * directoryPassCode  Password for logging in to Directory
     * * fhirStoreUrl       URL for Bridgehead Blaze store
     * * retryMax           Max number of times Directory sync will be retried on failure
     * * retryInterval      Interval (seconds) between retries
     *
     * @param jobExecutionContext
     * @throws JobExecutionException
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        // Get parameters
        JobDataMap data = jobExecutionContext.getJobDetail().getJobDataMap();
        String directoryUrl = data.getString("directoryUrl");
        String directoryUserName = data.getString("directoryUserName");
        String directoryPassCode = data.getString("directoryPassCode");
        String fhirStoreUrl = data.getString("fhirStoreUrl");
        String retryMax = data.getString("retryMax");
        String retryInterval = data.getString("retryInterval");

        new DirectorySync().syncWithDirectoryFailover(directoryUserName, directoryPassCode, directoryUrl, fhirStoreUrl, retryMax, retryInterval);
    }
}
