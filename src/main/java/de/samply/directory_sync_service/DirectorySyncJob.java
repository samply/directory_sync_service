package de.samply.directory_sync_service;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import de.samply.directory_sync.Sync;
import de.samply.directory_sync.directory.DirectoryApi;
import de.samply.directory_sync.directory.DirectoryService;
import de.samply.directory_sync.fhir.FhirApi;
import de.samply.directory_sync.fhir.FhirReporting;
import io.vavr.control.Either;
import java.io.IOException;
import java.util.List;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.OperationOutcome;

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
     *
     * @param jobExecutionContext
     * @throws JobExecutionException
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        logger.info("DirectorySyncJob.execute: starting job");

        // Get parameters
        JobDataMap data = jobExecutionContext.getJobDetail().getJobDataMap();
        String directoryUrl = data.getString("directoryUrl");
        String directoryUserName = data.getString("directoryUserName");
        String directoryPassCode = data.getString("directoryPassCode");
        String fhirStoreUrl = data.getString("fhirStoreUrl");

        try {
            syncWithDirectory(directoryUserName, directoryPassCode, directoryUrl, fhirStoreUrl);
        } catch (Exception e) {
            logger.error(e.toString());
            throw new JobExecutionException(e);
        }
    }

    /**
     * Performs the synchronization with the Directory.
     *
     * This method was pulled from samply.share.client.
     *
     * @param directoryUserName  User name for logging in to Directory
     * @param directoryPassCode  Password for logging in to Directory
     * @param directoryUrl       Base URL of the Directory
     * @param fhirStoreUrl       URL for Bridgehead Blaze store
     * @throws IOException
     */
    private void syncWithDirectory(String directoryUserName, String directoryPassCode, String directoryUrl, String fhirStoreUrl) throws IOException {
        DirectoryApi directoryApi = createDirectoryApi(directoryUserName, directoryPassCode, directoryUrl).get();
        DirectoryService directoryService = new DirectoryService(directoryApi);
        FhirApi fhirApi = createFhirApi(fhirStoreUrl);
        FhirReporting fhirReporting = new FhirReporting(ctx, fhirApi);
        Sync sync = new Sync(fhirApi, fhirReporting, directoryApi, directoryService);
        List<OperationOutcome> operationOutcomes = sync.syncCollectionSizesToDirectory();
        for (OperationOutcome oo : operationOutcomes) {
            logger.info("syncCollectionSizesToDirectory outcome: " + ctx.newJsonParser().encodeResourceToString(oo));
        }
        List<OperationOutcome> oo = sync.updateAllBiobanksOnFhirServerIfNecessary();
        for (OperationOutcome operationOutcomeTmp : oo) {
            logger.info("updateAllBiobanksOnFhirServerIfNecessary outcome: " + ctx.newJsonParser().encodeResourceToString(operationOutcomeTmp));
        }
    }

    /**
     * Opens a connection to the Directory API.
     *
     * This is where the login to the Directory happens.
     *
     * @param directoryUserName  User name for logging in to Directory
     * @param directoryPassCode  Password for logging in to Directory
     * @param directoryUrl       Base URL of the Directory
     * @return
     * @throws IOException
     */
    private Either<OperationOutcome, DirectoryApi> createDirectoryApi(String directoryUserName, String directoryPassCode, String directoryUrl)
            throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        return DirectoryApi.createWithLogin(client, directoryUrl, directoryUserName, directoryPassCode);
    }

    /**
     * Create a connection to the FHIR store (e.g. Blaze).
     *
     * @param fhirStoreUrl       URL for Bridgehead Blaze store
     * @return
     */
    private FhirApi createFhirApi(String fhirStoreUrl) {
        IGenericClient client = ctx.newRestfulGenericClient(fhirStoreUrl);
        client.registerInterceptor(new LoggingInterceptor(true));
        return new FhirApi(client);
    }
}
