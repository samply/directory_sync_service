package de.samply.directory_sync_service.service;

import de.samply.directory_sync_service.sync.Sync;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.fhir.FhirReporting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * This class sets up connections to the FHIR store and to the Directory and
 * them performs a synchronization.
 */
public class DirectorySync {
    private static Logger logger = LogManager.getLogger(DirectorySync.class);

    /**
     * Attempts to perform synchronization with the Directory repeatedly, until it either
     * succeeds, or the number of attempts exceeds a threshold.
     *
     * @throws IOException
     */
    public void syncWithDirectoryFailover(Configuration configuration) {
        String retryMax = configuration.getRetryMax();
        String retryInterval = configuration.getRetryInterval();
        for (int retryNum = 0; retryNum < Integer.parseInt(retryMax); retryNum++) {
            if (retryNum > 0) {
                try {
                    Thread.sleep(Integer.parseInt(retryInterval) * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("syncWithDirectoryFailover: retrying sync, attempt " + retryNum + " of " + retryMax);
            }
            if (syncWithDirectory(configuration))
                break;
        }
    }

    /**
     * Performs the synchronization with the Directory.
     *
     * @return                   Return true if synchronization successful.
     * @throws IOException
     */
    private boolean syncWithDirectory(Configuration configuration) {
        String directoryUrl = configuration.getDirectoryUrl();
        String fhirStoreUrl = configuration.getFhirStoreUrl();
        String directoryUserName = configuration.getDirectoryUserName();
        String directoryUserPass = configuration.getDirectoryUserPass();
        String directoryDefaultCollectionId = configuration.getDirectoryDefaultCollectionId();
        boolean directoryAllowStarModel = Boolean.parseBoolean(configuration.getDirectoryAllowStarModel());
        int directoryMinDonors = Integer.parseInt(configuration.getDirectoryMinDonors());
        int directoryMaxFacts = Integer.parseInt(configuration.getDirectoryMaxFacts());
        boolean directoryMock = Boolean.parseBoolean(configuration.getDirectoryMock());

        Sync sync = new Sync();
        return sync.syncWithDirectory(directoryUrl, fhirStoreUrl, directoryUserName, directoryUserPass, directoryDefaultCollectionId, directoryAllowStarModel, directoryMinDonors, directoryMaxFacts, directoryMock);
    }
}
