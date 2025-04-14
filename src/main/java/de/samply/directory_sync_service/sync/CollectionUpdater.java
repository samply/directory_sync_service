package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirCollectionToDirectoryCollectionPutConverter;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.DirectoryApiWriteToFile;
import de.samply.directory_sync_service.directory.MergeDirectoryCollectionGetToDirectoryCollectionPut;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.fhir.model.FhirCollection;
import de.samply.directory_sync_service.model.BbmriEricId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Update collections in the Directory with data from the local FHIR store.
 */
public class CollectionUpdater {
    private static final Logger logger = LoggerFactory.getLogger(CollectionUpdater.class);

    /**
     * Take information from the FHIR store and send aggregated updates to the Directory.
     * <p>
     * This is a multi step process:
     *  1. Fetch a list of collections objects from the FHIR store. These contain aggregated
     *     information over all specimens in the collections.
     *  2. Convert the FHIR collection objects into Directory collection PUT DTOs. Copy
     *     over avaialble information from FHIR, converting where necessary.
     *  3. Using the collection IDs found in the FHIR store, send queries to the Directory
     *     and fetch back the relevant GET collections. If any of the collection IDs cannot be
     *     found, this ie a breaking error.
     *  4. Transfer data from the Directory GET collections to the corresponding Directory PUT
     *     collections.
     *  5. Push the new information back to the Directory.
     *
     * @param directoryApi API for communicating with Directory.
     * @param correctedDiagnoses Maps ICD10 codes to corrected ICD10 codes.
     * @param fhirCollections List of FHIR collection objects.
     * @return A list of OperationOutcome objects indicating the outcome of the update operation.
     */
    public static boolean sendUpdatesToDirectory(DirectoryApi directoryApi, Map<String, String> correctedDiagnoses, List<FhirCollection> fhirCollections) {
        try {
            DirectoryCollectionPut directoryCollectionPut = FhirCollectionToDirectoryCollectionPutConverter.convert(fhirCollections);
            if (directoryCollectionPut == null) {
                logger.warn("Problem converting FHIR attributes to Directory attributes");
                return false;
            }
            logger.debug("sendUpdatesToDirectory: 1 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

            List<String> collectionIds = directoryCollectionPut.getCollectionIds();
            String countryCode = directoryCollectionPut.getCountryCode();
            directoryApi.login();
            DirectoryCollectionGet directoryCollectionGet = directoryApi.fetchCollectionGetOutcomes(countryCode, collectionIds);
            if (directoryCollectionGet == null) {
                logger.warn("Problem getting collections from Directory");
                return false;
            }
            logger.debug("sendUpdatesToDirectory: directoryCollectionGet.size(): " + directoryCollectionGet.size());

            // Merge the information relating to the collection that was pulled from the
            // Directory (directoryCollectionGet) with the information pulled from
            // the FHIR store (directoryCollectionPut). Don't do this however if
            // directoryApi is an instance of DirectoryApiWriteToFile, because this
            // class does not actually operate with a real Directory, so the
            // information will not actually be present.
            if (!(directoryApi instanceof DirectoryApiWriteToFile) &&
                    !MergeDirectoryCollectionGetToDirectoryCollectionPut.merge(directoryCollectionGet, directoryCollectionPut)) {
                logger.warn("Problem merging Directory GET attributes to Directory PUT attributes");
                return false;
            }
            logger.debug("sendUpdatesToDirectory: directoryCollectionGet.getItems().size()): " + directoryCollectionGet.getItems().size());

            // Apply corrections to ICD 10 diagnoses, to make them compatible with
            // the Directory.
            directoryCollectionPut.applyDiagnosisCorrections(correctedDiagnoses);
            logger.debug("sendUpdatesToDirectory: directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

            directoryApi.login();

            if (!directoryApi.updateEntities(directoryCollectionPut)) {
                logger.warn("sendUpdatesToDirectory: Problem during collection update");
                return false;
            }

            logger.info("sendUpdatesToDirectory: successfully sent updates to Directory");

            return true;
        } catch (Exception e) {
            logger.warn("sendUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
            return false;
        }
    }
}
