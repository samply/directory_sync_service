package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirCollectionToDirectoryCollectionPutConverter;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.MergeDirectoryCollectionGetToDirectoryCollectionPut;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.fhir.model.FhirCollection;
import de.samply.directory_sync_service.model.BbmriEricId;
import org.hl7.fhir.r4.model.OperationOutcome;
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
     * @param defaultCollectionId The default collection ID to use for fetching collections from the FHIR store.
     * @return A list of OperationOutcome objects indicating the outcome of the update operation.
     */
    public static boolean sendUpdatesToDirectory(FhirApi fhirApi, DirectoryApi directoryApi, Map<String, String> correctedDiagnoses, String defaultCollectionId) {
        try {
            BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                    .valueOf(defaultCollectionId)
                    .orElse(null);

            List<FhirCollection> fhirCollection = fhirApi.fetchFhirCollections(defaultBbmriEricCollectionId);
            if (fhirCollection == null) {
                logger.warn("Problem getting collections from FHIR store");
                return false;
            }
            logger.info("__________ sendUpdatesToDirectory: FHIR collection count): " + fhirCollection.size());

            DirectoryCollectionPut directoryCollectionPut = FhirCollectionToDirectoryCollectionPutConverter.convert(fhirCollection);
            if (directoryCollectionPut == null) {
                logger.warn("Problem converting FHIR attributes to Directory attributes");
                return false;
            }
            logger.info("__________ sendUpdatesToDirectory: 1 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

            List<String> collectionIds = directoryCollectionPut.getCollectionIds();
            String countryCode = directoryCollectionPut.getCountryCode();
            directoryApi.login();
            DirectoryCollectionGet directoryCollectionGet = directoryApi.fetchCollectionGetOutcomes(countryCode, collectionIds);
            if (directoryCollectionGet == null) {
                logger.warn("Problem getting collections from Directory");
                return false;
            }
            logger.info("__________ sendUpdatesToDirectory: 1 directoryCollectionGet.getItems().size()): " + directoryCollectionGet.getItems().size());

            if (!MergeDirectoryCollectionGetToDirectoryCollectionPut.merge(directoryCollectionGet, directoryCollectionPut)) {
                logger.warn("Problem merging Directory GET attributes to Directory PUT attributes");
                return false;
            }
            logger.info("__________ sendUpdatesToDirectory: 2 directoryCollectionGet.getItems().size()): " + directoryCollectionGet.getItems().size());

            // Apply corrections to ICD 10 diagnoses, to make them compatible with
            // the Directory.
            directoryCollectionPut.applyDiagnosisCorrections(correctedDiagnoses);
            logger.info("__________ sendUpdatesToDirectory: 2 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

            directoryApi.login();
            OperationOutcome updateOutcome = directoryApi.updateEntities(directoryCollectionPut);
            String errorMessage = Util.getErrorMessageFromOperationOutcome(updateOutcome);

            if (!errorMessage.isEmpty()) {
                logger.warn("sendUpdatesToDirectory: Problem during star model update");
                return false;
            }

            return true;
        } catch (Exception e) {
            logger.warn("sendUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
            return false;
        }
    }
}
