package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryApiRest;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.model.BbmriEricId;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Updates the biobanks in the local FHIR store with metadata from the Directory.
 */
public class BiobanksUpdater {
    private static final Logger logger = LoggerFactory.getLogger(BiobanksUpdater.class);
    public static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_NAME = t -> {
        t.fhirBiobank.setName(t.dirBiobank.getName());
        return t;
    };

    /**
     * Updates all biobanks from the FHIR server with information from the Directory.
     *
     * @return the individual {@link OperationOutcome}s from each update
     */
    public static boolean updateBiobanksInFhirStore(FhirApi fhirApi, DirectoryApiRest directoryApiRest) {
        // Retrieve the list of all biobanks
        List<Organization> organizations = fhirApi.listAllBiobanks();

        // Check if the result is a failure or success
        boolean succeeded = true;
        if (organizations == null) {
            logger.warn("error retrieving the biobanks");
            succeeded = false;
        } else {
            // If successful, process each biobank and update it on the FHIR server if necessary
            for (Organization organization : organizations) {
                // Update each biobank and report any errors
                if (!updateBiobankInFhirStore(fhirApi, directoryApiRest, organization)) {
                    logger.warn("updateBiobankOnFhirServerIfNecessary: problem updating: " + organization.getIdElement().getValue());
                    succeeded = false;
                }
            }
        }

        return succeeded;
    }

    /**
     * Takes a biobank from FHIR and updates it with current information from the Directory.
     *
     * @param fhirBiobank the biobank to update.
     * @return the {@link OperationOutcome} from the FHIR server update
     */
    private static boolean updateBiobankInFhirStore(FhirApi fhirApi, DirectoryApiRest directoryApiRest, Organization fhirBiobank) {
        logger.info("updateBiobankOnFhirServerIfNecessary: entered");

        // Retrieve the biobank's BBMRI-ERIC identifier from the FHIR organization
        Optional<BbmriEricId> bbmriEricIdOpt = FhirApi.bbmriEricId(fhirBiobank);

        logger.info("updateBiobankOnFhirServerIfNecessary: bbmriEricIdOpt: " + bbmriEricIdOpt);

        // Check if the identifier is present, if not, return false
        if (!bbmriEricIdOpt.isPresent()) {
            logger.warn("updateBiobankOnFhirServerIfNecessary: Missing BBMRI-ERIC identifier");
            return false;
        }
        BbmriEricId bbmriEricId = bbmriEricIdOpt.get();

        logger.info("updateBiobankOnFhirServerIfNecessary: bbmriEricId: " + bbmriEricId);

        // Fetch the corresponding biobank from the Directory API
        Biobank directoryBiobank = directoryApiRest.fetchBiobank(bbmriEricId);

        logger.info("updateBiobankOnFhirServerIfNecessary: directoryBiobank: " + directoryBiobank);

        // Check if fetching the biobank was successful, if not, return false
        if (directoryBiobank == null) {
            logger.warn("updateBiobankOnFhirServerIfNecessary: Failed to fetch biobank from Directory API");
            return false;
        }

        logger.info("updateBiobankOnFhirServerIfNecessary: Create a BiobankTuple containing the FHIR biobank and the Directory biobank");

        // Create a BiobankTuple containing the FHIR biobank and the Directory biobank
        BiobankTuple biobankTuple = new BiobankTuple(fhirBiobank, directoryBiobank);

        logger.info("updateBiobankOnFhirServerIfNecessary: Update the biobank name if necessary");

        // Update the biobank name if necessary
        BiobankTuple updatedBiobankTuple = UPDATE_BIOBANK_NAME.apply(biobankTuple);

        logger.info("updateBiobankOnFhirServerIfNecessary: Check if any changes have been made; if not, return a no-update necessary outcome");

        // Check if any changes have been made; if not, return true (because this outcome is OK)
        if (!updatedBiobankTuple.hasChanged()) {
            logger.info("updateBiobankOnFhirServerIfNecessary: No update necessary");
            return true;
        }

        logger.info("updateBiobankOnFhirServerIfNecessary: Update the biobank resource on the FHIR server if changes were made");

        // Update the biobank resource on the FHIR server
        OperationOutcome updateOutcome = fhirApi.updateResource(updatedBiobankTuple.fhirBiobank);

        String errorMessage = Util.getErrorMessageFromOperationOutcome(updateOutcome);

        if (!errorMessage.isEmpty()) {
            logger.warn("updateBiobankOnFhirServerIfNecessary: Problem during FHIR store update");
            return false;
        }

        logger.info("updateBiobankOnFhirServerIfNecessary: done!");

        return true;
    }
}
