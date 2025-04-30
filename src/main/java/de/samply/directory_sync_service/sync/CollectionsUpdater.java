package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.Collection;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Updates the collections in the local FHIR store with metadata from the Directory.
 */
public class CollectionsUpdater extends AbstractUpdater {
    private static final Logger logger = LoggerFactory.getLogger(CollectionsUpdater.class);
    public static final Function<CollectionTuple, CollectionTuple> UPDATE_COLLECTION_NAME = t -> {
        logger.debug("UPDATE_COLLECTION_NAME: set up comparison tuple");
        String name = t.dirCollection.getName();
        if (name == null || name.length() == 0)
            logger.debug("UPDATE_COLLECTION_NAME: no name available from the Directory");
        else
            t.fhirCollection.setName(t.dirCollection.getName());
        return t;
    };
    public static final Function<CollectionTuple, CollectionTuple> UPDATE_COLLECTION_DESCRIPTION = t -> {
        logger.debug("UPDATE_COLLECTION_DESCRIPTION: set up comparison tuple");
        String description = t.dirCollection.getDescription();
        if (description == null || description.length() == 0) {
            logger.debug("UPDATE_COLLECTION_DESCRIPTION: no description available from the Directory");
            return t;
        }
        List<Extension> extensions = t.fhirCollection.getExtension();
        List<Extension> newExtensions = new ArrayList<>();
        // Fill new extension list with all pre-existing extensions that are not descriptions.
        for (Extension extension : extensions)
            if (!extension.getUrl().equals("https://fhir.bbmri.de/StructureDefinition/OrganizationDescription"))
                newExtensions.add(extension);

        // Put description into an extension.
        Extension extension = new Extension();
        extension.setUrl("https://fhir.bbmri.de/StructureDefinition/OrganizationDescription");
        extension.setValue(new StringType(description));

        // Add to new extension list.
        newExtensions.add(extension);

        t.fhirCollection.setExtension(newExtensions);

        return t;
    };
    public static final Function<CollectionTuple, CollectionTuple> UPDATE_COLLECTION_CONTACT = t -> {
        logger.debug("UPDATE_COLLECTION_CONTACT: set up comparison tuple");
        List<Organization.OrganizationContactComponent> contacts = t.fhirCollection.getContact();

        // Add or update administrative contact email
        String administrativeEmail = t.dirCollection.getHead();
        if (administrativeEmail == null || administrativeEmail.length() == 0)
            logger.debug("UPDATE_COLLECTION_CONTACT: no head contact available from the Directory");
        else
            updateEmail(contacts, administrativeEmail, "ADMIN", "Administrative");

        // Add or update research contact email
        String researchEmail = t.dirCollection.getContact();
        if (researchEmail == null || researchEmail.length() == 0)
            logger.debug("UPDATE_COLLECTION_CONTACT: no research contact available from the Directory");
        else
            updateEmail(contacts, researchEmail, "RESEARCH", "Research");

        return t;
    };
    public static final Function<CollectionTuple, CollectionTuple> UPDATE_COLLECTION_ADDRESS = t -> {
        logger.debug("UPDATE_COLLECTION_ADDRESS: set up comparison tuple");
        List<Address> addresses = t.fhirCollection.getAddress();
        Address address = new Address();
        if (addresses.size() > 0)
            address = addresses.get(0);

        boolean dataFound = false;
        String city = t.dirCollection.getLocation();
        if (city == null || city.length() == 0)
            logger.debug("UPDATE_COLLECTION_ADDRESS: no city available from the Directory");
        else {
            address.setCity(city);
            dataFound = true;
        }
        String country = t.dirCollection.getCountry();
        if (country == null || country.length() == 0)
            logger.debug("UPDATE_COLLECTION_ADDRESS: no country available from the Directory");
        else {
            address.setCountry(country);
            dataFound = true;
        }

        if (dataFound) {
            if (addresses.size() > 0)
                addresses = new ArrayList<>();
            addresses.add(address);
        }

        t.fhirCollection.setAddress(addresses);

        return t;
    };
    public static final Function<CollectionTuple, CollectionTuple> UPDATE_COLLECTION_TELECOM = t -> {
        logger.debug("UPDATE_COLLECTION_ADDRESS: set up comparison tuple");
        String url = t.dirCollection.getUrl();
        if (url == null || url.length() == 0)
            logger.debug("UPDATE_COLLECTION_TELECOM: no URL available from the Directory");
        else {
            List<ContactPoint> telecoms = t.fhirCollection.getTelecom();

            // Remove any existing URL entries from the telecoms list
            telecoms.removeIf(contactPoint -> contactPoint.getSystem() == ContactPoint.ContactPointSystem.URL);

            // Add the new URL entry
            telecoms.add(new ContactPoint()
                    .setSystem(ContactPoint.ContactPointSystem.URL)
                    .setValue(url));
        }

        return t;
    };

    /**
     * Updates all collections from the FHIR server with information from the Directory.
     *
     * @return the individual {@link OperationOutcome}s from each update
     */
    public static boolean updateCollectionsInFhirStore(FhirApi fhirApi, de.samply.directory_sync_service.model.Collections collections) {
        List<String> collectionIds = collections.getCollectionIds();

        // Retrieve the list of all collections
        List<Organization> fhirCollections = fhirApi.listAllCollections(collectionIds);

        // Check if the result is a failure or success
        boolean succeeded = true;
        if (fhirCollections == null) {
            logger.warn("updateCollectionsInFhirStore: collections == null");
            return false;
        }
        if (fhirCollections.size() == 0)
            logger.info("updateCollectionsInFhirStore: no matching collections could be found in the FHIR store, collectionIds: " + collectionIds);

        // If successful, process each collection and update it on the FHIR server if necessary
        for (Organization fhirCollection : fhirCollections) {
            // Update each collection and report any errors
            if (!updateCollectionInFhirStore(fhirApi, collections, fhirCollection)) {
                logger.warn("updateCollectionsInFhirStore: problem updating: " + fhirCollection.getIdElement().getValue());
                succeeded = false;
            }
        }

        return succeeded;
    }

    /**
     * Takes a collection from FHIR and updates it with current information from the Directory
     * if necessary.
     *
     * @param fhirCollection the collection to update.
     * @return true if the FHIR server update was successful, false otherwise                      
     */
    private static boolean updateCollectionInFhirStore(FhirApi fhirApi, de.samply.directory_sync_service.model.Collections collections, Organization fhirCollection) {
        // Retrieve the collection's BBMRI-ERIC identifier from the FHIR organization
        Optional<BbmriEricId> bbmriEricIdOpt = FhirApi.bbmriEricId(fhirCollection);

        // Check if the identifier is present, if not, return false
        if (!bbmriEricIdOpt.isPresent()) {
            logger.warn("updateCollectionInFhirStore: Missing BBMRI-ERIC identifier");
            return false;
        }
        BbmriEricId bbmriEricId = bbmriEricIdOpt.get();

        logger.info("updateCollectionInFhirStore: bbmriEricId: " + bbmriEricId);

        // Fetch the corresponding collection from the Directory API
        Collection directoryCollection = collections.getCollection(bbmriEricId.toString());

        logger.info("updateCollectionInFhirStore: directoryCollection: " + directoryCollection);

        // Check if fetching the collection was successful, if not, return false
        if (directoryCollection == null) {
            logger.warn("updateCollectionInFhirStore: Failed to fetch collection from Directory API");
            return false;
        }

        logger.debug("updateCollectionInFhirStore: Create a CollectionTuple containing the FHIR collection and the Directory collection");

        // Create a CollectionTuple containing the FHIR collection and the Directory collection
        // This stashes a copy of the original FHIR collection, to allow comparisons to be
        // made later.
        CollectionTuple collectionTuple = new CollectionTuple(fhirCollection, directoryCollection);

        logger.debug("updateCollectionInFhirStore: Update the collection if necessary");

        collectionTuple = UPDATE_COLLECTION_NAME.apply(collectionTuple);
        collectionTuple = UPDATE_COLLECTION_DESCRIPTION.apply(collectionTuple);
        collectionTuple = UPDATE_COLLECTION_CONTACT.apply(collectionTuple);
        collectionTuple = UPDATE_COLLECTION_ADDRESS.apply(collectionTuple);
        collectionTuple = UPDATE_COLLECTION_TELECOM.apply(collectionTuple);

       // Check if any changes have been made; if not, return true (because this outcome is OK)
        if (collectionTuple.hasChanged())
            logger.debug("updateCollectionInFhirStore: Changes to collection will now be written to FHIR store");
        else {
            logger.info("updateCollectionInFhirStore: No changes were made to the collection, no update necessary");
            return true;
        }

        // Update the collection resource on the FHIR server if there have been changes due to new information from the Directory
        OperationOutcome updateOutcome = fhirApi.updateResource(collectionTuple.fhirCollection);

        String errorMessage = Util.getErrorMessageFromOperationOutcome(updateOutcome);

        if (!errorMessage.isEmpty()) {
            logger.warn("updateCollectionInFhirStore: Problem during FHIR store update, error: " + errorMessage);
            return false;
        }

        logger.debug("updateCollectionInFhirStore: done");

        return true;
    }
}
