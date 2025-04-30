package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.model.BbmriEricId;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Updates the biobanks in the local FHIR store with metadata from the Directory.
 */
public class BiobanksUpdater extends AbstractUpdater {
    private static final Logger logger = LoggerFactory.getLogger(BiobanksUpdater.class);
    public static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_NAME = t -> {
        logger.debug("UPDATE_BIOBANK_NAME: set up comparison tuple");
        String name = t.dirBiobank.getName();
        if (name == null || name.length() == 0)
            logger.debug("UPDATE_BIOBANK_NAME: no name available from the Directory");
        else
            t.fhirBiobank.setName(t.dirBiobank.getName());
        return t;
    };
    public static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_DESCRIPTION = t -> {
        logger.debug("UPDATE_BIOBANK_DESCRIPTION: set up comparison tuple");
        String description = t.dirBiobank.getDescription();
        if (description == null || description.length() == 0) {
            logger.debug("UPDATE_BIOBANK_DESCRIPTION: no description available from the Directory");
            return t;
        }
        List<Extension> extensions = t.fhirBiobank.getExtension();
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

        t.fhirBiobank.setExtension(newExtensions);

        return t;
    };
    public static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_JUDICIAL_PERSON = t -> {
        logger.debug("UPDATE_BIOBANK_JUDICIAL_PERSON: set up comparison tuple");
        String juridicialPerson = t.dirBiobank.getJuridicalPerson();
        if (juridicialPerson == null || juridicialPerson.length() == 0) {
            logger.debug("UPDATE_BIOBANK_JUDICIAL_PERSON: no juridicial person available from the Directory");
            return t;
        }
        List<Extension> extensions = t.fhirBiobank.getExtension();
        List<Extension> newExtensions = new ArrayList<>();
        // Fill new extension list with all pre-existing extensions that are not judicial persons.
        for (Extension extension : extensions)
            if (!extension.getUrl().equals("https://fhir.bbmri.de/StructureDefinition/JuridicalPerson"))
                newExtensions.add(extension);

        // Put juridicial person into an extension.
        Extension extension = new Extension();
        extension.setUrl("https://fhir.bbmri.de/StructureDefinition/JuridicalPerson");
        extension.setValue(new StringType(juridicialPerson));

        // Add to new extension list.
        newExtensions.add(extension);

        t.fhirBiobank.setExtension(newExtensions);

        return t;
    };
    public static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_ALIAS = t -> {
        logger.debug("UPDATE_BIOBANK_ALIAS: set up comparison tuple");
        String acronym = t.dirBiobank.getAcronym();
        if (acronym == null || acronym.length() == 0)
            logger.debug("UPDATE_BIOBANK_ALIAS: no acronym available from the Directory");
        else
            t.fhirBiobank.setAlias(Collections.singletonList(new StringType(acronym)));

        return t;
    };
    public static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_CAPABILITIES = t -> {
        logger.debug("UPDATE_BIOBANK_CAPABILITIES: set up comparison tuple");
        List<Map> biobankCapabilities = t.dirBiobank.getCapabilities();
        if (biobankCapabilities == null || biobankCapabilities.size() == 0) {
            logger.debug("UPDATE_BIOBANK_CAPABILITIES: no capabilities available from the Directory");
            return t;
        }
        List<Extension> extensions = t.fhirBiobank.getExtension();
        List<Extension> newExtensions = new ArrayList<>();
        // Fill new extension list with all pre-existing extensions that are not capabilities.
        for (Extension extension : extensions)
            if (!extension.getUrl().equals("https://fhir.bbmri.de/StructureDefinition/Capabilities"))
                newExtensions.add(extension);

        // Add new capabilities from Directory to Extension list.
        for (Map<String,String> biobankCapability : biobankCapabilities) {
            CodeableConcept capabilityConcept = new CodeableConcept();
            String id = biobankCapability.get("id");
            String label = biobankCapability.get("label");
            // Add capability to the CodeableConcept
            capabilityConcept.addCoding(new Coding()
                    .setSystem("https://samply.github.io/bbmri-fhir-ig/ValueSet-BiobankCapabilities.html") // Use the terminology system from the ValueSet
                    .setCode(id) // TODO: find out if this is really the code or actually the display value
                    .setDisplay(label));
            // Put codeable concept into an extension.
            Extension extension = new Extension();
            extension.setUrl("https://fhir.bbmri.de/StructureDefinition/Capabilities");
            extension.setValue(capabilityConcept);
            // Add extension to extension list.
            newExtensions.add(extension);
        }

        // Replace pre-existing extensions with new extensions.
        t.fhirBiobank.setExtension(newExtensions);

        return t;
    };
    public static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_CONTACT = t -> {
        logger.debug("UPDATE_BIOBANK_CONTACT: set up comparison tuple");
        List<Organization.OrganizationContactComponent> contacts = t.fhirBiobank.getContact();

        // Add or update administrative contact email
        Map head = t.dirBiobank.getHead();
        if (head == null || head.size() == 0)
            logger.debug("UPDATE_BIOBANK_CONTACT: no head contact available from the Directory");
        else {
            String administrativeEmail = (String) head.get("email");
            if (administrativeEmail == null || administrativeEmail.length() == 0)
                logger.debug("UPDATE_BIOBANK_CONTACT: no administrativeEmail available from the Directory");
            else
                updateEmail(contacts, administrativeEmail, "ADMIN", "Administrative");
        }

        // Add or update research contact email
        Map research = t.dirBiobank.getContact();
        if (research == null || research.size() == 0)
            logger.debug("UPDATE_BIOBANK_CONTACT: no research contact available from the Directory");
        else {
            String researchEmail = (String) research.get("email");
            if (researchEmail == null || researchEmail.length() == 0)
                logger.debug("UPDATE_BIOBANK_CONTACT: no researchEmail available from the Directory");
            else
                updateEmail(contacts, researchEmail, "RESEARCH", "Research");
        }

        return t;
    };
    public static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_ADDRESS = t -> {
        logger.debug("UPDATE_BIOBANK_ADDRESS: set up comparison tuple");
        List<Address> addresses = t.fhirBiobank.getAddress();
        Address address = new Address();
        if (addresses.size() > 0)
            address = addresses.get(0);

        boolean dataFound = false;
        String city = t.dirBiobank.getLocation();
        if (city == null || city.length() == 0)
            logger.debug("UPDATE_BIOBANK_ADDRESS: no city available from the Directory");
        else {
            address.setCity(city);
            dataFound = true;
        }
        Map country = t.dirBiobank.getCountry();
        if (country == null || country.size() == 0)
            logger.debug("UPDATE_BIOBANK_ADDRESS: no country available from the Directory");
        else {
            String countryId = (String) country.get("id");
            if (countryId == null || countryId.length() == 0)
                logger.debug("UPDATE_BIOBANK_ADDRESS: no country ID available from the Directory");
            else {
                address.setCountry(countryId);
                dataFound = true;
            }
        }

        if (dataFound) {
            if (addresses.size() > 0)
                addresses = new ArrayList<>();
            addresses.add(address);
        }

        t.fhirBiobank.setAddress(addresses);

        return t;
    };
    public static final Function<BiobankTuple, BiobankTuple> UPDATE_BIOBANK_TELECOM = t -> {
        logger.debug("UPDATE_BIOBANK_ADDRESS: set up comparison tuple");
        String url = t.dirBiobank.getUrl();
        if (url == null || url.length() == 0)
            logger.debug("UPDATE_BIOBANK_TELECOM: no URL available from the Directory");
        else {
            List<ContactPoint> telecoms = t.fhirBiobank.getTelecom();

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
     * Updates all biobanks from the FHIR server with information from the Directory.
     *
     * @return the individual {@link OperationOutcome}s from each update
     */
    public static boolean updateBiobanksInFhirStore(FhirApi fhirApi, DirectoryApi directoryApi) {
        // Retrieve the list of all biobanks
        List<Organization> biobanks = fhirApi.listAllBiobanks();

        // Check if the result is a failure or success
        boolean succeeded = true;
        if (biobanks == null) {
            logger.warn("updateBiobanksInFhirStore: organizations == null");
            return false;
        }

        // If successful, process each biobank and update it on the FHIR server if necessary
        for (Organization biobank : biobanks) {
            // Update each biobank and report any errors
            if (!updateBiobankInFhirStore(fhirApi, directoryApi, biobank)) {
                logger.warn("updateBiobankOnFhirServerIfNecessary: problem updating: " + biobank.getIdElement().getValue());
                succeeded = false;
            }
        }

        return succeeded;
    }

    /**
     * Takes a biobank from FHIR and updates it with current information from the Directory
     * if necessary.
     *
     * @param fhirBiobank the biobank to update.
     * @return true if the FHIR server update was successful, false otherwise
     */
    private static boolean updateBiobankInFhirStore(FhirApi fhirApi, DirectoryApi directoryApi, Organization fhirBiobank) {
        // Retrieve the biobank's BBMRI-ERIC identifier from the FHIR organization
        Optional<BbmriEricId> bbmriEricIdOpt = FhirApi.bbmriEricId(fhirBiobank);

        // Check if the identifier is present, if not, return false
        if (!bbmriEricIdOpt.isPresent()) {
            logger.warn("updateBiobankOnFhirServerIfNecessary: Missing BBMRI-ERIC identifier");
            return false;
        }
        BbmriEricId bbmriEricId = bbmriEricIdOpt.get();

        logger.info("updateBiobankOnFhirServerIfNecessary: bbmriEricId: " + bbmriEricId);

        // Fetch the corresponding biobank from the Directory API
        Biobank directoryBiobank = directoryApi.fetchBiobank(bbmriEricId);

        logger.info("updateBiobankOnFhirServerIfNecessary: directoryBiobank: " + directoryBiobank);

        // Check if fetching the biobank was successful, if not, return false
        if (directoryBiobank == null) {
            logger.warn("updateBiobankOnFhirServerIfNecessary: Failed to fetch biobank from Directory API");
            return false;
        }

        logger.debug("updateBiobankOnFhirServerIfNecessary: Create a BiobankTuple containing the FHIR biobank and the Directory biobank");

        // Create a BiobankTuple containing the FHIR biobank and the Directory biobank
        // This stashes a copy of the original FHIR biobank, to allow comparisons to be
        // made later.
        BiobankTuple biobankTuple = new BiobankTuple(fhirBiobank, directoryBiobank);

        logger.debug("updateBiobankOnFhirServerIfNecessary: Update the biobank if necessary");

        biobankTuple = UPDATE_BIOBANK_NAME.apply(biobankTuple);
        biobankTuple = UPDATE_BIOBANK_DESCRIPTION.apply(biobankTuple);
        biobankTuple = UPDATE_BIOBANK_JUDICIAL_PERSON.apply(biobankTuple);
        biobankTuple = UPDATE_BIOBANK_ALIAS.apply(biobankTuple);
        biobankTuple = UPDATE_BIOBANK_CAPABILITIES.apply(biobankTuple);
        biobankTuple = UPDATE_BIOBANK_CONTACT.apply(biobankTuple);
        biobankTuple = UPDATE_BIOBANK_ADDRESS.apply(biobankTuple);
        biobankTuple = UPDATE_BIOBANK_TELECOM.apply(biobankTuple);

       // Check if any changes have been made; if not, return true (because this outcome is OK)
        if (biobankTuple.hasChanged())
            logger.debug("updateBiobankOnFhirServerIfNecessary: Changes to biobank will now be written to FHIR store");
        else {
            logger.info("updateBiobankOnFhirServerIfNecessary: No changes were made to the biobank, no update necessary");
            return true;
        }

        // Update the biobank resource on the FHIR server if there have been changes due to new information from the Directory
        OperationOutcome updateOutcome = fhirApi.updateResource(biobankTuple.fhirBiobank);

        String errorMessage = Util.getErrorMessageFromOperationOutcome(updateOutcome);

        if (!errorMessage.isEmpty()) {
            logger.warn("updateBiobankOnFhirServerIfNecessary: Problem during FHIR store update, error: " + errorMessage);
            return false;
        }

        logger.debug("updateBiobankOnFhirServerIfNecessary: done");

        return true;
    }
}
