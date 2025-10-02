package de.samply.directory_sync_service.sync;

import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.r4.model.Organization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Core methods for updating biobanks and collections in the local FHIR store with metadata from the Directory.
 */
public abstract class AbstractUpdater {
    private static final Logger logger = LoggerFactory.getLogger(AbstractUpdater.class);

    /**
     * Ensures the specified email is set on the given contact list under the given purpose.
     *
     * Note: if the contacts list already contains an entry for the purpose, it will be replaced.
     *
     * @param contacts       list of existing contact components
     * @param email          email address to set
     * @param purposeCode    code for the contact purpose (e.g., ADMIN, RESEARCH)
     * @param purposeDisplay human-readable display for the purpose
     */
    protected static void updateEmail(List<Organization.OrganizationContactComponent> contacts, String email, String purposeCode, String purposeDisplay) {
        if (email != null && email.length() > 0) {
            // Find or create the contact component
            Organization.OrganizationContactComponent contact = extractContactByPurpose(contacts, purposeCode);

            // Remove any existing email contact points from the contact
            contact.getTelecom().removeIf(telecom -> telecom.getSystem() == ContactPoint.ContactPointSystem.EMAIL);

            // Set the purpose of the contact
            contact.setPurpose(new CodeableConcept()
                    .addCoding(new Coding().setCode(purposeCode).setDisplay(purposeDisplay)));

            // Add the new email
            contact.addTelecom(new ContactPoint()
                    .setSystem(ContactPoint.ContactPointSystem.EMAIL)
                    .setValue(email));

            // Add the updated contact component to the list
            contacts.add(contact);
        }
    }

    /**
     * Returns a contact component matching the given purpose code if it can find one in the contacts list. Otherwise
     * returns a new contact component if none already exists.
     *
     * Note: if a matching contact component is found, it is removed from the list!
     *
     * @param contacts    list of existing contact components
     * @param purposeCode code of the contact purpose to match
     * @return the existing contact component if found, otherwise a new one
     */
    protected static Organization.OrganizationContactComponent extractContactByPurpose(List<Organization.OrganizationContactComponent> contacts, String purposeCode) {
        for (Organization.OrganizationContactComponent contact : contacts) {
            if (contact.getPurpose().getCoding().get(0).getCode().equals(purposeCode)) {
                contacts.remove(contact);
                return contact;
            }
        }
        return new Organization.OrganizationContactComponent();
    }
}
