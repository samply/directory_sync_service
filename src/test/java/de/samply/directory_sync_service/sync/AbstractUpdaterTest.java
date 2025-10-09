package de.samply.directory_sync_service.sync;

import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.r4.model.Organization;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AbstractUpdaterTest {

    // ---------- helpers ----------
    private static Organization.OrganizationContactComponent contactWithPurpose(
            String code, String display, ContactPoint... telecoms) {
        var cc = new Organization.OrganizationContactComponent();
        cc.setPurpose(new CodeableConcept().addCoding(new Coding().setCode(code).setDisplay(display)));
        for (var t : telecoms) {
            cc.addTelecom(t);
        }
        return cc;
    }

    private static ContactPoint email(String value) {
        return new ContactPoint()
                .setSystem(ContactPoint.ContactPointSystem.EMAIL)
                .setValue(value);
    }

    private static ContactPoint phone(String value) {
        return new ContactPoint()
                .setSystem(ContactPoint.ContactPointSystem.PHONE)
                .setValue(value);
    }

    // ---------- updateEmail ----------

    @Test
    void updateEmail_addsNewContact_whenListEmpty() {
        List<Organization.OrganizationContactComponent> contacts = new ArrayList<>();

        AbstractUpdater.updateEmail(contacts, "alice@example.org", "ADMIN", "Administrator");

        assertEquals(1, contacts.size(), "one contact should be added");
        var c = contacts.get(0);
        assertEquals("ADMIN", c.getPurpose().getCodingFirstRep().getCode());
        assertEquals("Administrator", c.getPurpose().getCodingFirstRep().getDisplay());
        assertEquals(1, c.getTelecom().size());
        assertEquals(ContactPoint.ContactPointSystem.EMAIL, c.getTelecomFirstRep().getSystem());
        assertEquals("alice@example.org", c.getTelecomFirstRep().getValue());
    }

    @Test
    void updateEmail_replacesExistingEmail_forSamePurpose_and_preservesOtherTelecoms() {
        // existing contact w/ same purpose, with old EMAIL + a PHONE
        var existing = contactWithPurpose("RESEARCH", "Research",
                email("old@example.org"), phone("+49-111-222"));
        List<Organization.OrganizationContactComponent> contacts = new ArrayList<>(List.of(existing));

        AbstractUpdater.updateEmail(contacts, "new@example.org", "RESEARCH", "Research");

        // Net size should remain 1 (removed then re-added)
        assertEquals(1, contacts.size());
        var c = contacts.get(0);

        // purpose is (re)set
        assertEquals("RESEARCH", c.getPurpose().getCodingFirstRep().getCode());
        assertEquals("Research", c.getPurpose().getCodingFirstRep().getDisplay());

        // email replaced, phone preserved
        assertTrue(c.getTelecom().stream().anyMatch(tp ->
                tp.getSystem() == ContactPoint.ContactPointSystem.EMAIL &&
                        "new@example.org".equals(tp.getValue())));
        assertTrue(c.getTelecom().stream().anyMatch(tp ->
                tp.getSystem() == ContactPoint.ContactPointSystem.PHONE &&
                        "+49-111-222".equals(tp.getValue())));
        // no old email
        assertFalse(c.getTelecom().stream().anyMatch(tp ->
                tp.getSystem() == ContactPoint.ContactPointSystem.EMAIL &&
                        "old@example.org".equals(tp.getValue())));
    }

    @Test
    void updateEmail_doesNothing_whenEmailNull() {
        List<Organization.OrganizationContactComponent> contacts = new ArrayList<>();
        AbstractUpdater.updateEmail(contacts, null, "ADMIN", "Administrator");
        assertTrue(contacts.isEmpty());
    }

    @Test
    void updateEmail_doesNothing_whenEmailEmpty() {
        List<Organization.OrganizationContactComponent> contacts = new ArrayList<>();
        AbstractUpdater.updateEmail(contacts, "", "ADMIN", "Administrator");
        assertTrue(contacts.isEmpty());
    }

    // ---------- extractContactByPurpose ----------

    @Test
    void extractContactByPurpose_returnsExistingAndRemovesIt() {
        var admin = contactWithPurpose("ADMIN", "Administrator", email("a@x"));
        var research = contactWithPurpose("RESEARCH", "Research", email("r@x"));
        List<Organization.OrganizationContactComponent> contacts = new ArrayList<>(List.of(admin, research));

        var found = AbstractUpdater.extractContactByPurpose(contacts, "RESEARCH");

        assertSame(research, found, "should return the matching instance");
        assertEquals(1, contacts.size(), "matching contact should be removed from the list");
        assertSame(admin, contacts.get(0));
    }

    @Test
    void extractContactByPurpose_returnsNew_whenNoMatch() {
        var admin = contactWithPurpose("ADMIN", "Administrator", email("a@x"));
        List<Organization.OrganizationContactComponent> contacts = new ArrayList<>(List.of(admin));

        var found = AbstractUpdater.extractContactByPurpose(contacts, "RESEARCH");

        assertNotNull(found);
        assertNotSame(admin, found);
        assertEquals(1, contacts.size(), "list should be unchanged when no match");
        assertSame(admin, contacts.get(0));
        // new instance has no purpose yet
        assertFalse(found.hasPurpose());
    }

    @Test
    void extractContactByPurpose_removesFirstMatch_whenMultipleExist() {
        var first = contactWithPurpose("ADMIN", "Administrator", email("first@x"));
        var second = contactWithPurpose("ADMIN", "Administrator", email("second@x"));
        List<Organization.OrganizationContactComponent> contacts = new ArrayList<>(List.of(first, second));

        var found = AbstractUpdater.extractContactByPurpose(contacts, "ADMIN");

        assertSame(first, found, "should return the first matching contact");
        assertEquals(1, contacts.size(), "one admin remains in list");
        assertSame(second, contacts.get(0));
    }
}
