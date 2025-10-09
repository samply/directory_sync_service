package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.directory.model.Biobank;
import org.hl7.fhir.r4.model.*;
import org.junit.jupiter.api.Test;

import java.util.*;

import static de.samply.directory_sync_service.sync.BiobanksUpdater.*;
import static org.junit.jupiter.api.Assertions.*;

class BiobanksUpdaterTest {

    // --- helpers -------------------------------------------------------------

    private static Biobank dirBiobank() {
        return new Biobank();
    }

    private static Organization fhirOrg() {
        return new Organization();
    }

    private static BiobankTuple tuple(Organization org, Biobank dir) {
        // BiobankTuple is constructed in the production code; assuming it’s visible
        return new BiobankTuple(org, dir);
    }

    private static Map<String,String> mapOf(String k, String v) {
        Map<String,String> m = new HashMap<>();
        m.put(k, v);
        return m;
    }

    // --- UPDATE_BIOBANK_NAME -------------------------------------------------

    @Test
    void updateName_setsWhenPresent_andSkipsWhenEmpty() {
        var org = fhirOrg();
        var dir = dirBiobank();

        // empty -> no change
        dir.setName("");
        var t1 = UPDATE_BIOBANK_NAME.apply(tuple(org, dir));
        assertFalse(org.hasName());

        // non-empty -> set
        dir.setName("Biobank Alpha");
        var t2 = UPDATE_BIOBANK_NAME.apply(tuple(org, dir));
        assertEquals("Biobank Alpha", org.getName());
    }

    // --- UPDATE_BIOBANK_DESCRIPTION -----------------------------------------

    @Test
    void updateDescription_replacesOnlyThatExtension() {
        var org = fhirOrg();
        // seed org with an unrelated extension and a stale description extension
        org.addExtension(new Extension("https://example.org/else", new StringType("keep-me")));
        org.addExtension(new Extension("https://fhir.bbmri.de/StructureDefinition/OrganizationDescription",
                new StringType("OLD")));

        var dir = dirBiobank();
        dir.setDescription("New detailed description");

        UPDATE_BIOBANK_DESCRIPTION.apply(tuple(org, dir));

        // description extension present & updated
        var desc = org.getExtension().stream()
                .filter(e -> "https://fhir.bbmri.de/StructureDefinition/OrganizationDescription".equals(e.getUrl()))
                .findFirst().orElseThrow();
        assertEquals("New detailed description", ((StringType)desc.getValue()).getValue());

        // unrelated extension preserved
        assertTrue(org.getExtension().stream().anyMatch(e -> "https://example.org/else".equals(e.getUrl())));
        // exactly two: preserved + new description
        assertEquals(2, org.getExtension().size());
    }

    // --- UPDATE_BIOBANK_JUDICIAL_PERSON -------------------------------------

    @Test
    void updateJuridicalPerson_replacesOnlyThatExtension() {
        var org = fhirOrg();
        org.addExtension(new Extension("https://fhir.bbmri.de/StructureDefinition/JuridicalPerson",
                new StringType("OLD-JP")));
        org.addExtension(new Extension("https://example.org/keep", new StringType("ok")));

        var dir = dirBiobank();
        dir.setJuridicalPerson("ACME Foundation");

        UPDATE_BIOBANK_JUDICIAL_PERSON.apply(tuple(org, dir));

        var jp = org.getExtension().stream()
                .filter(e -> "https://fhir.bbmri.de/StructureDefinition/JuridicalPerson".equals(e.getUrl()))
                .findFirst().orElseThrow();
        assertEquals("ACME Foundation", ((StringType)jp.getValue()).getValue());

        assertTrue(org.getExtension().stream().anyMatch(e -> "https://example.org/keep".equals(e.getUrl())));
        assertEquals(2, org.getExtension().size());
    }

    // --- UPDATE_BIOBANK_ALIAS (acronym -> Organization.alias) ----------------

    @Test
    void updateAlias_setsSingleAlias_whenAcronymPresent() {
        var org = fhirOrg();
        var dir = dirBiobank();
        dir.setAcronym("BB-ALPHA");

        UPDATE_BIOBANK_ALIAS.apply(tuple(org, dir));

        assertEquals(1, org.getAlias().size());
        assertEquals("BB-ALPHA", org.getAlias().get(0).getValue());
    }

    @Test
    void updateAlias_noop_whenAcronymMissing() {
        var org = fhirOrg();
        var dir = dirBiobank();
        dir.setAcronym("");

        UPDATE_BIOBANK_ALIAS.apply(tuple(org, dir));
        assertTrue(org.getAlias().isEmpty());
    }

    // --- UPDATE_BIOBANK_CAPABILITIES ----------------------------------------

    @Test
    void updateCapabilities_replacesOnlyThatExtension_andSerializesCapabilities() {
        var org = fhirOrg();
        org.addExtension(new Extension("https://fhir.bbmri.de/StructureDefinition/Capabilities",
                new StringType("OLD"))); // will be removed
        org.addExtension(new Extension("https://example.org/preserve", new StringType("ok")));

        var dir = dirBiobank();
        var caps = new ArrayList<Map>();
        caps.add(new HashMap<>(Map.of("id", "cap1", "label", "Cap One")));
        caps.add(new HashMap<>(Map.of("id", "cap2", "label", "Cap Two")));
        dir.setCapabilities(caps);

        UPDATE_BIOBANK_CAPABILITIES.apply(tuple(org, dir));

        // preserved + two new capabilities = 3
        assertEquals(3, org.getExtension().size());
        assertTrue(org.getExtension().stream().anyMatch(e -> "https://example.org/preserve".equals(e.getUrl())));

        var newCaps = org.getExtension().stream()
                .filter(e -> "https://fhir.bbmri.de/StructureDefinition/Capabilities".equals(e.getUrl()))
                .toList();
        assertEquals(2, newCaps.size());
        // each capability stored as CodeableConcept with Coding(code=id, display=label)
        for (Extension e : newCaps) {
            assertTrue(e.getValue() instanceof CodeableConcept);
            var cc = (CodeableConcept) e.getValue();
            var coding = cc.getCodingFirstRep();
            assertEquals("https://samply.github.io/bbmri-fhir-ig/ValueSet-BiobankCapabilities.html", coding.getSystem());
            assertNotNull(coding.getCode());
            assertNotNull(coding.getDisplay());
        }
    }

    // --- UPDATE_BIOBANK_CONTACT (emails for ADMIN/RESEARCH) ------------------

    @Test
    void updateContact_setsAdminAndResearchEmails_and_replacesExistingEmails_only() {
        var org = fhirOrg();
        // pre-seed with a RESEARCH contact that has old email + a phone (phone should be preserved)
        var research = new Organization.OrganizationContactComponent();
        research.setPurpose(new CodeableConcept().addCoding(new Coding().setCode("RESEARCH").setDisplay("Research")));
        research.addTelecom(new ContactPoint().setSystem(ContactPoint.ContactPointSystem.EMAIL).setValue("old@research"));
        research.addTelecom(new ContactPoint().setSystem(ContactPoint.ContactPointSystem.PHONE).setValue("+49-111"));
//        org.setContact(List.of(research));
        org.getContact().add(research);

        var dir = dirBiobank();
        dir.setHead(new HashMap<>(Map.of("email", "admin@site")));     // ADMIN
        dir.setContact(new HashMap<>(Map.of("email", "new@research"))); // RESEARCH

        UPDATE_BIOBANK_CONTACT.apply(tuple(org, dir));

        // We expect two contacts after update
        assertEquals(2, org.getContact().size());
        // Find ADMIN
        var admin = org.getContact().stream()
                .filter(c -> "ADMIN".equals(c.getPurpose().getCodingFirstRep().getCode()))
                .findFirst().orElseThrow();
        assertEquals(1, admin.getTelecom().size());
        assertEquals(ContactPoint.ContactPointSystem.EMAIL, admin.getTelecomFirstRep().getSystem());
        assertEquals("admin@site", admin.getTelecomFirstRep().getValue());

        // Find RESEARCH
        var researchNow = org.getContact().stream()
                .filter(c -> "RESEARCH".equals(c.getPurpose().getCodingFirstRep().getCode()))
                .findFirst().orElseThrow();
        // email replaced
        assertTrue(researchNow.getTelecom().stream()
                .anyMatch(tp -> tp.getSystem() == ContactPoint.ContactPointSystem.EMAIL
                        && "new@research".equals(tp.getValue())));
        // phone preserved
        assertTrue(researchNow.getTelecom().stream()
                .anyMatch(tp -> tp.getSystem() == ContactPoint.ContactPointSystem.PHONE
                        && "+49-111".equals(tp.getValue())));
        // old email removed
        assertFalse(researchNow.getTelecom().stream()
                .anyMatch(tp -> tp.getSystem() == ContactPoint.ContactPointSystem.EMAIL
                        && "old@research".equals(tp.getValue())));
    }

    @Test
    void updateContact_noop_when_noEmailsProvided() {
        var org = fhirOrg();
        var dir = dirBiobank();
        dir.setHead(new HashMap<>());    // no email
        dir.setContact(new HashMap<>()); // no email

        UPDATE_BIOBANK_CONTACT.apply(tuple(org, dir));
        assertTrue(org.getContact().isEmpty());
    }

    // --- UPDATE_BIOBANK_ADDRESS ---------------------------------------------

    @Test
    void updateAddress_setsCityAndCountry_and_replacesOnlyWhenNeeded() {
        var org = fhirOrg();
        // put an existing address to check replacement path
        var existing = new Address();
        existing.setCity("Old City");
        org.setAddress(List.of(existing));

        var dir = dirBiobank();
        dir.setLocation("New City");
        dir.setCountry(new HashMap<>(Map.of("id", "DE")));

        UPDATE_BIOBANK_ADDRESS.apply(tuple(org, dir));

        assertEquals(1, org.getAddress().size());
        var a = org.getAddressFirstRep();
        assertEquals("New City", a.getCity());
        assertEquals("DE", a.getCountry());
    }

    @Test
    void updateAddress_noop_whenNothingProvided() {
        var org = fhirOrg();
        var dir = dirBiobank();

        UPDATE_BIOBANK_ADDRESS.apply(tuple(org, dir));
        assertTrue(org.getAddress().isEmpty());
    }

    // --- UPDATE_BIOBANK_TELECOM (URL) ---------------------------------------

    @Test
    void updateTelecom_replacesUrl_and_preservesOtherSystems() {
        var org = fhirOrg();
        org.addTelecom(new ContactPoint().setSystem(ContactPoint.ContactPointSystem.URL).setValue("http://old"));
        org.addTelecom(new ContactPoint().setSystem(ContactPoint.ContactPointSystem.PHONE).setValue("+1-222"));

        var dir = dirBiobank();
        dir.setUrl("https://new.example.org");

        UPDATE_BIOBANK_TELECOM.apply(tuple(org, dir));

        // one URL only, updated
        var urls = org.getTelecom().stream()
                .filter(tp -> tp.getSystem() == ContactPoint.ContactPointSystem.URL).toList();
        assertEquals(1, urls.size());
        assertEquals("https://new.example.org", urls.get(0).getValue());

        // phone preserved
        assertTrue(org.getTelecom().stream()
                .anyMatch(tp -> tp.getSystem() == ContactPoint.ContactPointSystem.PHONE
                        && "+1-222".equals(tp.getValue())));
    }

    @Test
    void updateTelecom_noop_whenUrlMissing() {
        var org = fhirOrg();
        org.addTelecom(new ContactPoint().setSystem(ContactPoint.ContactPointSystem.PHONE).setValue("+1-333"));

        var dir = dirBiobank(); // no URL
        UPDATE_BIOBANK_TELECOM.apply(tuple(org, dir));

        assertEquals(1, org.getTelecom().size());
        assertEquals(ContactPoint.ContactPointSystem.PHONE, org.getTelecomFirstRep().getSystem());
    }
}
