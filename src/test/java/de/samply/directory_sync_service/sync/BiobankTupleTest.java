package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.directory.model.Biobank;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BiobankTupleTest {

    // -- Constructor guards --------------------------------------------------

    @Test
    void constructor_throws_when_fhirBiobank_null() {
        Biobank dir = new Biobank();
        assertThrows(NullPointerException.class, () -> new BiobankTuple(null, dir));
    }

    @Test
    void constructor_throws_when_dirBiobank_null() {
        Organization org = new Organization();
        assertThrows(NullPointerException.class, () -> new BiobankTuple(org, null));
    }

    // -- Change detection (equalsDeep vs. snapshot) --------------------------

    @Test
    void hasChanged_isFalse_initially() {
        Organization org = new Organization();
        org.setName("Initial");
        Biobank dir = new Biobank();

        BiobankTuple tuple = new BiobankTuple(org, dir);

        assertFalse(tuple.hasChanged(), "Fresh tuple should report no changes");
    }

    @Test
    void hasChanged_isTrue_after_simple_field_change() {
        Organization org = new Organization();
        org.setName("A");
        Biobank dir = new Biobank();

        BiobankTuple tuple = new BiobankTuple(org, dir);
        assertFalse(tuple.hasChanged());

        // Modify a simple scalar field
        org.setName("B");

        assertTrue(tuple.hasChanged(), "Changing name should be detected");
    }

    @Test
    void hasChanged_isTrue_after_nested_collection_change() {
        Organization org = new Organization();
        Biobank dir = new Biobank();

        BiobankTuple tuple = new BiobankTuple(org, dir);
        assertFalse(tuple.hasChanged());

        // Modify a nested structure (addresses list)
        org.addAddress(new Address().setCity("Berlin"));

        assertTrue(tuple.hasChanged(), "Adding an Address should be detected");
    }

    @Test
    void hasChanged_isFalse_after_noOp_set_of_same_value() {
        Organization org = new Organization();
        org.setName("Same");
        Biobank dir = new Biobank();

        BiobankTuple tuple = new BiobankTuple(org, dir);
        assertFalse(tuple.hasChanged());

        // Set the same value again (no-op semantically)
        org.setName("Same");

        assertFalse(tuple.hasChanged(), "Re-setting the same value should not count as change");
    }

    @Test
    void hasChanged_isTrue_after_alias_added() {
        Organization org = new Organization();
        Biobank dir = new Biobank();

        BiobankTuple tuple = new BiobankTuple(org, dir);
        assertFalse(tuple.hasChanged());

        // Add an alias (nested list of StringType)
        org.setAlias(List.of(new StringType("ACRONYM")));

        assertTrue(tuple.hasChanged(), "Adding alias should be detected by equalsDeep");
    }
}
