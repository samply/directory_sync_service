package de.samply.directory_sync_service.directory.rest;

import de.samply.directory_sync_service.directory.DirectoryCalls;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.Collection;
import de.samply.directory_sync_service.model.Collections;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for DirectoryApiRest that stub out HTTP via a mocked DirectoryCallsRest.
 */
class DirectoryApiRestTest {

    /** Utility to inject a mock into private final field. */
    private static void setPrivate(Object target, String field, Object value) {
        try {
            Field f = DirectoryApiRest.class.getDeclaredField(field);
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Create an API under test with a mocked HTTP layer. */
    private static class Fixture {
        final DirectoryApiRest api;
        final DirectoryCallsRest http; // mock

        Fixture(boolean mockDirectoryFlag) {
            this.api = new DirectoryApiRest("https://host/base", mockDirectoryFlag, "u", "p");
            this.http = mock(DirectoryCallsRest.class);
            setPrivate(api, "directoryCallsRest", http);
        }
    }

    // -------------------- login --------------------

    @Test
    void login_mockMode_returnsTrue_andSkipsHttp() {
        Fixture fx = new Fixture(true);

        assertTrue(fx.api.login());
        verifyNoInteractions(fx.http);
    }

    @Test
    void login_realMode_delegatesToHttpLogin_resultTrue() {
        Fixture fx = new Fixture(false);
        when(fx.http.login()).thenReturn(true);

        assertTrue(fx.api.login());
        verify(fx.http).login();
    }

    @Test
    void login_realMode_delegatesToHttpLogin_resultFalse() {
        Fixture fx = new Fixture(false);
        when(fx.http.login()).thenReturn(false);

        assertFalse(fx.api.login());
        verify(fx.http).login();
    }

    // -------------------- fetchBiobank --------------------

    @Test
    void fetchBiobank_firstTryWithCountry_succeeds() {
        Fixture fx = new Fixture(false);
        Biobank bb = new Biobank();
        when(fx.http.get(contains("/eu_bbmri_eric_DE_biobanks/"), eq(Biobank.class))).thenReturn(bb);

        var id = BbmriEricId.valueOf("bbmri-eric:ID:DE_ABC").orElseThrow();
        Biobank out = fx.api.fetchBiobank(id);

        assertSame(bb, out);
        // ensure fallback was NOT used
        verify(fx.http, never()).get(Mockito.matches(".*/eu_bbmri_eric_biobanks/.*"), eq(Biobank.class));
    }

    @Test
    void fetchBiobank_fallbackToNoCountry_whenFirstIsNull() {
        Fixture fx = new Fixture(false);
        when(fx.http.get(contains("/eu_bbmri_eric_DE_biobanks/"), eq(Biobank.class))).thenReturn(null);
        Biobank bb = new Biobank();
        when(fx.http.get(contains("/eu_bbmri_eric_biobanks/"), eq(Biobank.class))).thenReturn(bb);

        var id = BbmriEricId.valueOf("bbmri-eric:ID:DE_XYZ").orElseThrow();
        assertSame(bb, fx.api.fetchBiobank(id));
    }

    // -------------------- fetchBasicCollectionData --------------------

    private Map<String,Object> singleCollectionGetPayload(Map<String,Object> collectionMap) {
        return Map.of("items", List.of(collectionMap));
    }

    @Test
    void fetchBasicCollectionData_buildsCollections_withCountry_thenAddToCollections() {
        Fixture fx = new Fixture(false);

        // Directory GET response for that collection id
        Map<String,Object> item = new HashMap<>();
        item.put("id", "bbmri-eric:ID:DE_COLL1");
        item.put("name", Map.of("name", "My Collection"));
        when(fx.http.get(contains("/eu_bbmri_eric_DE_collections"), eq(Map.class)))
                .thenReturn(singleCollectionGetPayload(item));

        Collections cols = new Collections();
        // Prepare Collections with a country code and an id to fetch
        var c = new Collection();
        c.setId("bbmri-eric:ID:DE_COLL1");
        c.setCountry("DE");
        cols.addCollection(c.getId(), c);

        fx.api.fetchBasicCollectionData(cols);

        // After conversion the same id should still be present; name populated from payload.
        var after = cols.getCollections().get(0);
        assertEquals("bbmri-eric:ID:DE_COLL1", after.getId());
        assertEquals("My Collection", after.getName());
    }

    @Test
    void fetchBasicCollectionData_fallbackNoCountry_whenFirstNull() {
        Fixture fx = new Fixture(false);

        Map<String,Object> item = new HashMap<>();
        item.put("id", "bbmri-eric:ID:CY_COLL2");
        item.put("name", Map.of("name", "CY Collection"));
        when(fx.http.get(contains("/eu_bbmri_eric_CY_collections"), eq(Map.class))).thenReturn(null);
        when(fx.http.get(contains("/eu_bbmri_eric_collections"), eq(Map.class)))
                .thenReturn(singleCollectionGetPayload(item));

        Collections cols = new Collections();
        var c = new Collection();
        c.setId("bbmri-eric:ID:CY_COLL2");
        c.setCountry("CY");
        cols.addCollection(c.getId(), c);

        fx.api.fetchBasicCollectionData(cols);
        assertEquals("CY Collection", cols.getCollections().get(0).getName());
    }

    @Test
    void fetchBasicCollectionData_handlesMissingItems_gracefully() {
        Fixture fx = new Fixture(false);
        when(fx.http.get(anyString(), eq(Map.class))).thenReturn(Map.of("not_items", 1));

        Collections cols = new Collections();
        var c = new Collection();
        c.setId("bbmri-eric:ID:DE_COLL3");
        c.setCountry("DE");
        cols.addCollection(c.getId(), c);

        assertDoesNotThrow(() -> fx.api.fetchBasicCollectionData(cols));
        // Nothing added/changed (name remains null)
        assertNull(cols.getCollections().get(0).getName());
    }

    // -------------------- sendUpdatedCollections --------------------

    /**
     * Build a minimal Collections input that the converter can handle without NPE and yields DE country code.
     */
    private Collections minimalCollectionsDE() {
        Collections cols = new Collections();
        Collection c = new Collection();
        c.setId("bbmri-eric:ID:DE_COLL");
        c.setCountry("DE"); // allows DirectoryCollectionPut.getCountryCode()
        c.setSize(100);     // avoid Math.log10(null) in converter
        c.setNumberOfDonors(100);
        c.setSex(List.of("male"));
        c.setMaterials(List.of("FFPE"));
        c.setStorageTemperatures(List.of("temperatureLiquidNitrogen"));
        c.setDiagnosisAvailable(List.of("C10"));
        cols.addCollection(c.getId(), c);
        return cols;
    }

    @Test
    void sendUpdatedCollections_success_onFirstPut() {
        Fixture fx = new Fixture(false);
        when(fx.http.put(contains("/eu_bbmri_eric_DE_collections"), any())).thenReturn("{\"ok\":true}");

        assertTrue(fx.api.sendUpdatedCollections(minimalCollectionsDE()));
        verify(fx.http).put(contains("/eu_bbmri_eric_DE_collections"), any(DirectoryCollectionPut.class));
    }

    @Test
    void sendUpdatedCollections_fallback_whenFirstPutNull() {
        Fixture fx = new Fixture(false);
        when(fx.http.put(contains("/eu_bbmri_eric_DE_collections"), any())).thenReturn(null);
        when(fx.http.put(contains("/eu_bbmri_eric_collections"), any())).thenReturn("{\"ok\":true}");

        assertTrue(fx.api.sendUpdatedCollections(minimalCollectionsDE()));
        verify(fx.http).put(contains("/eu_bbmri_eric_collections"), any(DirectoryCollectionPut.class));
    }

    @Test
    void sendUpdatedCollections_converterReturnsNull_returnsFalse() {
        // Create input that causes converter to throw inside (size=null → Math.log10)
        Fixture fx = new Fixture(false);
        Collections cols = new Collections();
        Collection c = new Collection();
        c.setId("bbmri-eric:ID:DE_BAD");
        c.setCountry("DE");
        c.setSize(null); // NPE -> converter returns null -> sendUpdatedCollections false
        cols.addCollection(c.getId(), c);

        assertFalse(fx.api.sendUpdatedCollections(cols));
        verifyNoInteractions(fx.http);
    }

    // -------------------- updateFactTablesBlock --------------------

    @Test
    void updateFactTablesBlock_emptyList_shortCircuitsTrue() {
        Fixture fx = new Fixture(false);
        assertTrue(fx.api.updateFactTablesBlock("DE", List.of()));
        verifyNoInteractions(fx.http);
    }

    @Test
    void updateFactTablesBlock_successFirstPost() {
        Fixture fx = new Fixture(false);
        when(fx.http.post(contains("/eu_bbmri_eric_DE_facts"), any())).thenReturn("{\"ok\":1}");

        assertTrue(fx.api.updateFactTablesBlock("DE", List.of(Map.of("id","x"))));
        verify(fx.http).post(contains("/eu_bbmri_eric_DE_facts"), any());
    }

    @Test
    void updateFactTablesBlock_fallbackOnNull_thenSuccess() {
        Fixture fx = new Fixture(false);
        when(fx.http.post(contains("/eu_bbmri_eric_DE_facts"), any())).thenReturn(null);
        when(fx.http.post(contains("/eu_bbmri_eric_facts"), any())).thenReturn("{\"ok\":1}");

        assertTrue(fx.api.updateFactTablesBlock("DE", List.of(Map.of("id","x"))));
        verify(fx.http).post(contains("/eu_bbmri_eric_facts"), any());
    }

    @Test
    void updateFactTablesBlock_bothNull_returnsFalse() {
        Fixture fx = new Fixture(false);
        when(fx.http.post(anyString(), any())).thenReturn(null);

        assertFalse(fx.api.updateFactTablesBlock("DE", List.of(Map.of("id","x"))));
    }

    // -------------------- getNextPageOfFactIdsForCollection --------------------

    @Test
    void getNextPageOfFactIds_okAndExtractsIds() {
        Fixture fx = new Fixture(false);
        when(fx.http.get(contains("/eu_bbmri_eric_DE_facts"), eq(Map.class)))
                .thenReturn(Map.of("items", List.of(Map.of("id","f1"), Map.of("id","f2"))));

        List<String> ids = fx.api.getNextPageOfFactIdsForCollection("bbmri-eric:ID:DE_COLL");
        assertEquals(List.of("f1", "f2"), ids);
    }

    @Test
    void getNextPageOfFactIds_fallbackWhenNull_thenOk() {
        Fixture fx = new Fixture(false);
        when(fx.http.get(contains("/eu_bbmri_eric_DE_facts"), eq(Map.class))).thenReturn(null);
        when(fx.http.get(contains("/eu_bbmri_eric_facts"), eq(Map.class)))
                .thenReturn(Map.of("items", List.of(Map.of("id","f9"))));

        List<String> ids = fx.api.getNextPageOfFactIdsForCollection("bbmri-eric:ID:DE_COLL");
        assertEquals(List.of("f9"), ids);
    }

    @Test
    void getNextPageOfFactIds_itemsEmpty_returnsEmptyList() {
        Fixture fx = new Fixture(false);
        when(fx.http.get(anyString(), eq(Map.class))).thenReturn(Map.of("items", List.of()));

        List<String> ids = fx.api.getNextPageOfFactIdsForCollection("bbmri-eric:ID:DE_COLL");
        assertNotNull(ids);
        assertTrue(ids.isEmpty());
    }

    @Test
    void getNextPageOfFactIds_missingItems_returnsNull() {
        Fixture fx = new Fixture(false);
        when(fx.http.get(anyString(), eq(Map.class))).thenReturn(Map.of("total", 0));

        assertNull(fx.api.getNextPageOfFactIdsForCollection("bbmri-eric:ID:DE_COLL"));
    }

    // -------------------- deleteFactsByIds --------------------

    @Test
    void deleteFactsByIds_empty_returnsTrue() {
        Fixture fx = new Fixture(false);
        assertTrue(fx.api.deleteFactsByIds("DE", List.of()));
        verifyNoInteractions(fx.http);
    }

    @Test
    void deleteFactsByIds_successFirstDelete() {
        Fixture fx = new Fixture(false);
        when(fx.http.delete(contains("/eu_bbmri_eric_DE_facts"), any())).thenReturn("ok");

        assertTrue(fx.api.deleteFactsByIds("DE", List.of("a","b")));
        verify(fx.http).delete(contains("/eu_bbmri_eric_DE_facts"), any());
    }

    @Test
    void deleteFactsByIds_fallback_thenSuccess() {
        Fixture fx = new Fixture(false);
        when(fx.http.delete(contains("/eu_bbmri_eric_DE_facts"), any())).thenReturn(null);
        when(fx.http.delete(contains("/eu_bbmri_eric_facts"), any())).thenReturn("ok");

        assertTrue(fx.api.deleteFactsByIds("DE", List.of("a")));
        verify(fx.http).delete(contains("/eu_bbmri_eric_facts"), any());
    }

    @Test
    void deleteFactsByIds_bothNull_returnsFalse() {
        Fixture fx = new Fixture(false);
        when(fx.http.delete(anyString(), any())).thenReturn(null);

        assertFalse(fx.api.deleteFactsByIds("DE", List.of("a")));
    }

    // -------------------- isValidIcdValue --------------------

    @Test
    void isValidIcdValue_totalPositive_true() {
        Fixture fx = new Fixture(false);
        when(fx.http.get(contains("/eu_bbmri_eric_disease_types"), eq(Map.class)))
                .thenReturn(Map.of("total", 1.0d));

        assertTrue(fx.api.isValidIcdValue("C10"));
    }

    @Test
    void isValidIcdValue_totalZero_false() {
        Fixture fx = new Fixture(false);
        when(fx.http.get(anyString(), eq(Map.class))).thenReturn(Map.of("total", 0.0d));

        assertFalse(fx.api.isValidIcdValue("Z99"));
    }

    @Test
    void isValidIcdValue_missingTotal_false() {
        Fixture fx = new Fixture(false);
        when(fx.http.get(anyString(), eq(Map.class))).thenReturn(Map.of("items", List.of()));

        assertFalse(fx.api.isValidIcdValue("C10"));
    }

    @Test
    void isValidIcdValue_nullBody_false() {
        Fixture fx = new Fixture(false);
        when(fx.http.get(anyString(), eq(Map.class))).thenReturn(null);

        assertFalse(fx.api.isValidIcdValue("C10"));
    }
}
