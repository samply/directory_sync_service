package de.samply.directory_sync_service.model;

import de.samply.directory_sync_service.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class for holding multiple Collection objects.
 */
public class Collections {
    private static final Logger logger = LoggerFactory.getLogger(Collections.class);
    // A Map of Collections, keyed by collection ID.
    private final TreeMap <String, Collection> collections = new TreeMap<>();

    public void addCollection(String collectionId, Collection collection) {
        if (collection == null) {
            logger.warn("addCollection: collection is null");
            return;
        }
        if (collections.containsKey(collectionId)) {
            Collection existingCollection = collections.get(collectionId);
            existingCollection.combineCollections(collection);
        } else
            collections.put(collectionId, collection);
    }

    public boolean isEmpty() {
        return collections.isEmpty();
    }

    public int size() {
        return collections.size();
    }

    public Collection getOrDefault(String key, Collection collection) {
        return collections.getOrDefault(key, collection);
    }

    public List<Collection> getCollections() {
        return List.copyOf(collections.values());
    }

    public List<String> getCollectionIds() {
        return collections.keySet().stream().collect(Collectors.toList());
    }

    public Collection getCollection(String collectionId) {
        return collections.get(collectionId);
    }

    /**
     * Gets the country code for the collections, e.g. "DE".
     * <p>
     * Assumes that all collections will have the same code and simply returns
     * the code of the first collection.
     * <p>
     * If there are no collections, returns null.
     * <p>
     * May throw a null pointer exception.
     *
     * @return Country code
     */
    public String getCountryCode() {
        String countryCode = null;
        try {
            if (collections.size() == 0) {
                logger.warn("getCountryCode: collectionList empty, cannot determine country code");
                return null;
            }
            logger.debug("getCountryCode: entities.size: " + collections.size());
            // First see if we can extract the country code from a collection.
            for (String collectionID: collections.keySet()) {
                Collection collection = collections.get(collectionID);
                countryCode = collection.getCountry();
                if (countryCode != null && !countryCode.isEmpty())
                    break;
            }
            // If that didn't work, try to extract it from a collection ID.
            if (countryCode == null || countryCode.isEmpty()) {
                for (String collectionID: collections.keySet()) {
                    logger.debug("getCountryCode: collectionID: " + collectionID);
                    Optional<BbmriEricId> bbmriEricId = BbmriEricId.valueOf(collectionID);
                    logger.debug("getCountryCode: bbmriEricId: " + bbmriEricId);
                    countryCode = bbmriEricId.orElse(null).getCountryCode();
                    break;
                }
            }
        } catch (Exception e) {
            logger.warn("getCountryCode: exception: " + Util.traceFromException(e));
            return null;
        }

        logger.debug("getCountryCode: countryCode: " + countryCode);
        return countryCode;
    }

    /**
     * Applies corrections to the available diagnoses of each Collection based on a provided map.
     * The method iterates through the list of entities and updates the available diagnoses
     * using the provided map of corrections.
     *
     * @param correctedDiagnoses A map containing diagnosis corrections, where the keys
     *                           represent the original diagnoses and the values represent
     *                           the corrected diagnoses. Return without applying corrections
     *                           if this parameter is null.
     */
    public void applyDiagnosisCorrections(Map<String, String> correctedDiagnoses) {
        if (correctedDiagnoses == null) // No corrections, so do nothing.
            return;
        for (Collection collection: collections.values())
            collection.applyDiagnosisCorrections(correctedDiagnoses);
    }
}
