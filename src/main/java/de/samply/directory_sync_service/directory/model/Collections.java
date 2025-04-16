package de.samply.directory_sync_service.directory.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;
import java.util.List;

/**
 * A class for holding multiple Collection objects.
 */
public class Collections {
    private static final Logger logger = LoggerFactory.getLogger(Collections.class);
    // A Map of Collections, keyed by collection ID.
    private TreeMap <String, Collection> collections = new TreeMap<>();
    private boolean mockDirectory = false;


    public Collection getCollection(String id) {
        return collections.get(id);
    }

    public void addCollection(Collection collection, String collectionId) {
        if (collection == null) {
            logger.warn("addCollection: collection is null");
            return;
        }
        collections.put(collectionId, collection);
    }

    public void setMockDirectory(boolean mockDirectory) {
        this.mockDirectory = mockDirectory;
    }

    public boolean isMockDirectory() {
        return mockDirectory;
    }

    public void addCollectionFromMap(Map<String, Object> collectionMap, String collectionId) {
        addCollection(Collection.newFromMap(collectionMap), collectionId);
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
}
