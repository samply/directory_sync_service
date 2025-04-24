package de.samply.directory_sync_service.converter;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.model.Collection;
import de.samply.directory_sync_service.model.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class that converts data obtained from the Directory in a map-based structure
 * into {@link Collection} objects and integrates them into {@link Collections}.
 */
public class ConvertDirectoryCollectionGetToCollections {
    private static final Logger logger = LoggerFactory.getLogger(ConvertDirectoryCollectionGetToCollections.class);

    /**
     * Adds a new collection to the specified {@link Collections} object using data extracted from the given map,
     * which will have been obtained from the Directory.
     *
     * @param collections   The {@link Collections} object to which the new collection will be added.
     * @param collectionId  The unique identifier of the collection.
     * @param collectionMap A {@link Map} containing attributes that define the collection.
     */
    public static void addCollectionFromMap(Collections collections, String collectionId, Map<String, Object> collectionMap) {
        collections.addCollection(collectionId, newFromMap(collectionMap));
    }

    /**
     * Converts the provided map representation into a {@link Collection} instance.
     * <p>
     * This method extracts predefined attributes from the map, including identifiers,
     * location details, and metadata, while handling potential null or empty values.
     * </p>
     *
     * @param collectionMap A {@link Map} containing attributes that define the collection.
     * @return A {@link Collection} populated with values from the map, or {@code null} if the map is {@code null}.
     */
    private static Collection newFromMap(Map<String, Object> collectionMap) {
        if (collectionMap == null) {
            logger.warn("newFromMap: collectionMap is null");
            return null;
        }
        Collection collection = new Collection();
        if (collectionMap.isEmpty()) {
            logger.warn("newFromMap: collectionMap is empty");
            return collection;
        }

        if (collectionMap.containsKey("id"))
            collection.setId(getStringFromAttributeMap(collectionMap.get("id")));

        if (collectionMap.containsKey("country"))
            collection.setCountry(getStringFromAttributeMap(collectionMap.get("country")));

        if (collectionMap.containsKey("contact"))
            collection.setContact(getStringFromAttributeMap(collectionMap.get("contact")));

        if (collectionMap.containsKey("biobank"))
            collection.setBiobank(getStringFromAttributeMap(collectionMap.get("biobank")));

        if (collectionMap.containsKey("type"))
            collection.setType(getListOfStringsFromAttribute(collectionMap.get("type")));

        if (collectionMap.containsKey("data_categories"))
            collection.setDataCategories(getListOfStringsFromAttribute(collectionMap.get("data_categories")));

        if (collectionMap.containsKey("network"))
            collection.setNetwork(getListOfStringsFromAttribute(collectionMap.get("network")));

        if (collectionMap.containsKey("name"))
            collection.setName(getStringFromAttributeMap(collectionMap.get("name")));

        if (collectionMap.containsKey("description"))
            collection.setDescription(getStringFromAttributeMap(collectionMap.get("description")));

        return collection;
    }

    /**
     * Extracts a list of strings from the given attribute, ensuring proper handling of list structures.
     * <p>
     * If the attribute is not a {@link List}, an error is logged and {@code null} is returned.
     * The method iterates over each item in the list, converting valid objects into string representations.
     * </p>
     *
     * @param attribute The object expected to contain a list of strings or maps with identifiable attributes.
     * @return A {@link List} of strings extracted from the attribute, or {@code null} if the attribute is invalid.
     */
    private static List<String> getListOfStringsFromAttribute(Object attribute) {
        if (attribute == null) {
            logger.warn("getListOfStringsFromAttribute: attribute is null");
            return null;
        }
        if (!(attribute instanceof List)) {
            logger.warn("getListOfStringsFromAttribute: attribute is not a List: " + Util.jsonStringFomObject(attribute));
            return null;
        }

        List<String> attributeListOfStrings = null;
        try {
            List attributeListOfMaps = (List) attribute;
            attributeListOfStrings = new ArrayList<String>();
            for (Object attributeMap: attributeListOfMaps) {
                String attributeString = getStringFromAttributeMap(attributeMap);
                if (attributeString == null) {
                    logger.warn("getListOfStringsFromAttribute: problem with attribute, skipping");
                    continue;
                }
                attributeListOfStrings.add(attributeString);
            }
        } catch (Exception e) {
            logger.error("getListOfStringsFromAttribute: error" + Util.traceFromException(e));
            return null;
        }

        return attributeListOfStrings;
    }

    /**
     * Extracts a string value from a given attribute, handling both direct string values and maps with identifier keys.
     * <p>
     * If the attribute is a {@link String}, it is returned as-is.
     * If the attribute is a {@link Map}, the method looks for either an {@code id} or {@code name} key and retrieves its value.
     * </p>
     *
     * @param attribute The object expected to be either a string or a map with an identifiable key.
     * @return The extracted string value, or {@code null} if the attribute is invalid or missing identifiers.
     */
    private static String getStringFromAttributeMap(Object attribute) {
        if (attribute == null) {
            logger.warn("getStringFromMapAttribute: attribute is null");
            return null;
        }
        if (attribute instanceof String)
            return (String) attribute;
        if (!(attribute instanceof Map)) {
            logger.warn("getStringFromMapAttribute: attribute is not a Map: " + Util.jsonStringFomObject(attribute));
            return null;
        }

        Map attributeMap = (Map) attribute;
        if (!attributeMap.containsKey("id") && !attributeMap.containsKey("name")) {
            logger.warn("getStringFromMapAttribute: attribute has no id or name: " + Util.jsonStringFomObject(attribute));
            return null;
        }

        if (attributeMap.containsKey("id"))
            return (String) attributeMap.get("id");
        else
            return (String) attributeMap.get("name");
    }
}
