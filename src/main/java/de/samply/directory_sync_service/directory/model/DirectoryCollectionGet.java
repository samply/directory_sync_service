package de.samply.directory_sync_service.directory.model;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a data transfer object that maps onto the JSON returned by a GET request
 * to the Directory API when you want to obtain information about collections.
 * <p>
 * It simply extends a Map and adds a single key, "items". This contains a list
 * of collections. Each collection is also a Map, with keys corresponding to the
 * various attributes needed when updating, such as collection name or ID.
 * <p>
 * The getter methods allow you to get attributes in collections identified by
 * collection ID. If you use an ID that is not known, you will get a null pointer
 * exception.
 */
public class DirectoryCollectionGet extends HashMap {
    private static final Logger logger = LoggerFactory.getLogger(DirectoryCollectionGet.class);
    private boolean mockDirectory = false;

    public void setMockDirectory(boolean mockDirectory) {
        this.mockDirectory = mockDirectory;
    }

    public boolean isMockDirectory() {
        return mockDirectory;
    }

    public void init() {
        put("items", new ArrayList());
    }

    public String getCountryId(String id) {
        return (String) ((Map) getItem(id).get("country")).get("id");
    }

    public String getContactId(String id) {
        return (String) ((Map) getItem(id).get("contact")).get("id");
    }

    public String getBiobankId(String id) {
        return (String) ((Map) getItem(id).get("biobank")).get("id");
    }

    public List<String> getTypeIds(String id) {
        Map item = getItem(id);
        List<Map<String,Object>> types = (List<Map<String,Object>>) item.get("type");
        List<String> typeLabels = new ArrayList<String>();
        for (Map type: types)
            typeLabels.add((String) type.get("id"));

        return typeLabels;
    }

    public List<String> getDataCategoryIds(String id) {
        Map item = getItem(id);
        List<Map<String,Object>> dataCategories = (List<Map<String,Object>>) item.get("data_categories");
        List<String> dataCategoryLabels = new ArrayList<String>();
        for (Map type: dataCategories)
            dataCategoryLabels.add((String) type.get("id"));

        return dataCategoryLabels;
    }

    public List<String> getNetworkIds(String id) {
        Map item = getItem(id);
        List<Map<String,Object>> networks = (List<Map<String,Object>>) item.get("network");
        List<String> networkLabels = new ArrayList<String>();
        for (Map type: networks)
            networkLabels.add((String) type.get("id"));

        return networkLabels;
    }

    public String getName(String id) {
        return (String) getItem(id).get("name");
    }

    public String getDescription(String id) {
        return (String) getItem(id).get("description");
    }

    public List<Map> getItems() {
        if (!this.containsKey("items")) {
            logger.warn("DirectoryCollectionGet.getItems: no items key, aborting");
            return null;
        }
        return (List<Map>) get("items");
    }

    public Map getItemZero() {
        if (!containsKey("items"))
            return null;
        List<Map> itemList = (List<Map>) get("items");
        if (itemList == null || itemList.size() == 0)
            return null;
        return itemList.get(0);
    }

   private Map getItem(String id) {
        Map item = null;

        List<Map> items = getItems();
        if (items == null)
            return null;

        for (Map e: items) {
            if (e == null) {
                logger.warn("DirectoryCollectionGet.getItem: problem with getItems()");
                continue;
            }
            if (e.get("id").equals(id)) {
                item = e;
                break;
            }
        }

        return item;
    }
}
