package de.samply.directory_sync_service.directory.model;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.samply.directory_sync_service.Util;

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
        if (getItem(id) == null)
            logger.warn("getCountryId: item is null for id: " + id);

        if (!getItem(id).containsKey("country"))
            return "";
        return (String) ((Map) getItem(id).get("country")).get("id");
    }

    public String getContactId(String id) {
        if (getItem(id) == null)
            logger.warn("getContactId: item is null for id: " + id);

        if (!getItem(id).containsKey("contact"))
            return "";
        return (String) ((Map) getItem(id).get("contact")).get("id");
    }

    public String getBiobankId(String id) {
        if (getItem(id) == null)
            logger.warn("getBiobankId: item is null for id: " + id);

        if (!getItem(id).containsKey("biobank"))
            return "";
        return (String) ((Map) getItem(id).get("biobank")).get("id");
    }

    public List<String> getTypeIds(String id) {
        if (getItem(id) == null)
            logger.warn("getTypeIds: item is null for id: " + id);

        if (!getItem(id).containsKey("type"))
            return new ArrayList<String>();
        Map item = getItem(id);
        List<Map<String,Object>> types = (List<Map<String,Object>>) item.get("type");
        List<String> typeLabels = new ArrayList<String>();
        for (Map type: types)
            if (type.containsKey("id"))
                typeLabels.add((String) type.get("id"));
            else if (type.containsKey("name"))
                typeLabels.add((String) type.get("name"));
            else {
                logger.warn("getTypeIds: one of the types has no id or name, type: " + Util.jsonStringFomObject(type));
                return null;
            }

        return typeLabels;
    }

    public List<String> getDataCategoryIds(String id) {
        if (getItem(id) == null)
            logger.warn("getDataCategoryIds: item is null for id: " + id);

        if (!getItem(id).containsKey("data_categories"))
            return new ArrayList<String>();
        Map item = getItem(id);
        List<Map<String,Object>> dataCategories = (List<Map<String,Object>>) item.get("data_categories");
        List<String> dataCategoryLabels = new ArrayList<String>();
        for (Map dataCategory: dataCategories)
            if (dataCategory.containsKey("id"))
                dataCategoryLabels.add((String) dataCategory.get("id"));
            else if (dataCategory.containsKey("name"))
                dataCategoryLabels.add((String) dataCategory.get("name"));
            else {
                logger.warn("getDataCategoryIds: one of the types has no id or name, type: " + Util.jsonStringFomObject(dataCategory));
                return null;
            }

        return dataCategoryLabels;
    }

    public List<String> getNetworkIds(String id) {
        if (getItem(id) == null)
            logger.warn("getNetworkIds: item is null for id: " + id);

        if (!getItem(id).containsKey("network"))
            return new ArrayList<String>();
        Map item = getItem(id);
        List<Map<String,Object>> networks = (List<Map<String,Object>>) item.get("network");
        List<String> networkLabels = new ArrayList<String>();
        for (Map type: networks)
            networkLabels.add((String) type.get("id"));

        return networkLabels;
    }

    public String getName(String id) {
        if (getItem(id) == null)
            logger.warn("getName: item is null for id: " + id);

        if (getItem(id) == null) {
            logger.warn("getName: item is null for id: " + id + ", aborting");
            return "";
        }
        if (!getItem(id).containsKey("name")) {
            logger.warn("getName: no name key, aborting");
            return "";
        }
        return (String) getItem(id).get("name");
    }

    public String getDescription(String id) {
        if (getItem(id) == null)
            logger.warn("getDescription: item is null for id: " + id);

        if (!getItem(id).containsKey("description"))
            return "";
        return (String) getItem(id).get("description");
    }

    public List<Map> getItems() {
        if (!this.containsKey("items")) {
            logger.warn("getItems: no items key, aborting");
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
        if (items == null) {
            logger.warn("DirectoryCollectionGet.getItem: getItems() returns null!");
            return null;
        }

        for (Map e: items) {
            if (e == null) {
                logger.warn("DirectoryCollectionGet.getItem: one of the items is null, skipping it");
                continue;
            }
            String eId = null;
            if (e.containsKey("id"))
                eId = (String) e.get("id");
            else {
                logger.warn("DirectoryCollectionGet.getItem: one of the items has no id, skipping it");
                continue;
            }
            if (eId.equals(id)) {
                item = e;
                break;
            }
        }

        if (item == null)
            logger.warn("DirectoryCollectionGet.getItem: could not find item with id " + id);

        return item;
    }
}
