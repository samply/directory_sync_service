package de.samply.directory_sync_service.directory.model;

import de.samply.directory_sync_service.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A DTO for carrying the data relating to a single collection of samples.
 */
public class Collection {
  private static final Logger logger = LoggerFactory.getLogger(Collection.class);
  private String id;
  private Integer size;
  private Integer numberOfDonors;
  private List<String> sex;
  private Integer ageLow;
  private Integer ageHigh;
  private List<String> materials;
  private List<String> storageTemperatures;
  private List<String> diagnosisAvailable;

  private String country;
  private String contact;
  private String biobank;
  private List<String> type;
  private List<String> dataCategories;
  private List<String> network;
  private String name;
  private String description;

  public static Collection newFromMap(Map<String, Object> collectionMap) {
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

  private void setDescription(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  private void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  private void setNetwork(List<String> network) {
    this.network = network;
  }

  public List<String> getNetwork() {
    return network;
  }

  private void setDataCategories(List<String> dataCategories) {
    this.dataCategories = dataCategories;
  }

  public List<String> getDataCategories() {
    return dataCategories;
  }

  private void setType(List<String> type) {
    this.type = type;
  }

  public List<String> getType() {
    return type;
  }

  private void setBiobank(String biobank) {
    this.biobank = biobank;
  }

  public String getBiobank() {
    return biobank;
  }

  private void setContact(String contact) {
    this.contact = contact;
  }

  public String getContact() {
    return contact;
  }

  private void setCountry(String country) {
    this.country = country;
  }

  public String getCountry() {
    return country;
  }

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

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  public Integer getSize() {
    return size;
  }

  public void setNumberOfDonors(Integer numberOfDonors) {
    this.numberOfDonors = numberOfDonors;
  }

  public Integer getNumberOfDonors() {
    return numberOfDonors;
  }

  public void setSex(List<String> sex) {
    this.sex = sex;
  }

  public List<String> getSex() {
    return sex;
  }

  public void setAgeLow(Integer ageLow) {
    this.ageLow = ageLow;
  }

  public Integer getAgeLow() {
    return ageLow;
  }

  public void setAgeHigh(Integer ageHigh) {
    this.ageHigh = ageHigh;
  }

  public Integer getAgeHigh() {
    return ageHigh;
  }

  public void setMaterials(List<String> materials) {
    this.materials = materials;
  }

  public List<String> getMaterials() {
    return materials;
  }

  public void setStorageTemperatures(List<String> storageTemperatures) {
    this.storageTemperatures = storageTemperatures;
  }

  public List<String> getStorageTemperatures() {
    return storageTemperatures;
  }

  public void setDiagnosisAvailable(List<String> diagnosisAvailable) {
    this.diagnosisAvailable = diagnosisAvailable;
  }

  public List<String> getDiagnosisAvailable() {
    return diagnosisAvailable;
  }
}
