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
  private Integer ageHigh;
  private Integer ageLow;
  private String biobank;
  private String contact;
  private String country;
  private List<String> dataCategories;
  private String description;
  private List<String> diagnosisAvailable;
  private String id;
  private List<String> materials;
  private String name;
  private List<String> network;
  private Integer numberOfDonors;
  private List<String> sex;
  private Integer size;
  private List<String> storageTemperatures;
  private List<String> type;

  public Integer getAgeHigh() {
    return ageHigh;
  }

  public void setAgeHigh(Integer ageHigh) {
    this.ageHigh = ageHigh;
  }

  public Integer getAgeLow() {
    return ageLow;
  }

  public void setAgeLow(Integer ageLow) {
    this.ageLow = ageLow;
  }

  public String getBiobank() {
    return biobank;
  }

  public void setBiobank(String biobank) {
    this.biobank = biobank;
  }

  public String getContact() {
    return contact;
  }

  public void setContact(String contact) {
    this.contact = contact;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public List<String> getDataCategories() {
    return dataCategories;
  }

  public void setDataCategories(List<String> dataCategories) {
    this.dataCategories = dataCategories;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public List<String> getDiagnosisAvailable() {
    return diagnosisAvailable;
  }

  public void setDiagnosisAvailable(List<String> diagnosisAvailable) {
    this.diagnosisAvailable = diagnosisAvailable;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public List<String> getMaterials() {
    return materials;
  }

  public void setMaterials(List<String> materials) {
    this.materials = materials;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getNetwork() {
    return network;
  }

  public void setNetwork(List<String> network) {
    this.network = network;
  }

  public Integer getNumberOfDonors() {
    return numberOfDonors;
  }

  public void setNumberOfDonors(Integer numberOfDonors) {
    this.numberOfDonors = numberOfDonors;
  }

  public List<String> getSex() {
    return sex;
  }

  public void setSex(List<String> sex) {
    this.sex = sex;
  }

  public Integer getSize() {
    return size;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  public List<String> getStorageTemperatures() {
    return storageTemperatures;
  }

  public void setStorageTemperatures(List<String> storageTemperatures) {
    this.storageTemperatures = storageTemperatures;
  }

  public List<String> getType() {
    return type;
  }

  public void setType(List<String> type) {
    this.type = type;
  }

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

  /**
   * Applies corrections to the available diagnoses based on a provided map.
   *
   * @param correctedDiagnoses A map containing diagnosis corrections, where the keys
   *                           represent the original diagnoses and the values represent
   *                           the corrected diagnoses. Return without applying corrections
   *                           if this parameter is null.
   */
  public void applyDiagnosisCorrections(Map<String, String> correctedDiagnoses) {
    if (correctedDiagnoses == null)
      return;
    List<String> directoryDiagnoses = new ArrayList<>();

    if (correctedDiagnoses.isEmpty())
      logger.debug("applyDiagnosisCorrections: correctedDiagnoses is empty");
    else
      logger.debug("applyDiagnosisCorrections: correctedDiagnoses: " + Util.jsonStringFomObject(correctedDiagnoses));

    // Retrieve available diagnoses
    for (String diagnosis : getDiagnosisAvailable()) {
      String miriamDiagnosis = diagnosis;
      if (!miriamDiagnosis.startsWith("urn:miriam:icd:"))
        miriamDiagnosis = "urn:miriam:icd:" + diagnosis;
      logger.debug("applyDiagnosisCorrections: diagnosis: " + diagnosis);
      if (miriamDiagnosis != null && correctedDiagnoses.containsKey(miriamDiagnosis) && correctedDiagnoses.get(miriamDiagnosis) != null) {
        String correctedDiagnosis = correctedDiagnoses.get(miriamDiagnosis);
        logger.debug("applyDiagnosisCorrections: corrected diagnosis: " + correctedDiagnosis);

        // Add to list only if it's not already present
        if (!directoryDiagnoses.contains(correctedDiagnosis)) {
          logger.debug("applyDiagnosisCorrections: adding diagnosis: " + correctedDiagnosis.substring(15));
          // Strip MIRIAM part of diagnosis before adding to list
          directoryDiagnoses.add(correctedDiagnosis.substring(15));
        }
      }
    }

    setDiagnosisAvailable(directoryDiagnoses);
  }

  /**
   * Merges data from the provided {@code collection} into the current object.
   * <p>
   * This method checks for filled values in the provided collection's fields.
   * If a field is filled, it is copied over into the current object.
   * </p>
   *
   * @param collection the {@link Collection} object containing data to merge into the current collection.
   *                   If {@code null}, a warning is logged, and no merging occurs.
   */
  public void combineCollections(Collection collection) {
    if (collection == null) {
      logger.warn("combineCollections: collection is null, cannot combine");
      return;
    }

    if (collection.getAgeHigh() != null) setAgeHigh(collection.getAgeHigh());
    if (collection.getAgeLow() != null) setAgeLow(collection.getAgeLow());
    if (collection.getBiobank() != null) setBiobank(collection.getBiobank());
    if (collection.getContact() != null) setContact(collection.getContact());
    if (collection.getCountry() != null) setCountry(collection.getCountry());
    if (collection.getDataCategories() != null && !collection.getDataCategories().isEmpty()) setDataCategories(collection.getDataCategories());
    if (collection.getDescription() != null) setDescription(collection.getDescription());
    if (collection.getDiagnosisAvailable() != null && !collection.getDiagnosisAvailable().isEmpty()) setDiagnosisAvailable(collection.getDiagnosisAvailable());
    if (collection.getId() != null) setId(collection.getId());
    if (collection.getMaterials() != null && !collection.getMaterials().isEmpty()) setMaterials(collection.getMaterials());
    if (collection.getName() != null) setName(collection.getName());
    if (collection.getNetwork() != null && !collection.getNetwork().isEmpty()) setNetwork(collection.getNetwork());
    if (collection.getNumberOfDonors() != null) setNumberOfDonors(collection.getNumberOfDonors());
    if (collection.getSex() != null && !collection.getSex().isEmpty()) setSex(collection.getSex());
    if (collection.getSize() != null) setSize(collection.getSize());
    if (collection.getStorageTemperatures() != null && !collection.getStorageTemperatures().isEmpty()) setStorageTemperatures(collection.getStorageTemperatures());
    if (collection.getType() != null && !collection.getType().isEmpty()) setType(collection.getType());

  }
}
