package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.Collections;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.StarModelData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The DirectoryApi class provides an interface for interacting with the Directory service.
 * This class allows for fetching and updating biobank and collection information, managing star models,
 * and performing various validation and correction operations.
 * It supports a mock mode for testing purposes, where no real Directory interactions are performed.
 */
public abstract class DirectoryApi {
  protected static final Logger logger = LoggerFactory.getLogger(DirectoryApi.class);
  protected DirectoryCalls directoryCalls;
  protected DirectoryEndpoints directoryEndpoints;
  // Setting this variable to true will prevent any contact being made to the Directory.
  // All public methods will return feasible fake results.
  protected boolean mockDirectory = false;

  /**
   * Constructs a new DirectoryApiRest instance.
   * If we are not in mocking mode, log in to the Directory.
   *
   * @param baseUrl The base URL of the Directory service.
   * @param mockDirectory If true, the instance operates in mock mode, returning fake data.
   * @param username The username for authenticating with the Directory.
   * @param password The password for authenticating with the Directory.
   */
  public DirectoryApi(String baseUrl, boolean mockDirectory, String username, String password) {
    this.mockDirectory = mockDirectory;
  }

  /**
   * @return true if a login endpoint for this API is accessible, false otherwise.
   */
  public boolean isLoginAvailable() {
    String endpoint = directoryEndpoints.getLoginEndpoint();

    if (!directoryCalls.endpointExists(endpoint)) {
      logger.debug("isLoginAvailable: failing availablity test because " + endpoint + " is not accessible");
      return false;
    }

    logger.debug("isAvailable: login availability test has succeeded");

    return true;
  }

  /**
   * Log in to the Directory. You can log in as many times as you like.
   */
  public abstract boolean login();

  /**
   * Fetches the Biobank with the given {@code id}.
   *
   * @param id the ID of the Biobank to fetch.
   * @return either the Biobank or null if an error occurs
   */
  public abstract Biobank fetchBiobank(BbmriEricId id);

  /**
   * Make API calls to the Directory to fill a Collections object with attributes
   * for all of the collections listed in collectionIds. The countryCode is used solely for
   * constructing the URL for the API call.
   *
   * @param putCollections
   * @return
   */
  public abstract Collections fetchBasicCollectionData(Collections putCollections);

  /**
   * Send aggregated collection information to the Directory.
   *
   * @param collections Summary information about one or more collections
   * @return an outcome, either successful or null
   */
  public abstract boolean sendUpdatedCollections(Collections collections);

  /**
   * Updates the Star Model data in the Directory service based on the provided StarModelInputData.
   * <p>
   * Before sending any star model data to the Directory, the original
   * star model data for all known collections will be deleted from the
   * Directory.
   *
   * @param starModelInputData The input data for updating the Star Model.
   * @return An OperationOutcome indicating the success or failure of the update.
   */
  public boolean updateStarModel(StarModelData starModelInputData) {
    if (mockDirectory) {
      // Dummy return if we're in mock mode
      return true;
    }

    // Get rid of previous star models first. This is necessary, because:
    // 1. A new star model may be decomposed into different hypercubes.
    // 2. The new fact IDs may be different from the old ones.
    // 3. We will be using a POST and it will return an error if we try
    //    to overwrite an existing fact.
    logger.debug("updateStarModel: deleting old star models");
    if (!deleteStarModel(starModelInputData)) {
      logger.warn("updateStarModel: Problem deleting star models");
      logger.warn("updateStarModel: carrying on regardless");
//      return false;
    }

    String countryCode = starModelInputData.getCountryCode();
    List<Map<String, String>> factTables = starModelInputData.getFactTables();
    int blockSize = 1000;

    // Break the fact table into blocks of 1000 before sending to the Directory.
    // This is the maximum number of facts allowed per Directory API call.
    for (int i = 0; i < factTables.size(); i += blockSize) {
      List<Map<String, String>> factTablesBlock = factTables.subList(i, Math.min(i + blockSize, factTables.size()));

      logger.debug("updateStarModel: sending block: " + i + " of " + factTables.size());
      if (!updateFactTablesBlock(countryCode, factTablesBlock)) {
        logger.warn("updateStarModel: failed, block: " + i + " of " + factTables.size());
        return false;
      }
    }

    return true;
  }

  /**
   * Updates the fact tables block for a specific country with the provided data.
   *
   * @param countryCode The country code, e.g. DE.
   * @param factTablesBlock A list of maps representing the fact tables block data.
   * @return true if the update was successful, false otherwise.
   */
  protected abstract boolean updateFactTablesBlock(String countryCode, List<Map<String, String>> factTablesBlock);

  /**
   * Deletes existing star models from the Directory service for each of the collection IDs in the supplied StarModelInputData object.
   *
   * @param starModelInputData The input data for deleting existing star models.
   * @return An boolean indicating the success or failure of the deletion.
   */
  protected boolean deleteStarModel(StarModelData starModelInputData) {
    try {
      for (String collectionId: starModelInputData.getInputCollectionIds())
        if (!deleteStarModelForCollection(starModelInputData, collectionId)) {
          logger.warn("deleteStarModel: Problem deleting star model for collection: " + collectionId);
          return false;
        }
    } catch(Exception e) {
      logger.warn("deleteStarModel: Exception during delete: " + Util.traceFromException(e));
      return false;
    }

    return true;
  }

  private boolean deleteStarModelForCollection(StarModelData starModelInputData, String collectionId) {
    String countryCode = extractCountryCodeFromBbmriEricId(collectionId);

    // Collect fact IDs by looping until no more facts are left in the Directory.
    // We need to do things this way, because the Directory implements paging
    // and a single pass may not get all facts.
    do {
      List<String> pageFactIds = getNextPageOfFactIdsForCollection(collectionId);

      if (pageFactIds == null) {
        logger.warn("deleteStarModelForCollection: Problem getting facts for collection: " + collectionId);
        return false;
      }
      if (pageFactIds.size() == 0)
        // Terminate the do loop if there are no more facts left.
        break;

      logger.debug("deleteStarModelForCollection: number of facts found: " + pageFactIds.size() + " for collection: " + collectionId);

      if (!deleteFactsByIds(countryCode, pageFactIds))
        logger.warn("deleteStarModelForCollection: Problem deleting facts for collection: " + collectionId);
    } while (true);

    return true;
  }

  /**
   * Retrieves a list of fact IDs from the Directory associated with a specific collection.
   *
   * @param collectionId The ID of the collection to retrieve fact IDs for.
   * @return A list of fact IDs for the specified collection, or null if there is an issue retrieving the data. An empty list indicates that there are no more facts left to be retrieved.
   */
  protected abstract List<String> getNextPageOfFactIdsForCollection(String collectionId);

  /**
   * Deletes facts from the Directory service based on a list of fact IDs.
   *
   * @param countryCode    e.g. DE.
   * @param factIds   The list of fact IDs to be deleted.
   * @return An OperationOutcome indicating the success or failure of the deletion.
   */
  protected abstract boolean deleteFactsByIds(String countryCode, List<String> factIds);

  /**
   * Collects diagnosis corrections from the Directory.
   * <p>
   * It checks with the Directory if the diagnosis codes are valid ICD values and corrects them if necessary.
   * <p>
   * Two levels of correction are possible:
   * <p>
   * 1. If the full code is not correct, remove the number after the period and try again. If the new truncated code is OK, use it to replace the existing diagnosis.
   * 2. If that doesn't work, replace the existing diagnosis with null.
   *
   * The supplied Map object, {@code diagnoses}, is modified in-place.
   *
   * @param diagnoses A string map containing diagnoses to be corrected.
   */
  public void collectDiagnosisCorrections(Map<String, String> diagnoses) {
    if (mockDirectory)
      // Don't do anything if we're in mock mode
      return;

    if (diagnoses.size() == 0) {
      logger.warn("collectDiagnosisCorrections: diagnoses is empty");
      return;
    }

    if (diagnoses.keySet().size() > 0 && diagnoses.keySet().size() < 5) {
      logger.debug("collectDiagnosisCorrections: uncorrected diagnoses: ");
      for (String diagnosis : diagnoses.keySet())
        logger.debug("collectDiagnosisCorrections: diagnosis: " + diagnosis);
    }

    int diagnosisCounter = 0; // for diagnostics only
    int invalidIcdValueCounter = 0;
    int correctedIcdValueCounter = 0;
    int discardedIcdValueCounter = 0;
    for (String diagnosis: diagnoses.keySet()) {
      if (diagnosisCounter%500 == 0)
        logger.debug("collectDiagnosisCorrections: diagnosisCounter: " + diagnosisCounter + ", total diagnoses: " + diagnoses.size());
      if (!isValidIcdValue(diagnosis)) {
        invalidIcdValueCounter++;
        String diagnosisCategory = diagnosis.split("\\.")[0];
        if (isValidIcdValue(diagnosisCategory)) {
          correctedIcdValueCounter++;
          diagnoses.put(diagnosis, diagnosisCategory);
        } else {
          discardedIcdValueCounter++;}
          diagnoses.put(diagnosis, null);
      }
      diagnosisCounter++;
    }

    logger.debug("collectDiagnosisCorrections: invalidIcdValueCounter: " + invalidIcdValueCounter + ", correctedIcdValueCounter: " + correctedIcdValueCounter + ", discardedIcdValueCounter: " + discardedIcdValueCounter);
    if (diagnoses.keySet().size() > 0 && diagnoses.keySet().size() < 5) {
      logger.debug("collectDiagnosisCorrections: corrected diagnoses: ");
      for (String diagnosis : diagnoses.keySet())
        logger.debug("collectDiagnosisCorrections: diagnosis: " + diagnosis);
    }
  }

  protected abstract boolean isValidIcdValue(String diagnosis);

  /**
   * Extracts the country code from a given BBMRI ID string. Works for both
   * biobank and collection IDs.
   *
   * @param id The BBMRI ID string from which to extract the country code.
   * @return The country code extracted from the BBMRI ID string.
   */
  protected String extractCountryCodeFromBbmriEricId(String id) {
    BbmriEricId bbmriEricCollectionId = BbmriEricId
            .valueOf(id)
            .orElse(null);
    String countryCode = bbmriEricCollectionId.getCountryCode();

    return countryCode;
  }

  /**
   * Removes keys from the given collection if the corresponding value is an empty list or a list with a single null element.
   *
   * @param entity The collection to clean.
   */
  protected void cleanEntity(Map<String, Object> entity) {
    List<String> badKeys = new ArrayList<String>(); // List of keys to remove>
    for (String key: entity.keySet()) {
      if (entity.get(key) instanceof List list) {
        if (list.size() == 0) {
          logger.debug("cleanEntity: attribute \"" + key + "\" is an empty list");
          badKeys.add(key);
        }
        if (list.size() == 1 && list.get(0) == null) {
          logger.debug("cleanEntity: attribute \"" + key + "\" has a single null element");
          badKeys.add(key);
        }
      }
    }
    for (String key: badKeys) {
      logger.debug("cleanEntity: removing bad attribute: \"" + key + "\"");
      entity.remove(key);
    }
  }

  /**
   * Inserts missing attributes into the collection based on the provided DirectoryCollectionPut object.
   *
   * @param directoryCollectionPut The object containing the missing attributes.
   * @param entity The collection to insert missing attributes into.
   */
  protected void insertMissingAttributesIntoEntity(DirectoryCollectionPut directoryCollectionPut, Map<String, Object> entity) {
    if (!entity.containsKey("country"))
      entity.put("country", directoryCollectionPut.getCountryCode());
    if (!entity.containsKey("timestamp"))
      entity.put("timestamp", LocalDateTime.now().toString());
    if (!entity.containsKey("national_node"))
      entity.put("national_node", directoryCollectionPut.getCountryCode());
    if (!entity.containsKey("biobank_label") && entity.containsKey("biobank") && entity.get("biobank") instanceof String)
      entity.put("biobank_label", entity.get("biobank"));
    if (!entity.containsKey("type")) {
      List<String> type = new ArrayList<>();
      type.add("SAMPLE");
      entity.put("type", type);
    }
    if (!entity.containsKey("data_categories")) {
      List<String> dataCategories = new ArrayList<>();
      dataCategories.add("BIOLOGICAL_SAMPLES");
      entity.put("data_categories", dataCategories);
    }
  }

  /**
   * Cleans the timestamp by removing any trailing non-numeric characters.
   * E.g. there might be a 'Z' at the end of the timestamp, which the GrapQL
   * API doesn't need.
   *
   * @param timestamp The timestamp string to be cleaned.
   * @return The cleaned timestamp string without any trailing non-numeric characters.
   */
  protected String cleanTimestamp(String timestamp) {
    // Use the String.matches method to check if the timestamp ends with a non-numeric character
    if (timestamp.matches(".*[^\\d]$")) {
      timestamp = timestamp.substring(0, timestamp.length() - 1);
      logger.debug("cleanTimestamp: corrected timestamp: " + timestamp);
    }

    return timestamp;
  }

  /**
   * Transforms the collection for the EMX2 format by transforming specific attributes.
   *
   * @param entity The collection to transform.
   */
  protected void transformEntityForEmx2(Map<String, Object> entity) {
    transformAttributeForEmx2(entity, "diagnosis_available", "name");
    transformAttributeForEmx2(entity, "data_categories", "name");
    transformAttributeForEmx2(entity, "storage_temperatures", "name");
    transformAttributeForEmx2(entity, "sex", "name");
    transformAttributeForEmx2(entity, "materials", "name");
    transformAttributeForEmx2(entity, "order_of_magnitude_donors", "name");
    transformAttributeForEmx2(entity, "order_of_magnitude", "name");
    transformAttributeForEmx2(entity, "country", "name");
    transformAttributeForEmx2(entity, "type", "name");
    transformAttributeForEmx2(entity, "data_categories", "name");
    transformAttributeForEmx2(entity, "national_node", "id");
    transformAttributeForEmx2(entity, "contact", "id");
    transformAttributeForEmx2(entity, "biobank", "id");
  }

  /**
   * Transforms a specific attribute of a collection for the EMX2 format by converting it into a specific structure.
   *
   * @param entity The collection containing the attribute to transform.
   * @param attributeName The name of the attribute to transform.
   * @param attributeElementName The name of the element within the attribute to transform.
   */
  protected void transformAttributeForEmx2(Map<String, Object> entity, String attributeName, String attributeElementName) {
    // Transform a single attribute
    if (entity.containsKey(attributeName)) {
      Object attribute = entity.get(attributeName);
      if (attribute instanceof List) {
        List<Object> attributeList = (List<Object>) attribute;
        List newAttributeList = new ArrayList();
        if (attributeList.size() > 0) {
          for (Object attributeElementValue : attributeList) {
            if (attributeElementValue instanceof String || attributeElementValue instanceof Integer) {
              Map<String, String> newAttributeValue = new HashMap();
              newAttributeValue.put(attributeElementName, attributeElementValue.toString());
              newAttributeList.add(newAttributeValue);
            } else if (attributeElementValue instanceof Map) {
              // Looks like this attribute has already been transformed, so keep it unchanged
              newAttributeList.add(attributeElementValue);
            } else {
              logger.warn("transformAttributeForEmx2: attribute \"" + attributeName + "\" is not a list or string, value: " + Util.jsonStringFomObject(attributeElementValue));
            }
          }
          entity.remove(attributeName); // Remove old attribute
          entity.put(attributeName, newAttributeList);
        }
      } else if (attribute instanceof String || attribute instanceof Integer) {
        Map<String,String> newAttributeValue = new HashMap();
        newAttributeValue.put(attributeElementName, attribute.toString());
        entity.remove(attributeName); // Remove old attribute
        entity.put(attributeName, newAttributeValue);
      } else {
        logger.warn("transformAttributeForEmx2: attribute \"" + attributeName + "\" is not a list or string");
      }
    }
  }
}
