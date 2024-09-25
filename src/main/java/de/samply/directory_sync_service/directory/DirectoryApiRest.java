package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.model.StarModelData;
import de.samply.directory_sync_service.Util;

import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hl7.fhir.r4.model.OperationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The DirectoryApiRest class provides an interface for interacting with the Directory service.
 * This class allows for fetching and updating biobank and collection information, managing star models,
 * and performing various validation and correction operations.
 * It supports a mock mode for testing purposes, where no real Directory interactions are performed.
 */
public class DirectoryApiRest extends DirectoryApi {
  private DirectoryRestCalls directoryRestCalls;

  /**
   * Constructs a new DirectoryApiRest instance.
   * If we are not in mocking mode, log in to the Directory.
   *
   * @param baseUrl The base URL of the Directory service.
   * @param mockDirectory If true, the instance operates in mock mode, returning fake data.
   * @param username The username for authenticating with the Directory.
   * @param password The password for authenticating with the Directory.
   */
  public DirectoryApiRest(String baseUrl, boolean mockDirectory, String username, String password) {
    super(baseUrl, mockDirectory, username, password);
    this.directoryRestCalls = new DirectoryRestCalls(baseUrl, username, password);
  }

  /**
   * @return true if this API is accessible, false otherwise.
   */
  public boolean isAvailable() {
    List<String> endpoints = DirectoryRestEndpoints.getAllEndpoints();

    for (String endpoint: endpoints)
      if (!directoryRestCalls.endpointExists(endpoint)) {
        logger.warn("isAvailable: failing availablity test because " + endpoint + " is not accessible");
        return false;
      }

    logger.info("isAvailable: all availability tests have succeeded");

    return true;
  }

  /**
   * Log in to the Directory. You can log in as many times as you like.
   */
  public boolean login() {
    logger.info("login: logging  in");

    if (mockDirectory)
      // Don't try logging in if we are mocking
      return true;

    return directoryRestCalls.login();
  }

  /**
   * Fetches the Biobank with the given {@code id}.
   *
   * @param id the ID of the Biobank to fetch.
   * @return either the Biobank or null if an error occurs
   */
  public Biobank fetchBiobank(BbmriEricId id) {
    if (mockDirectory)
      // Return a fake Biobank if we are mocking
      return new Biobank();

    Biobank biobank = (Biobank) directoryRestCalls.get(DirectoryRestEndpoints.getBiobankEndpoint(id.getCountryCode()) + "/" + id, Biobank.class);
    if (biobank == null) {
      logger.warn("fetchBiobank: No Biobank in Directory with id: " + id);
      return null;
    }
    return biobank;
  }

  /**
   * Make API calls to the Directory to fill a DirectoryCollectionGet object containing attributes
   * for all of the collections listed in collectionIds. The countryCode is used solely for
   * constructing the URL for the API call.
   * 
   * @param countryCode E.g. "DE".
   * @param collectionIds IDs of the collections whose data will be harvested.
   * @return
   */
  public DirectoryCollectionGet fetchCollectionGetOutcomes(String countryCode, List<String> collectionIds) {
    DirectoryCollectionGet directoryCollectionGet = new DirectoryCollectionGet(); // for all collections retrieved from Directory
    directoryCollectionGet.init();

    if (mockDirectory) {
      // Dummy return if we're in mock mode
      directoryCollectionGet.setMockDirectory(true);
      return directoryCollectionGet;
    }

    for (String collectionId: collectionIds) {
      DirectoryCollectionGet singleDirectoryCollectionGet = (DirectoryCollectionGet) directoryRestCalls.get(DirectoryRestEndpoints.getCollectionEndpoint(countryCode) + "?q=id==%22" + collectionId  + "%22", DirectoryCollectionGet.class);
      if (singleDirectoryCollectionGet == null) {
        logger.warn("fetchCollectionGetOutcomes: singleDirectoryCollectionGet is null, does the collection exist in the Directory: " + collectionId);
        return null;
      }
      Map item = singleDirectoryCollectionGet.getItemZero(); // assume that only one collection matches collectionId
      if (item == null) {
        logger.warn("fetchCollectionGetOutcomes: entity get item is null, does the collection exist in the Directory: " + collectionId);
        return null;
      }
      directoryCollectionGet.getItems().add(item);
    }

    return directoryCollectionGet;
  }

  /**
   * Send aggregated collection information to the Directory.
   *
   * @param directoryCollectionPut Summary information about one or more collections
   * @return an outcome, either successful or null
   */
  public boolean updateEntities(DirectoryCollectionPut directoryCollectionPut) {
    if (mockDirectory) {
      // Dummy return if we're in mock mode
      logger.info("DirectoryApiRest.updateEntities: in mock mode, skip update");
      return true;
    }

    String response = directoryRestCalls.put(DirectoryRestEndpoints.getCollectionEndpoint(directoryCollectionPut.getCountryCode()), directoryCollectionPut);
    if (response == null) {
      logger.warn("entity update, PUT problem");
      return false;
    }

    logger.info("DirectoryApiRest.updateEntities: successfully put " + directoryCollectionPut.size() + " collections to the Directory");

    return true;
  }

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
      logger.info("DirectoryApiRest.updateStarModel: in mock mode, skip update");
      return true;
    }

    // Get rid of previous star models first. This is necessary, because:
    // 1. A new star model may be decomposed into different hypercubes.
    // 2. The new fact IDs may be different from the old ones.
    // 3. We will be using a POST and it will return an error if we try
    //    to overwrite an existing fact.
    if (!deleteStarModel(starModelInputData)) {
      logger.warn("updateStarModel: Problem deleting star models");
      return false;
    }

    String countryCode = starModelInputData.getCountryCode();
    List<Map<String, String>> factTables = starModelInputData.getFactTables();
    int blockSize = 1000;

    // Break the fact table into blocks of 1000 before sending to the Directory.
    // This is the maximum number of facts allowed per Directory API call.
    for (int i = 0; i < factTables.size(); i += blockSize) {
      List<Map<String, String>> factTablesBlock = factTables.subList(i, Math.min(i + blockSize, factTables.size()));

      Map<String,Object> body = new HashMap<String,Object>();
      body.put("entities", factTablesBlock);
      String response = directoryRestCalls.post(DirectoryRestEndpoints.getFactEndpoint(countryCode), body);
      if (response == null) {
        logger.warn("updateStarModel, failed, block: " + i);
        return false;
      }
    }

    logger.info("DirectoryApiRest.updateStarModel: successfully posted " + starModelInputData.getFactCount() + " facts to the Directory");

    return true;
  }

  /**
   * Deletes existing star models from the Directory service for each of the collection IDs in the supplied StarModelInputData object.
   *
   * @param starModelInputData The input data for deleting existing star models.
   * @return An boolean indicating the success or failure of the deletion.
   */
  private boolean deleteStarModel(StarModelData starModelInputData) {
    String apiUrl = DirectoryRestEndpoints.getFactEndpoint(starModelInputData.getCountryCode());

    try {
      for (String collectionId: starModelInputData.getInputCollectionIds()) {
        List<String> factIds;
        // Loop until no more facts are left in the Directory.
        // We need to do things this way, because the Directory implements paging
        // and a single pass may not get all facts.
        do {
          // First get a list of fact IDs for this collection
          Map factWrapper = (Map) directoryRestCalls.get(apiUrl + "?q=collection==%22" + collectionId + "%22", Map.class);

          if (factWrapper == null) {
            logger.warn("deleteStarModel: Problem getting facts for collection, factWrapper == null, collectionId=" + collectionId);
            return false;
          }
          if (!factWrapper.containsKey("items")) {
            logger.warn("deleteStarModel: Problem getting facts for collection, no item key present: " + collectionId);
            return false;
          }
          List<Map<String, String>> facts = (List<Map<String, String>>) factWrapper.get("items");
          if (facts.size() == 0)
            break;
          factIds = facts.stream()
            .map(map -> map.get("id"))
            .collect(Collectors.toList());

          // Take the list of fact IDs and delete all of the corresponding facts
          // at the Directory.
          if (!deleteFactsByIds(apiUrl, factIds)) {
            logger.info("deleteStarModel: Problem deleting facts for collection: " + collectionId);
            return false;
          }
        } while (true);
      }
    } catch(Exception e) {
      logger.warn("deleteStarModel: Exception during delete: " + Util.traceFromException(e));
      return false;
    }

    return true;
  }

  /**
   * Deletes facts from the Directory service based on a list of fact IDs.
   *
   * @param apiUrl    The base URL for the Directory API.
   * @param factIds   The list of fact IDs to be deleted.
   * @return An OperationOutcome indicating the success or failure of the deletion.
   */
  private boolean deleteFactsByIds(String apiUrl, List<String> factIds) {
    if (factIds.size() == 0)
      // Nothing to delete
      return true;

    String result = directoryRestCalls.delete(apiUrl, factIds);

    if (result == null) {
      logger.warn("deleteFactsByIds, Problem during delete of factIds");
      return false;
    }

    return true;
  }

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
   * @param diagnoses A string map containing diagnoses to be corrected.
   */
  public void collectDiagnosisCorrections(Map<String, String> diagnoses) {
    if (mockDirectory)
      // Don't do anything if we're in mock mode
      return;

    int diagnosisCounter = 0; // for diagnostics only
    int invalidIcdValueCounter = 0;
    int correctedIcdValueCounter = 0;
    for (String diagnosis: diagnoses.keySet()) {
      if (diagnosisCounter%500 == 0)
        logger.info("__________ collectDiagnosisCorrections: diagnosisCounter: " + diagnosisCounter + ", total diagnoses: " + diagnoses.size());
      if (!isValidIcdValue(diagnosis)) {
        invalidIcdValueCounter++;
        String diagnosisCategory = diagnosis.split("\\.")[0];
        if (isValidIcdValue(diagnosisCategory)) {
          correctedIcdValueCounter++;
          diagnoses.put(diagnosis, diagnosisCategory);
        } else
          diagnoses.put(diagnosis, null);
      }
      diagnosisCounter++;
    }

    logger.info("__________ collectDiagnosisCorrections: invalidIcdValueCounter: " + invalidIcdValueCounter + ", correctedIcdValueCounter: " + correctedIcdValueCounter);
  }

  /**
   * Checks if a given diagnosis code is a valid ICD value by querying the Directory service.
   *
   * @param diagnosis The diagnosis code to be validated.
   * @return true if the diagnosis code is a valid ICD value, false if not, or if an error condition was encountered.
   */
  private boolean isValidIcdValue(String diagnosis) {
    String url = DirectoryRestEndpoints.getDiseaseTypeEndpoint() + "?q=id=='" + diagnosis + "'";
    Map body = (Map) directoryRestCalls.get(url, Map.class);
    if (body != null) {
      if (body.containsKey("total")) {
        Object total = body.get("total");
        if (total instanceof Double) {
          Integer intTotal = ((Double) total).intValue();
          if (intTotal > 0)
            return true;
        } else
          logger.warn("isValidIcdValue: key 'total' is not a double");
      } else
        logger.warn("isValidIcdValue: key 'total' is not present");
    } else
      logger.warn("isValidIcdValue: get response is null");

    return false;
  }
}
