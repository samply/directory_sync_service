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
 * The DirectoryApi class provides an interface for interacting with the Directory service.
 * This class allows for fetching and updating biobank and collection information, managing star models,
 * and performing various validation and correction operations.
 * It supports a mock mode for testing purposes, where no real Directory interactions are performed.
 */
public class DirectoryApi {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryApi.class);
  private DirectoryRest directoryRest;

  // Setting this variable to true will prevent any contact being made to the Directory.
  // All public methods will return feasible fake results.
  private boolean mockDirectory = false;

  /**
   * Constructs a new DirectoryApi instance.
   * If we are not in mocking mode, log in to the Directory.
   *
   * @param baseUrl The base URL of the Directory service.
   * @param mockDirectory If true, the instance operates in mock mode, returning fake data.
   * @param username The username for authenticating with the Directory.
   * @param password The password for authenticating with the Directory.
   */
  public DirectoryApi(String baseUrl, boolean mockDirectory, String username, String password) {
    this.mockDirectory = mockDirectory;
    this.directoryRest = new DirectoryRest(baseUrl, username, password);
    if (!mockDirectory)
      // Log in if we are not in mock mode
      this.directoryRest.login();
  }

  /**
   * Log back in to the Directory. This is typically used in situations where there has
   * been a long pause since the last API call to the Directory. It returns a fresh
   * DirectoryApi object, which you should use to replace the existing one.
   * <p>
   * Returns null if something goes wrong.
   *
   * @return new DirectoryApi object.
   */
  public void relogin() {
    logger.info("login: logging back in");

    if (mockDirectory)
      // Don't try logging in if we are mocking
      return;

    directoryRest.login();
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

    Biobank biobank = (Biobank) directoryRest.get(buildBiobankApiUrl(id.getCountryCode()) + "/" + id, Biobank.class);
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
      DirectoryCollectionGet singleDirectoryCollectionGet = (DirectoryCollectionGet) directoryRest.get(buildCollectionApiUrl(countryCode) + "?q=id==%22" + collectionId  + "%22", DirectoryCollectionGet.class);
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
  public OperationOutcome updateEntities(DirectoryCollectionPut directoryCollectionPut) {
    if (mockDirectory)
      // Dummy return if we're in mock mode
      return DirectoryUtils.success("DirectoryApi.updateEntities: in mock mode, skip update");

    String response = directoryRest.put(buildCollectionApiUrl(directoryCollectionPut.getCountryCode()), directoryCollectionPut);
    if (response == null)
      return DirectoryUtils.error("entity update, PUT problem");

    return DirectoryUtils.success("DirectoryApi.updateEntities: successfully put " + directoryCollectionPut.size() + " collections to the Directory");
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
  public OperationOutcome updateStarModel(StarModelData starModelInputData) {
    if (mockDirectory)
      // Dummy return if we're in mock mode
      return DirectoryUtils.success("DirectoryApi.updateStarModel: in mock mode, skip update");

    // Get rid of previous star models first. This is necessary, because:
    // 1. A new star model may be decomposed into different hypercubes.
    // 2. The new fact IDs may be different from the old ones.
    // 3. We will be using a POST and it will return an error if we try
    //    to overwrite an existing fact.
    OperationOutcome deleteOutcome = deleteStarModel(starModelInputData);
    if (deleteOutcome.getIssue().size() > 0) {
      logger.warn("updateStarModel: Problem deleting star models");
      return deleteOutcome;
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
      String response = directoryRest.post(buildApiUrl(countryCode, "facts"), body);
      if (response == null)
        return DirectoryUtils.error("updateStarModel, failed, block: " + i);
    }

    return DirectoryUtils.success("DirectoryApi.updateStarModel: successfully posted " + starModelInputData.getFactCount() + " facts to the Directory");
  }

  /**
   * Deletes existing star models from the Directory service for each of the collection IDs in the supplied StarModelInputData object.
   *
   * @param starModelInputData The input data for deleting existing star models.
   * @return An OperationOutcome indicating the success or failure of the deletion.
   */
  private OperationOutcome deleteStarModel(StarModelData starModelInputData) {
    String apiUrl = buildApiUrl(starModelInputData.getCountryCode(), "facts");

    try {
      for (String collectionId: starModelInputData.getInputCollectionIds()) {
        List<String> factIds;
        // Loop until no more facts are left in the Directory.
        // We need to do things this way, because the Directory implements paging
        // and a single pass may not get all facts.
        do {
          // First get a list of fact IDs for this collection
          Map factWrapper = (Map) directoryRest.get(apiUrl + "?q=collection==%22" + collectionId + "%22", Map.class);

          if (factWrapper == null)
            return DirectoryUtils.error("deleteStarModel: Problem getting facts for collection, factWrapper == null, collectionId=" + collectionId);
          if (!factWrapper.containsKey("items"))
            return DirectoryUtils.error("deleteStarModel: Problem getting facts for collection, no item key present: " + collectionId);
          List<Map<String, String>> facts = (List<Map<String, String>>) factWrapper.get("items");
          if (facts.size() == 0)
            break;
          factIds = facts.stream()
            .map(map -> map.get("id"))
            .collect(Collectors.toList());

          // Take the list of fact IDs and delete all of the corresponding facts
          // at the Directory.
          OperationOutcome deleteOutcome = deleteFactsByIds(apiUrl, factIds);
          if (deleteOutcome.getIssue().size() > 0)
            return deleteOutcome;
        } while (true);
      }
    } catch(Exception e) {
      return DirectoryUtils.error("deleteStarModel: Exception during delete: " + Util.traceFromException(e));
    }

    return new OperationOutcome();
  }

  /**
   * Deletes facts from the Directory service based on a list of fact IDs.
   *
   * @param apiUrl    The base URL for the Directory API.
   * @param factIds   The list of fact IDs to be deleted.
   * @return An OperationOutcome indicating the success or failure of the deletion.
   */
  private OperationOutcome deleteFactsByIds(String apiUrl, List<String> factIds) {
    if (factIds.size() == 0)
      // Nothing to delete
      return new OperationOutcome();

    String result = directoryRest.delete(apiUrl, factIds);

    if (result == null)
      return DirectoryUtils.error("deleteFactsByIds, Problem during delete of factIds");

    return new OperationOutcome();
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
    String url = "/api/v2/eu_bbmri_eric_disease_types?q=id=='" + diagnosis + "'";
    Map body = (Map) directoryRest.get(url, Map.class);
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

  /**
   * Constructs the URL for accessing the biobank endpoint of the Directory API based on the country code.
   *
   * @param countryCode The country code (e.g., "DE").
   * @return the constructed biobank API URL.
   */
  private String buildBiobankApiUrl(String countryCode) {
    return buildApiUrl(countryCode, "biobanks");
  }

  /**
   * Constructs the URL for accessing the collection endpoint of the Directory API based on the country code.
   *
   * @param countryCode The country code (e.g., "DE").
   * @return the constructed collection API URL.
   */
  private String buildCollectionApiUrl(String countryCode) {
    return buildApiUrl(countryCode, "collections");
  }

  /**
   * Create a URL for a specific Directory API endpoint.
   * 
   * @param countryCode a code such as "DE" specifying the country the URL should address. May be null.
   * @param function specifies the type of the endpoint, e.g. "collections".
   * @return
   */
  private String buildApiUrl(String countryCode, String function) {
    String countryCodeInsert = "";
    if (countryCode != null && !countryCode.isEmpty())
      countryCodeInsert = countryCode + "_";
    String collectionApiUrl = "/api/v2/eu_bbmri_eric_" + countryCodeInsert + function;

    return collectionApiUrl;
  }
}
