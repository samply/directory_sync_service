package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.StarModelData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      logger.warn("isAvailable: failing availablity test because " + endpoint + " is not accessible");
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
   * Make API calls to the Directory to fill a DirectoryCollectionGet object containing attributes
   * for all of the collections listed in collectionIds. The countryCode is used solely for
   * constructing the URL for the API call.
   * 
   * @param countryCode E.g. "DE".
   * @param collectionIds IDs of the collections whose data will be harvested.
   * @return
   */
  public abstract DirectoryCollectionGet fetchCollectionGetOutcomes(String countryCode, List<String> collectionIds);

  /**
   * Send aggregated collection information to the Directory.
   *
   * @param directoryCollectionPut Summary information about one or more collections
   * @return an outcome, either successful or null
   */
  public abstract boolean updateEntities(DirectoryCollectionPut directoryCollectionPut);

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

      if (!updateFactTablesBlock(countryCode, factTablesBlock)) {
        logger.warn("updateStarModel: failed, block: " + i);
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
    String countryCode = starModelInputData.getCountryCode();

    try {
      for (String collectionId: starModelInputData.getInputCollectionIds()) {
        // Loop until no more facts are left in the Directory.
        // We need to do things this way, because the Directory implements paging
        // and a single pass may not get all facts.
        do {
          List<String> factIds = getNextPageOfFactIdsForCollection(countryCode, collectionId);

          if (factIds == null) {
            logger.warn("deleteStarModel: Problem getting facts for collection: " + collectionId);
            return false;
          }
          logger.debug("deleteStarModel: number of facts found: " + factIds.size());
          if (factIds.size() == 0) {
            // Terminate the do loop if there are no more facts left.
            break;
          }

          // Take the list of fact IDs and delete all of the corresponding facts
          // at the Directory.
          if (!deleteFactsByIds(countryCode, factIds)) {
            logger.warn("deleteStarModel: Problem deleting facts for collection: " + collectionId);
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
   * Retrieves a list of fact IDs from the Directory associated with a specific collection.
   *
   * @param countryCode The country code, e.g. DE.
   * @param collectionId The ID of the collection to retrieve fact IDs for.
   * @return A list of fact IDs for the specified collection, or null if there is an issue retrieving the data. An empty list indicates that there are no more facts left to be retrieved.
   */
  protected abstract List<String> getNextPageOfFactIdsForCollection(String countryCode, String collectionId);

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
   * @param diagnoses A string map containing diagnoses to be corrected.
   */
  public void collectDiagnosisCorrections(Map<String, String> diagnoses) {
    if (mockDirectory)
      // Don't do anything if we're in mock mode
      return;

    if (diagnoses.keySet().size() > 0 && diagnoses.keySet().size() < 5) {
      logger.debug("__________ collectDiagnosisCorrections: uncorrected diagnoses: ");
      for (String diagnosis : diagnoses.keySet())
        logger.debug("__________ collectDiagnosisCorrections: diagnosis: " + diagnosis);
    }

    int diagnosisCounter = 0; // for diagnostics only
    int invalidIcdValueCounter = 0;
    int correctedIcdValueCounter = 0;
    int discardedIcdValueCounter = 0;
    for (String diagnosis: diagnoses.keySet()) {
      if (diagnosisCounter%500 == 0)
        logger.debug("__________ collectDiagnosisCorrections: diagnosisCounter: " + diagnosisCounter + ", total diagnoses: " + diagnoses.size());
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

    logger.debug("__________ collectDiagnosisCorrections: invalidIcdValueCounter: " + invalidIcdValueCounter + ", correctedIcdValueCounter: " + correctedIcdValueCounter + ", discardedIcdValueCounter: " + discardedIcdValueCounter);
    if (diagnoses.keySet().size() > 0 && diagnoses.keySet().size() < 5) {
      logger.debug("__________ collectDiagnosisCorrections: corrected diagnoses: ");
      for (String diagnosis : diagnoses.keySet())
        logger.debug("__________ collectDiagnosisCorrections: diagnosis: " + diagnosis);
    }
  }

  protected abstract boolean isValidIcdValue(String diagnosis);
}
