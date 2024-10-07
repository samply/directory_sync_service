package de.samply.directory_sync_service.directory;

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
   * @return true if this API is accessible, false otherwise.
   */
  public boolean isAvailable() {
    List<String> endpoints = directoryEndpoints.getAllEndpoints();

    // Simply loop over all known endpoints for this API and check if they exist
    boolean available = true;
    for (String endpoint: endpoints)
      if (!directoryCalls.endpointExists(endpoint)) {
        logger.warn("isAvailable: failing availablity test because " + endpoint + " is not accessible");
        available = false;
      }

    if (available)
      logger.info("isAvailable: all availability tests have succeeded");

    return available;
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
  public abstract boolean updateStarModel(StarModelData starModelInputData);

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

  protected abstract boolean isValidIcdValue(String diagnosis);
}
