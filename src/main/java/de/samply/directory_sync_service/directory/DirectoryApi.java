package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.model.StarModelData;
import de.samply.directory_sync_service.Util;

import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.INFORMATION;

import de.samply.directory_sync_service.directory.model.BbmriEricId;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import io.vavr.control.Either;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.impl.client.CloseableHttpClient;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryApi {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryApi.class);
  private DirectoryRest directoryRest;

  // Setting this variable to true will prevent any contact being made to the Directory.
  // All public methods will return feasible fake results.
  private boolean mockDirectory = false;

  public DirectoryApi(CloseableHttpClient httpClient, String baseUrl, boolean mockDirectory, String username, String password) {
    this.mockDirectory = mockDirectory;
    this.directoryRest = new DirectoryRest(httpClient, baseUrl, username, password);
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

  private static OperationOutcome updateSuccessful(int number) {
    OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue()
        .setSeverity(INFORMATION)
        .setDiagnostics(String.format("Successful update of %d collection size values.", number));
    return outcome;
  }

  /**
   * Fetches the Biobank with the given {@code id}.
   *
   * @param id the ID of the Biobank to fetch.
   * @return either the Biobank or an error
   */
  public Either<OperationOutcome, Biobank> fetchBiobank(BbmriEricId id) {
    Biobank biobank = (Biobank) directoryRest.get("/api/v2/eu_bbmri_eric_" + id.getCountryCode() + "_biobanks/" + id, Biobank.class);
    if (biobank == null)
      return Either.left(DirectoryUtils.error(id.toString(), "No Biobank in Directory with id: " + id));
    return Either.right(biobank);
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
  public Either<OperationOutcome, DirectoryCollectionGet> fetchCollectionGetOutcomes(String countryCode, List<String> collectionIds) {
    DirectoryCollectionGet directoryCollectionGet = new DirectoryCollectionGet(); // for all collections retrieved from Directory
    directoryCollectionGet.init();

    if (mockDirectory) {
      // Dummy return if we're in mock mode
      directoryCollectionGet.setMockDirectory(true);
      return Either.right(directoryCollectionGet);
    }

    for (String collectionId: collectionIds) {
      DirectoryCollectionGet singleDirectoryCollectionGet = (DirectoryCollectionGet) directoryRest.get(buildCollectionApiUrl(countryCode) + "?q=id==%22" + collectionId  + "%22", DirectoryCollectionGet.class);
      if (singleDirectoryCollectionGet == null)
        return Either.left(DirectoryUtils.error("fetchCollectionGetOutcomes: singleDirectoryCollectionGet is null, does the collection exist in the Directory: ", collectionId));
      Map item = singleDirectoryCollectionGet.getItemZero(); // assume that only one collection matches collectionId
      if (item == null)
        return Either.left(DirectoryUtils.error("fetchCollectionGetOutcomes: entity get item is null, does the collection exist in the Directory: ", collectionId));
      directoryCollectionGet.getItems().add(item);
    }

    return Either.right(directoryCollectionGet);
  }

  /**
   * Send aggregated collection information to the Directory.
   *
   * @param directoryCollectionPut Summary information about one or more collections
   * @return an outcome, either successful or an error
   */
  public OperationOutcome updateEntities(DirectoryCollectionPut directoryCollectionPut) {
    logger.info("DirectoryApi.updateEntities: entered");

    if (mockDirectory)
      // Dummy return if we're in mock mode
      return updateSuccessful(directoryCollectionPut.size());

    String response = directoryRest.put(buildCollectionApiUrl(directoryCollectionPut.getCountryCode()), directoryCollectionPut);
    if (response == null)
      return DirectoryUtils.error("entity update", "PUT problem");
    return updateSuccessful(directoryCollectionPut.size());
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
      return updateSuccessful(starModelInputData.getFactCount());

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
        return DirectoryUtils.error("updateStarModel", "failed, block: " + i);
    }

    return updateSuccessful(starModelInputData.getFactCount());
  }

  /**
   * Deletes existing star models from the Directory service for each of the collection IDs in the supplied StarModelInputData object.
   *
   * @param starModelInputData The input data for deleting existing star models.
   * @return An OperationOutcome indicating the success or failure of the deletion.
   */
  private OperationOutcome deleteStarModel(StarModelData starModelInputData) {
    if (mockDirectory)
      // Dummy return if we're in mock mode
      return new OperationOutcome();

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
            return DirectoryUtils.error("deleteStarModel: Problem getting facts for collection, factWrapper == null, collectionId=", collectionId);
          if (!factWrapper.containsKey("items"))
            return DirectoryUtils.error("deleteStarModel: Problem getting facts for collection, no item key present: ", collectionId);
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
      return DirectoryUtils.error("deleteStarModel: Exception during delete", Util.traceFromException(e));
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
  public OperationOutcome deleteFactsByIds(String apiUrl, List<String> factIds) {
    if (factIds.size() == 0)
      // Nothing to delete
      return new OperationOutcome();

    String result = directoryRest.delete(apiUrl, factIds);

    if (result == null)
      return DirectoryUtils.error("deleteFactsByIds", "Problem during delete of factIds");

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
