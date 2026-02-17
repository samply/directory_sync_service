package de.samply.directory_sync_service.directory.rest;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.ConvertDirectoryCollectionGetToCollections;
import de.samply.directory_sync_service.directory.DirectoryApi;

import de.samply.directory_sync_service.model.Collections;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.converter.ConvertCollectionsToDirectoryCollectionPut;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The DirectoryApiRest class provides an interface for interacting with the Directory service.
 * This class allows for fetching and updating biobank and collection information, managing star models,
 * and performing various validation and correction operations.
 * <p>
 * It supports a mock mode for testing purposes, where no real Directory interactions are performed.
 * <p>
 * Most methods will first try using a URL containing the country code, and If that fails, they will
 * use a URL without country code. This is because the inclusion of the country code is necessary when
 * synchronizing with a national node Directory, but not when synchronizing with the central Directory.
 * Both of these cases can occur: larger countries (e.g. DE) tend to have their own national node
 * Directories, but smaller countries (e.g. CY) use the central Directory.
 */
public class DirectoryApiRest extends DirectoryApi {
  private final DirectoryCallsRest directoryCallsRest;
  private final DirectoryEndpointsRest directoryEndpointsRest;

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
    super(mockDirectory);
    this.directoryCallsRest = new DirectoryCallsRest(baseUrl, username, password);
    this.directoryEndpointsRest = new DirectoryEndpointsRest();
    directoryEndpoints = new DirectoryEndpointsRest();
  }

  /**
   * Log in to the Directory. You can log in as many times as you like.
   */
  public boolean login() {
    logger.debug("DirectoryApiRest.login: logging  in");

    if (mockDirectory) {
      logger.info("DirectoryApiRest.login: mocking login, will not actually contact the Directory");
      // Don't try logging in if we are mocking
      return true;
    }

    boolean success = directoryCallsRest.login();

    logger.debug("login(Rest): success: " + success);

    return success;
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

    Biobank biobank = fetchBiobank(id.getCountryCode(), id);
    if (biobank == null) {
      logger.info("fetchBiobank: biobank is null, trying URL without country code");
      biobank = fetchBiobank(null, id);
      if (biobank == null)
        logger.warn("fetchBiobank: No Biobank in Directory with id: " + id);
    }
    return biobank;
  }

  private Biobank fetchBiobank(String countryCode, BbmriEricId id) {
    return (Biobank) directoryCallsRest.get(directoryEndpointsRest.getBiobankEndpoint(countryCode) + "/" + id, Biobank.class);
  }

  /**
   * Make API calls to the Directory to fill a Collections object containing attributes
   * for all of the collections listed in collectionIds.
   *
   * @param collections
   */
  public void fetchBasicCollectionData(Collections collections) {
    if (mockDirectory) {
      // Dummy return if we're in mock mode
      return;
    }

    login();

    List<String> collectionIds = collections.getCollectionIds();
    //  The countryCode is used solely for constructing the URL for the API call.
    String countryCode = collections.getCountryCode();

    boolean warnFlag = false;
    for (String collectionId: collectionIds) {
      try {
        logger.debug("generateCollections(Rest): collectionId: " + collectionId);
        String commandUrl = directoryEndpointsRest.getCollectionEndpoint(countryCode) + "?q=id==%22" + collectionId  + "%22";
        logger.debug("generateCollections(Rest): commandUrl: " + commandUrl);
        Map singleDirectoryCollectionGet = (Map) directoryCallsRest.get(commandUrl, Map.class);
        if (singleDirectoryCollectionGet == null) {
          logger.info("generateCollections(Rest): singleDirectoryCollectionGet is null, trying URL without country code");
          commandUrl = directoryEndpointsRest.getCollectionEndpoint(null) + "?q=id==%22" + collectionId + "%22";
          singleDirectoryCollectionGet = (Map) directoryCallsRest.get(commandUrl, Map.class);
          if (singleDirectoryCollectionGet == null) {
            logger.warn("generateCollections(Rest): singleDirectoryCollectionGet is null, does the collection exist in the Directory: " + collectionId);
            warnFlag = true;
            continue;
          }
        }
        Map collectionMap = getItemZero(singleDirectoryCollectionGet); // assume that only one collection matches collectionId
        if (collectionMap == null) {
          logger.warn("generateCollections(Rest): entity get item is null, does the collection exist in the Directory: " + collectionId);
          warnFlag = true;
          continue;
        }
        ConvertDirectoryCollectionGetToCollections.addCollectionFromMap(collections, collectionId, collectionMap);
      } catch (Exception e) {
        logger.warn("generateCollections(Rest): Exception during collection import: " + Util.traceFromException(e));
        warnFlag = true;
      }
    }

    if (warnFlag && collections.isEmpty()) {
      logger.warn("generateCollections(Rest): No entities retrieved from Directory");
      return;
    }

    logger.debug("generateCollections(Rest): done");

  }

  private Map getItemZero(Map singleDirectoryCollectionGet) {
    if (singleDirectoryCollectionGet == null) {
      logger.warn("getItemZero: singleDirectoryCollectionGet is null");
      return null;
    }
    if (!singleDirectoryCollectionGet.containsKey("items")) {
      logger.warn("getItemZero: no items in singleDirectoryCollectionGet");
      return null;
    }
    if (!(singleDirectoryCollectionGet.get("items") instanceof List)) {
      logger.warn("getItemZero: items in singleDirectoryCollectionGet is not a list");
      return null;
    }
    List<Map> itemList;
    try {
      itemList = (List<Map>) singleDirectoryCollectionGet.get("items");
      if (itemList == null || itemList.size() == 0)
        return null;
    } catch (Exception e) {
      logger.warn("getItemZero: exception: " + Util.traceFromException(e));
      return null;
    }
    return itemList.get(0);
  }


  /**
   * Send aggregated collection information to the Directory.
   *
   * @param collections Summary information about one or more collections
   * @return an outcome, either successful or null
   */
  public boolean sendUpdatedCollections(Collections collections) {
    if (mockDirectory) {
      // Dummy return if we're in mock mode
      return true;
    }

    try {
      // Munge the Collection data into a form that can be turned into JSON and
      // sent to the Directory.
      DirectoryCollectionPut directoryCollectionPut = ConvertCollectionsToDirectoryCollectionPut.convert(collections);
      if (directoryCollectionPut == null) {
        logger.warn("sendUpdatedCollections: Problem converting FHIR attributes to Directory attributes");
        return false;
      }
      logger.debug("sendUpdatedCollections: 1 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

      login();

      String response = updateEntities(directoryCollectionPut.getCountryCode(), directoryCollectionPut);
      if (response == null) {
        logger.info("sendUpdatedCollections: PUT problem, trying URL without country code");
        response = updateEntities(null, directoryCollectionPut);
        if (response == null) {
          logger.warn("sendUpdatedCollections: PUT problem even without country code, aborting");
          return false;
        }
      }
    } catch (Exception e) {
      logger.warn("sendUpdatedCollections: Exception during collection update: " + Util.traceFromException(e));
      return false;
    }

    return true;
  }

  private String updateEntities(String countryCode, DirectoryCollectionPut directoryCollectionPut) {
    return directoryCallsRest.put(directoryEndpointsRest.getCollectionEndpoint(countryCode), directoryCollectionPut);
  }


  /**
   * Updates the fact tables block for a specific country with the provided data.
   *
   * @param countryCode The country code, e.g. DE.
   * @param factTablesBlock A list of maps representing the fact tables block data.
   * @return true if the update was successful, false otherwise.
   */
  @Override
  protected boolean updateFactTablesBlock(String countryCode, List<Map<String, String>> factTablesBlock) {
    if (factTablesBlock.size() == 0) {
      logger.debug("updateFactTablesBlock: factTablesBlock is empty, no need to contact the Directory");
      return true;
    }

    if (mockDirectory) {
      // Dummy return if we're in mock mode
      return true;
    }

    Map<String,Object> body = new HashMap<String,Object>();
    body.put("entities", factTablesBlock);

    String response = directoryCallsRest.post(directoryEndpointsRest.getFactEndpoint(countryCode), body);
    if (response == null) {
      logger.info("updateFactTablesBlock: null response from REST call, trying URL without country code");
      response = directoryCallsRest.post(directoryEndpointsRest.getFactEndpoint(null), body);
      if (response == null) {
        logger.warn("updateFactTablesBlock: null response from REST call even without country code, aborting");
        return false;
      }
    }

    logger.debug("updateFactTablesBlock: response: " + Util.jsonStringFomObject(response));

    return true;
  }

  /**
   * Retrieves a list of fact IDs from the Directory associated with a specific collection.
   *
   * @param collectionId The ID of the collection to retrieve fact IDs for.
   * @return A list of fact IDs for the specified collection, or null if there is an issue retrieving the data. An empty list indicates that there are no more facts left to be retrieved.
   */
  @Override
  protected List<String> getNextPageOfFactIdsForCollection(String collectionId) {
    // Get a list of fact IDs for this collection
    String apiUrl = directoryEndpointsRest.getFactEndpoint(extractCountryCodeFromBbmriEricId(collectionId));
    Map factWrapper = (Map) directoryCallsRest.get(apiUrl + "?q=collection==%22" + collectionId + "%22", Map.class);
    if (factWrapper == null) {
      logger.info("getNextPageOfFactIdsForCollection: Problem getting facts for collection, factWrapper == null, collectionId=" + collectionId + ", trying without country code");
      apiUrl = directoryEndpointsRest.getFactEndpoint(extractCountryCodeFromBbmriEricId(null));
      factWrapper = (Map) directoryCallsRest.get(apiUrl + "?q=collection==%22" + collectionId + "%22", Map.class);
      if (factWrapper == null) {
        logger.warn("getNextPageOfFactIdsForCollection: Problem getting facts for collection, factWrapper == null, collectionId=" + collectionId + " even without contry code, aborting");
        return null;
      }
    }

    if (!factWrapper.containsKey("items")) {
      logger.warn("getNextPageOfFactIdsForCollection: Problem getting facts for collection, no item key present: " + collectionId);
      return null;
    }
    if (!(factWrapper.get("items") instanceof List<?>)) {
      logger.warn("getNextPageOfFactIdsForCollection: items is not a List: " + Util.jsonStringFomObject(factWrapper.get("items")));
      return null;
    }
    List<Map<String, String>> facts = (List<Map<String, String>>) factWrapper.get("items");
    if (facts.size() == 0)
      // No more facts left
      return new ArrayList<String>();

    return facts.stream()
            .map(map -> map.get("id"))
            .collect(Collectors.toList());
  }

  /**
   * Deletes facts from the Directory service based on a list of fact IDs.
   *
   * @param countryCode    e.g. DE.
   * @param factIds   The list of fact IDs to be deleted.
   * @return An OperationOutcome indicating the success or failure of the deletion.
   */
  @Override
  protected boolean deleteFactsByIds(String countryCode, List<String> factIds) {
    if (factIds.size() == 0)
      // Nothing to delete
      return true;

    if (mockDirectory) {
      // Dummy return if we're in mock mode
      return true;
    }

    // Directory likes to have its delete data wrapped in a map with key "entityIds".
    HashMap<String, List<String>> factIdMap = new HashMap<>(Map.of("entityIds", factIds));
    logger.debug("deleteFactsByIds, the following IDs will be deleted: " + Util.jsonStringFomObject(factIdMap));
    String result = directoryCallsRest.delete(directoryEndpointsRest.getFactEndpoint(countryCode), factIdMap);
    if (result == null) {
      logger.info("deleteFactsByIds, Problem during delete of factIds, trying without country code");
      result = directoryCallsRest.delete(directoryEndpointsRest.getFactEndpoint(null), factIdMap);
      if (result == null) {
        logger.warn("deleteFactsByIds, Problem during delete of factIds even without contry code, aborting");
        return false;
      }
    }

    return true;
  }

  /**
   * Checks if a given diagnosis code is a valid ICD value by querying the Directory service.
   *
   * @param diagnosis The diagnosis code to be validated.
   * @return true if the diagnosis code is a valid ICD value, false if not, or if an error condition was encountered.
   */
  public boolean isValidIcdValue(String diagnosis) {
    if (mockDirectory)
      return true;

    String url = directoryEndpointsRest.getDiseaseTypeEndpoint() + "?q=id=='" + diagnosis + "'";
    Map body = (Map) directoryCallsRest.get(url, Map.class);
    if (body != null) {
      if (body.containsKey("total")) {
        Object total = body.get("total");
        if (total instanceof Double) {
          Integer intTotal = ((Double) total).intValue();
          return intTotal > 0;
        } else
          logger.warn("isValidIcdValue: key 'total' is not a double");
      } else
        logger.warn("isValidIcdValue: key 'total' is not present");
    } else
      logger.warn("isValidIcdValue: get response is null");

    return false;
  }
}
