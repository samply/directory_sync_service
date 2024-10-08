package de.samply.directory_sync_service.directory.graphql;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.StarModelData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The DirectoryApiRest class provides an interface for interacting with the Directory service.
 * This class allows for fetching and updating biobank and collection information, managing star models,
 * and performing various validation and correction operations.
 * It supports a mock mode for testing purposes, where no real Directory interactions are performed.
 */
public class DirectoryApiGraphql extends DirectoryApi {
  private final String username;
  private final String password;
  private DirectoryCallsGraphql directoryCallsGraphql;

  /**
   * Constructs a new DirectoryApiRest instance.
   * If we are not in mocking mode, log in to the Directory.
   *
   * @param baseUrl The base URL of the Directory service.
   * @param mockDirectory If true, the instance operates in mock mode, returning fake data.
   * @param username The username for authenticating with the Directory.
   * @param password The password for authenticating with the Directory.
   */
  public DirectoryApiGraphql(String baseUrl, boolean mockDirectory, String username, String password) {
    super(baseUrl, mockDirectory, username, password);
    this.directoryCallsGraphql = new DirectoryCallsGraphql(baseUrl, username, password);
    directoryCalls = directoryCallsGraphql; // Used in superclass
    directoryEndpoints = new DirectoryEndpointsGraphql();
    this.username = username;
    this.password = password;
  }

  /**
   * @return true if this API is accessible, false otherwise.
   */
  public boolean isAvailable() {
    if (!super.isAvailable()) {
      logger.warn("DirectoryApiGraphql.isAvailable: failing availablity test because one or more GraphQL endpoints are not there");
      return false;
    }

    List<String> endpoints = directoryEndpoints.getAllEndpoints();

    // Loop over all known endpoints for this API and check to see if they report errors
    boolean available = true;
    for (String endpoint: endpoints)
      if (!directoryCallsGraphql.endpointIsValidGraphql(endpoint)) {
        logger.warn("DirectoryApiGraphql.isAvailable: failing availablity test because " + endpoint + " returns an error");
        available = false;
      }

    if (available)
      logger.info("DirectoryApiGraphql.isAvailable: all availability tests have succeeded");

    return available;
  }

  /**
   * Log in to the Directory. You can log in as many times as you like.
   */
  public boolean login() {
    logger.info("login: entered");

    if (mockDirectory)
      // Don't try logging in if we are mocking
      return true;

    String grapqlCommand = "mutation {\n" +
            "  signin(password: \"" + password + "\", email: \"" + username + "\") {\n" +
            "    message\n" +
            "    token\n" +
            "  }\n" +
            "}";

    JsonObject result = directoryCallsGraphql.runGraphqlCommand(grapqlCommand);

    if (result == null) {
      logger.warn("login: result is null");
      return false;
    }

    logger.info("login: result: " + result);

    String token = result.get("signin").getAsJsonObject().get("token").getAsString();
    if (token == null) {
      logger.warn("login: token is null");
      return false;
    }

    directoryCallsGraphql.setToken(token);

    return true;
  }

  /**
   * Fetches the Biobank with the given {@code id}.
   *
   * @param id the ID of the Biobank to fetch.
   * @return either the Biobank or null if an error occurs
   */
  public Biobank fetchBiobank(BbmriEricId id) {
    logger.info("fetchBiobank: entered");

    Biobank biobank = new Biobank();

    if (mockDirectory)
      // Return a fake Biobank if we are mocking
      return biobank;

    try {
      String grapqlCommand = "query {\n" +
              "  Biobanks( filter: { id: { equals: \"" + id.toString() + "\" } } ) {\n" +
              "    id\n" +
              "    name\n" +
              "  }\n" +
              "}";

      JsonObject result = directoryCallsGraphql.runGraphqlCommand(DirectoryEndpointsGraphql.getDatabaseEricEndpoint(), grapqlCommand);

      if (result == null) {
        logger.warn("fetchBiobank: result is null");
        return null;
      }

      logger.info("fetchBiobank: result: " + result);

      Map<String, Object> biobanks = directoryCallsGraphql.convertJsonObjectToMap(result);

      if (!biobanks.containsKey("Biobanks")) {
        logger.warn("fetchBiobank: no Biobanks element found, skipping");
        return null;
      }

      List<Map<String, Object>> biobankList = (List<Map<String, Object>>) biobanks.get("Biobanks");
      if (biobankList == null || biobankList.size() == 0) {
        logger.warn("fetchBiobank: Collections list is null or empty, skipping");
        return null;
      }

      Map<String, Object> item = biobankList.get(0);

      if (item == null) {
        logger.warn("fetchBiobank: first element of biobankList is null");
        return null;
      }

      if (!item.containsKey("id")) {
        logger.warn("fetchBiobank: no id element found in item: " + Util.jsonStringFomObject(item));
        return null;
      }

      String biobankId = (String) item.get("id");

      if (!id.toString().equals(biobankId)) {
        logger.warn("fetchBiobank: id in item: " + biobankId + " does not match id: " + id);
        return null;
      }

      if (!item.containsKey("name")) {
        logger.warn("fetchBiobank: no name element found in item: " + Util.jsonStringFomObject(item));
        return null;
      }

      String name = (String) item.get("name");

      biobank.setId(biobankId);
      biobank.setName(name);
    } catch (Exception e) {
      logger.warn("fetchBiobank: Exception during biobank import: " + Util.traceFromException(e));
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
      logger.info("fetchCollectionGetOutcomes: collectionId: " + collectionId);
      String grapqlCommand = "query {" +
              "  Collections( filter: { id: { equals: \"" + collectionId + "\" } } ) {\n" +
              "    id\n" +
              "    name\n" +
              "    description\n" +
              "    country {\n" +
              "      label\n" +
              "      name\n" +
              "    }\n" +
              "    contact {" +
              "      id\n" +
              "      email\n" +
              "    }\n" +
              "    biobank {\n" +
              "      id\n" +
              "      name\n" +
              "    }\n" +
              "    type {\n" +
              "      name\n" +
              "      label\n" +
              "    }\n" +
              "    data_categories {\n" +
              "      name\n" +
              "      label\n" +
              "    }\n" +
              "    order_of_magnitude {\n" +
              "      name\n" +
              "      label\n" +
              "    }\n" +
              "  }\n" +
              "}";

      JsonObject result = directoryCallsGraphql.runGraphqlCommand(DirectoryEndpointsGraphql.getDatabaseEricEndpoint(), grapqlCommand);

      logger.info("fetchCollectionGetOutcomes: result: " + result);

      if (result == null) {
        logger.warn("fetchBiobank: result is null");
        return null;
      }

      Map<String, Object> collections = directoryCallsGraphql.convertJsonObjectToMap(result);

      if (!collections.containsKey("Collections")) {
        logger.warn("fetchCollectionGetOutcomes: no Collections element found, skipping");
        continue;
      }

      List<Map<String, Object>> collectionsList = (List<Map<String, Object>>) collections.get("Collections");
      if (collectionsList == null || collectionsList.size() == 0) {
        logger.warn("fetchCollectionGetOutcomes: Collections list is null or empty, skipping");
        continue;
      }

      Map<String, Object> item = collectionsList.get(0);

      if (item == null) {
        logger.warn("fetchCollectionGetOutcomes: entity get item is null, does the collection exist in the Directory: " + collectionId);
        continue;
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
    logger.info("DirectoryApiRest.updateEntities: :::::::::::::::::::: entered");

    if (mockDirectory) {
      // Dummy return if we're in mock mode
      logger.info("DirectoryApiRest.updateEntities: :::::::::::::::::::: in mock mode, skip update");
      return true;
    }

    for (String collectionId: directoryCollectionPut.getCollectionIds()) {
      logger.info("updateEntities: :::::::::::::::::::: collectionId: " + collectionId);

      Map<String, Object> entity = directoryCollectionPut.getEntity(collectionId);

      logger.info("updateEntities: :::::::::::::::::::: before cleanEntity: " + Util.jsonStringFomObject(entity));

      cleanEntity(entity);

      logger.info("updateEntities: :::::::::::::::::::: before insertMissingAttributesIntoEntity: " + Util.jsonStringFomObject(entity));

      insertMissingAttributesIntoEntity(directoryCollectionPut, entity);

      logger.info("updateEntities: :::::::::::::::::::: before transformEntityForEmx2: " + Util.jsonStringFomObject(entity));

      transformEntityForEmx2(entity);

      logger.info("updateEntities: :::::::::::::::::::: before mapToGraphQL: " + Util.jsonStringFomObject(entity));

      String entityGraphql = mapToGraphQL(entity);

      logger.info("updateEntities: :::::::::::::::::::: entityGraphql: " + entityGraphql);

      String grapqlCommand = "mutation {\n" +
              "  update (Collections: \n" +
              entityGraphql +
              "  ) { message }\n" +
              "}";

      logger.info("updateEntities: :::::::::::::::::::: grapqlCommand: " + grapqlCommand);

      JsonObject result = directoryCallsGraphql.runGraphqlCommand(DirectoryEndpointsGraphql.getDatabaseEricEndpoint(), grapqlCommand);

      logger.info("updateEntities: :::::::::::::::::::: result: " + result);
    }

    logger.info("DirectoryApiRest.updateEntities: :::::::::::::::::::: done");

    return true;
  }

  /**
   * Removes keys from the given collection if the corresponding value is an empty list or a list with a single null element.
   *
   * @param entity The collection to clean.
   */
  private void cleanEntity(Map<String, Object> entity) {
    List<String> badKeys = new ArrayList<String>(); // List of keys to remove>
    for (String key: entity.keySet()) {
      if (entity.get(key) instanceof List) {
        List list = (List) entity.get(key);
        if (list.size() == 0)
          badKeys.add(key);
        if (list.size() == 1 && list.get(0) == null) {
          logger.warn("cleanEntity: attribute \"" + key + "\" has a single null element");
          badKeys.add(key);
        }
      }
    }
    for (String key: badKeys)
      entity.remove(key);
  }

  /**
   * Inserts missing attributes into the collection based on the provided DirectoryCollectionPut object.
   *
   * @param directoryCollectionPut The object containing the missing attributes.
   * @param entity The collection to insert missing attributes into.
   */
  private void insertMissingAttributesIntoEntity(DirectoryCollectionPut directoryCollectionPut, Map<String, Object> entity) {
    if (!entity.containsKey("country"))
      entity.put("country", directoryCollectionPut.getCountryCode());
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
      logger.info("updateEntities: :::::::::::::::::::: initial value of data_categories: " + entity.get("data_categories"));
      List<String> dataCategories = new ArrayList<>();
      dataCategories.add("BIOLOGICAL_SAMPLES");
      entity.put("data_categories", dataCategories);
    }
  }

  /**
   * Transforms the collection for the EMX2 format by transforming specific attributes.
   *
   * @param entity The collection to transform.
   */
  private void transformEntityForEmx2(Map<String, Object> entity) {
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
  private void transformAttributeForEmx2(Map<String, Object> entity, String attributeName, String attributeElementName) {
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

  // Convert Java Map to GraphQL mutation-friendly string
  public static String mapToGraphQL(Map<String, Object> map) {
    // Convert Map to JSON using Gson
    Gson gson = new GsonBuilder().serializeNulls().create();  // Handles nulls if needed
    String json = gson.toJson(map);

    // Post-process JSON for GraphQL syntax
    String graphQLFormatted = json
            .replaceAll("\"([^\"]+)\":", "$1:");   // Remove quotes around keys

    return graphQLFormatted;
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

    logger.info("DirectoryApiRest.updateStarModel: successfully posted " + starModelInputData.getFactCount() + " facts to the Directory");

    return true;
  }

//  /**
//   * Deletes existing star models from the Directory service for each of the collection IDs in the supplied StarModelInputData object.
//   *
//   * @param starModelInputData The input data for deleting existing star models.
//   * @return An boolean indicating the success or failure of the deletion.
//   */
//  private boolean deleteStarModel(StarModelData starModelInputData) {
//    try {
//      String grapqlCommand = "query {\n" +
//              "  CollectionFacts( filter: { id: { equals: \"" + id.toString() + "\" } } ) {\n" +
//              "    id\n" +
//              "    name\n" +
//              "  }\n" +
//              "}";
//
//      JsonObject result = directoryCallsGraphql.runGraphqlCommand(DirectoryEndpointsGraphql.getDatabaseEricEndpoint(), grapqlCommand);
//
//      if (result == null) {
//        logger.warn("deleteStarModel: result is null");
//        return false;
//      }
//
//      logger.info("deleteStarModel: result: " + result);
//
//      Map<String, Object> biobanks = directoryCallsGraphql.convertJsonObjectToMap(result);
//
//      if (!biobanks.containsKey("Biobanks")) {
//        logger.warn("deleteStarModel: no Biobanks element found, skipping");
//        return false;
//      }
//
//      List<Map<String, Object>> biobankList = (List<Map<String, Object>>) biobanks.get("Biobanks");
//      if (biobankList == null || biobankList.size() == 0) {
//        logger.warn("deleteStarModel: Collections list is null or empty, skipping");
//        return false;
//      }
//
//      Map<String, Object> item = biobankList.get(0);
//
//      if (item == null) {
//        logger.warn("deleteStarModel: first element of biobankList is null");
//        return false;
//      }
//
//      if (!item.containsKey("id")) {
//        logger.warn("deleteStarModel: no id element found in item: " + Util.jsonStringFomObject(item));
//        return false;
//      }
//
//      String biobankId = (String) item.get("id");
//
//      if (!id.toString().equals(biobankId)) {
//        logger.warn("deleteStarModel: id in item: " + biobankId + " does not match id: " + id);
//        return false;
//      }
//
//      if (!item.containsKey("name")) {
//        logger.warn("deleteStarModel: no name element found in item: " + Util.jsonStringFomObject(item));
//        return false;
//      }
//
//      String name = (String) item.get("name");
//
//      biobank.setId(biobankId);
//      biobank.setName(name);
//    } catch (Exception e) {
//      logger.warn("deleteStarModel: Exception during biobank import: " + Util.traceFromException(e));
//      return false;
//    }
//
//    return true;
//  }

  /**
   * Updates the fact tables block for a specific country with the provided data.
   *
   * @param countryCode The country code, e.g. DE.
   * @param factTablesBlock A list of maps representing the fact tables block data.
   * @return true if the update was successful, false otherwise.
   */
  @Override
  protected boolean updateFactTablesBlock(String countryCode, List<Map<String, String>> factTablesBlock) {
    return false;
  }

  /**
   * Retrieves a list of fact IDs from the Directory associated with a specific collection.
   *
   * @param countryCode The country code, e.g. DE.
   * @param collectionId The ID of the collection to retrieve fact IDs for.
   * @return A list of fact IDs for the specified collection, or null if there is an issue retrieving the data. An empty list indicates that there are no more facts left to be retrieved.
   */
  @Override
  protected List<String> getNextPageOfFactIdsForCollection(String countryCode, String collectionId) {
    return null;
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
    return false;
  }

  /**
   * Checks if a given diagnosis code is a valid ICD value by querying the Directory service.
   *
   * @param diagnosis The diagnosis code to be validated.
   * @return true if the diagnosis code is a valid ICD value, false if not, or if an error condition was encountered.
   */
  protected boolean isValidIcdValue(String diagnosis) {
    try {
      String grapqlCommand = "query {\n" +
              "  DiseaseTypes( filter: { name: { equals: \"" + diagnosis + "\" } } ) {\n" +
              "    name\n" +
              "  }\n" +
              "}";

      JsonObject result = directoryCallsGraphql.runGraphqlCommand(DirectoryEndpointsGraphql.getDatabaseDirectoryOntologiesEndpoint(), grapqlCommand);

      if (result == null) {
        logger.warn("isValidIcdValue: result is null");
        return false;
      }

      logger.info("isValidIcdValue: result: " + result);

      Map<String, Object> diseaseType = directoryCallsGraphql.convertJsonObjectToMap(result);

      if (!diseaseType.containsKey("DiseaseTypes")) {
        logger.warn("isValidIcdValue: no DiseaseTypes element found");
        return false;
      }

      List<Map<String, Object>> diseaseTypeList = (List<Map<String, Object>>) diseaseType.get("DiseaseTypes");
      if (diseaseTypeList.size() == 0) {
        logger.warn("isValidIcdValue: diseaseTypeList is empty");
        return false;
      }

      Map<String, Object> item = diseaseTypeList.get(0);

      if (item == null) {
        logger.warn("fetchCollectionGetOutcomes: first item in list of disease types is null!");
        return false;
      }

      if (!item.containsKey("name")) {
        logger.warn("isValidIcdValue: no name element found in item: " + Util.jsonStringFomObject(item));
        return false;
      }

      String name = (String) item.get("name");
      if (name.equals(diagnosis)) {
        return true;
      }

      logger.warn("isValidIcdValue: name: " + name + " does not equal diagnosis: " + diagnosis);
    } catch (Exception e) {
      logger.warn("isValidIcdValue: Exception during disease code check: " + Util.traceFromException(e));
    }

    return false;
  }
}
