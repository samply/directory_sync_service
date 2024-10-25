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
  private DirectoryEndpointsGraphql directoryEndpointsGraphql;

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
    this.directoryEndpointsGraphql = new DirectoryEndpointsGraphql();
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
    if (mockDirectory)
      // Don't try logging in if we are mocking
      return true;

    try {
      String graphqlCommand = "mutation {\n" +
              "  signin(password: \"" + password + "\", email: \"" + username + "\") {\n" +
              "    message\n" +
              "    token\n" +
              "  }\n" +
              "}";

      String endpoint = directoryEndpointsGraphql.getLoginEndpoint();
      JsonObject result = directoryCallsGraphql.runGraphqlCommand(endpoint, graphqlCommand);

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
    } catch (Exception e) {
      logger.warn("login: exception: " + Util.traceFromException(e));
      return false;
    }

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
      Map<String, Object> item = directoryCallsGraphql.runGraphqlQueryReturnMap(directoryEndpointsGraphql.getDatabaseEricEndpoint(), "Biobanks", "filter: { id: { equals: \"" + id.toString() + "\" } }", new ArrayList<>(List.of("id", "name")));
      if (item == null) {
        logger.warn("fetchBiobank: item is null");
        return null;
      }
      if (item.isEmpty()) {
        logger.info("fetchBiobank: no results from query");
        return biobank;
      }

      String biobankId = (String) item.get("id");

      if (!id.toString().equals(biobankId)) {
        logger.warn("fetchBiobank: id in item: " + biobankId + " does not match id: " + id);
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
      String graphqlCommand = "query {" +
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

      List<Map<String, Object>> collectionsList = directoryCallsGraphql.runGraphqlQueryReturnList(directoryEndpointsGraphql.getDatabaseEricEndpoint(), graphqlCommand);
      if (collectionsList == null) {
        logger.warn("fetchCollectionGetOutcomes: biobankList list is null");
        continue;
      }
      if (collectionsList.size() == 0) {
        logger.info("fetchCollectionGetOutcomes: collectionFactsList list is empty");
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
    if (mockDirectory) {
      // Dummy return if we're in mock mode
      logger.info("DirectoryApiRest.updateEntities: :::::::::::::::::::: in mock mode, skip update");
      return true;
    }

    for (String collectionId: directoryCollectionPut.getCollectionIds()) {
      Map<String, Object> entity = directoryCollectionPut.getEntity(collectionId);
      cleanEntity(entity);
      insertMissingAttributesIntoEntity(directoryCollectionPut, entity);
      transformEntityForEmx2(entity);
      String entityGraphql = mapToGraphQL(entity);

      String graphqlCommand = "mutation {\n" +
              "  update (Collections: \n" +
              entityGraphql +
              "  ) { message }\n" +
              "}";

      JsonObject result = directoryCallsGraphql.runGraphqlCommand(directoryEndpointsGraphql.getDatabaseEricEndpoint(), graphqlCommand);
    }

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
  private String mapToGraphQL(Map<String, Object> map) {
    // Convert Map to JSON using Gson
    Gson gson = new GsonBuilder().serializeNulls().create();  // Handles nulls if needed
    String json = gson.toJson(map);

    // Post-process JSON for GraphQL syntax
    String graphQLFormatted = json
            .replaceAll("\"([^\"]+)\":", "$1:");   // Remove quotes around keys

    return graphQLFormatted;
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
    logger.info("updateFactTablesBlock: <><><><><><><><><><> entered");

    if (factTablesBlock.size() == 0)
      // Nothing to insert
      return true;

    for (Map<String, String> factTable : factTablesBlock)
      try {
        if (!factTable.containsKey("national_node") && countryCode != null && !countryCode.isEmpty())
          factTable.put("national_node", countryCode);
        String factTableAttributeString = buildFactTableAttributeString(factTable);

        String graphqlCommand = "mutation {\n" +
                "  insert( CollectionFacts: { " + factTableAttributeString + " } ) {\n" +
                "    message\n" +
                "  }\n" +
                "}";

        logger.info("updateFactTablesBlock: graphqlCommand: " + graphqlCommand);

        JsonObject result = directoryCallsGraphql.runGraphqlCommand(directoryEndpointsGraphql.getDatabaseEricEndpoint(), graphqlCommand);

        if (result == null) {
          logger.warn("updateFactTablesBlock: result is null");
          return false;
        }

        logger.info("updateFactTablesBlock: result: " + result);
      } catch (Exception e) {
        logger.warn("login: Exception during fact deletion: " + Util.traceFromException(e));
        return false;
      }

    return true;
  }

  private String buildFactTableAttributeString(Map<String, String> factTable) {
    StringBuilder result = new StringBuilder();
    boolean first = true;

    for (Map.Entry<String, String> entry : factTable.entrySet()) {
      if (!first) {
        result.append(", ");  // Add comma between key-value pairs
      } else {
        first = false;
      }

      String key = entry.getKey();
      String value = entry.getValue();

      String transformedValue = "\"" + value + "\""; // Default: surround value with double quotes
      // Transform value depending on its type
      if (
              key.equals("age_range") ||
              key.equals("sex") ||
              key.equals("disease") ||
              key.equals("sample_type"))
        transformedValue = wrapValueInHashWithAttribute("name", value);
      else if (
              key.equals("collection") ||
              key.equals("national_node")
      )
        transformedValue = wrapValueInHashWithAttribute("id", value);
      else if (
              key.equals("number_of_samples") ||
              key.equals("number_of_donors")
      )
        transformedValue = value; // Don't put quotes around numbers

      // Append the key and value in the desired format
      result.append(key).append(": ").append(transformedValue);
    }

    return result.toString();
  }

  private String wrapValueInHashWithAttribute(String attributeName, String value) {
    return "{ " + attributeName + ": \"" + value + "\" }";
  }

  private boolean getFactPageToggle = false; // used to ensure that getNextPageOfFactIdsForCollection gets run only once
  /**
   * Retrieves a list of fact IDs from the Directory associated with a specific collection.
   *
   * @param countryCode The country code, e.g. DE.
   * @param collectionId The ID of the collection to retrieve fact IDs for.
   * @return A list of fact IDs for the specified collection, or null if there is an issue retrieving the data. An empty list indicates that there are no more facts left to be retrieved.
   */
  @Override
  protected List<String> getNextPageOfFactIdsForCollection(String countryCode, String collectionId) {
    List<String> factIds = new ArrayList<>();

    // Use getFactPageToggle to ensure that this method gets run only once.
    // My assumption is that the GraphQL API does not do any paging, so that a single API
    // call gets everything.
    if (getFactPageToggle) {
        getFactPageToggle = false;
        return factIds;
    }
    getFactPageToggle = true;

    try {
      List<Map<String, Object>> collectionFactsList = directoryCallsGraphql.runGraphqlQueryReturnList(directoryEndpointsGraphql.getDatabaseEricEndpoint(), "CollectionFacts", null, new ArrayList<>(List.of("id")));
      if (collectionFactsList == null) {
        logger.warn("getNextPageOfFactIdsForCollection: diseaseTypeList is null for collectionId: " + collectionId + ", there may be a problem");
        return null;
      }
      if (collectionFactsList.size() == 0) {
        logger.info("getNextPageOfFactIdsForCollection: diseaseTypeList is empty for collectionId: " + collectionId + ", which is presumably unknown");
        return factIds;
      }

      for (Map<String, Object> item : collectionFactsList) {
        if (item == null) {
          logger.warn("getNextPageOfFactIdsForCollection: item is null");
          return null;
        }
        if (!item.containsKey("id")) {
          logger.info("getNextPageOfFactIdsForCollection: id key missing from item");
          return null;
        }

        String collectionFactsId = (String) item.get("id");

        factIds.add(collectionFactsId);
      }
    } catch (Exception e) {
      logger.warn("getNextPageOfFactIdsForCollection: Exception during biobank import: " + Util.traceFromException(e));
      return null;
    }

    return factIds;
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

    for (String factId : factIds)
      try {
        String graphqlCommand = "mutation {\n" +
                "  delete( CollectionFacts: { id: \"" + factId + "\" } ) {\n" +
                "    message\n" +
                "  }\n" +
                "}";

        JsonObject result = directoryCallsGraphql.runGraphqlCommand(directoryEndpointsGraphql.getDatabaseEricEndpoint(), graphqlCommand);

        if (result == null) {
          logger.warn("deleteFactsByIds: result is null");
          return false;
        }

        logger.info("login: result: " + result);
      } catch (Exception e) {
        logger.warn("deleteFactsByIds: Exception during fact deletion: " + Util.traceFromException(e));
        return false;
      }

    return true;
  }

  /**
   * Checks if a given diagnosis code is a valid ICD value by querying the Directory service.
   *
   * @param diagnosis The diagnosis code to be validated.
   * @return true if the diagnosis code is a valid ICD value, false if not, or if an error condition was encountered.
   */
  protected boolean isValidIcdValue(String diagnosis) {
    try {
      List<Map<String, Object>> diseaseTypeList = directoryCallsGraphql.runGraphqlQueryReturnList(directoryEndpointsGraphql.getDatabaseDirectoryOntologiesEndpoint(), "DiseaseTypes", "filter: { name: { equals: \"" + diagnosis + "\" } }", new ArrayList<>(List.of("name")));
      if (diseaseTypeList == null) {
        logger.warn("isValidIcdValue: diseaseTypeList is null for diagnosis: " + diagnosis + ", there may be a problem");
        return false;
      }
      if (diseaseTypeList.size() == 0) {
        logger.info("isValidIcdValue: diseaseTypeList is empty for diagnosis: " + diagnosis + ", which is presumably unknown");
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
