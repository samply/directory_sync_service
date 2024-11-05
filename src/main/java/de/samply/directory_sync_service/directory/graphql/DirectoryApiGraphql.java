package de.samply.directory_sync_service.directory.graphql;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.BbmriEricId;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The DirectoryApiRest class provides an interface for interacting with the Directory service.
 * This class allows for fetching and updating biobank and collection information, managing star models,
 * and performing various validation and correction operations.
 * It supports a mock mode for testing purposes, where no real Directory interactions are performed.
 */
public class DirectoryApiGraphql extends DirectoryApi {
  private final String username;
  private final String password;
  private final DirectoryCallsGraphql directoryCallsGraphql;
  private final DirectoryEndpointsGraphql directoryEndpointsGraphql;
  private final Map<String,String> databaseEricEndpointMap = new HashMap<>();
  private final Map<String,Boolean> columnInTableMap = new HashMap<>();


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
   * @return true if a login endpoint for this API is accessible, false otherwise.
   */
  @Override
  public boolean isLoginAvailable() {
    if (!super.isLoginAvailable()) {
      logger.warn("DirectoryApiGraphql.isLoginAvailable: failing availablity test because the login GraphQL endpoint is not there");
      return false;
    }

    String endpoint = directoryEndpoints.getLoginEndpoint();

    if (!directoryCallsGraphql.endpointIsValidGraphql(endpoint)) {
      logger.warn("DirectoryApiGraphql.isLoginAvailable: failing login availablity test because " + endpoint + " returns an error");
      return false;
    }

    logger.info("DirectoryApiGraphql.isLoginAvailable: login availability test has succeeded");

    return true;
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
      String countryCode = id.getCountryCode();
      Map<String, Object> item = directoryCallsGraphql.runGraphqlQueryReturnMap(getDatabaseEricEndpoint(countryCode), "Biobanks", "filter: { id: { equals: \"" + id + "\" } }", new ArrayList<>(List.of("id", "name")));
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
   * for all of the collections listed in collectionIds. The countryCode is not used.
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

      String collectionCountryCode = extractCountryCodeFromBbmriEricId(collectionId);
      List<Map<String, Object>> collectionsList = directoryCallsGraphql.runGraphqlQueryReturnList(getDatabaseEricEndpoint(collectionCountryCode), graphqlCommand);
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

    logger.info("DirectoryApiRest.updateEntities: :::::::::::::::::::: about to update " + directoryCollectionPut.getCollectionIds().size() + " collections");

    for (String collectionId: directoryCollectionPut.getCollectionIds()) {
      logger.info("DirectoryApiRest.updateEntities: :::::::::::::::::::: about to update collection: " + collectionId);

      Map<String, Object> entity = directoryCollectionPut.getEntity(collectionId);
      cleanEntity(entity);
      if (entity.containsKey("timestamp"))
        entity.put("timestamp", cleanTimestamp(entity.get("timestamp").toString()));
      insertMissingAttributesIntoEntity(directoryCollectionPut, entity);
      transformEntityForEmx2(entity);
      deleteUnknownFieldsFromEntity(extractCountryCodeFromBbmriEricId(collectionId), entity);
      String entityGraphql = mapToGraphQL(entity);
      logger.info("DirectoryApiRest.updateEntities: :::::::::::::::::::: entityGraphql: " + entityGraphql);
      String countryCode = extractCountryCodeFromBbmriEricId(collectionId);

      String graphqlCommand = "mutation {\n" +
              "  update (Collections: \n" +
              entityGraphql +
              "  ) { message }\n" +
              "}";

      JsonObject result = directoryCallsGraphql.runGraphqlCommand(getDatabaseEricEndpoint(countryCode), graphqlCommand);

      if (result == null) {
        logger.warn("updateEntities: result is null");
        return false;
      }

      logger.info("DirectoryApiRest.updateEntities: :::::::::::::::::::: result: " + result);
    }

    return true;
  }

  /**
   * Deletes unknown fields from an entity.
   *
   * Background: different implementations of the Directory support different fields
   * in the Collection type. In particular, the "national_node" and "biobank_label"
   * fields are not universally supported. This method will make an API call to the
   * Directory to see if these fields are known, and if not, will delete them from
   * the entity.
   *
   * @param countryCode The country code used to determine the database endpoint.
   * @param entity The entity map from which to delete unknown fields.
   */
  private void deleteUnknownFieldsFromEntity(String countryCode, Map<String, Object> entity) {
    if (entity.containsKey("national_node") && !isColumnInTable(countryCode, "Collection", "national_node"))
      entity.remove("national_node");
    if (entity.containsKey("biobank_label") && !isColumnInTable(countryCode, "Collection", "biobank_label"))
      entity.remove("biobank_label");
  }

  /**
   * Extracts the country code from a given BBMRI ID string. Works for both
   * biobank and collection IDs.
   *
   * @param id The BBMRI ID string from which to extract the country code.
   * @return The country code extracted from the BBMRI ID string.
   */
  private String extractCountryCodeFromBbmriEricId(String id) {
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
  private void cleanEntity(Map<String, Object> entity) {
    List<String> badKeys = new ArrayList<String>(); // List of keys to remove>
    for (String key: entity.keySet()) {
      if (entity.get(key) instanceof List list) {
        if (list.size() == 0) {
          logger.warn("cleanEntity: attribute \"" + key + "\" is an empty list");
          badKeys.add(key);
        }
        if (list.size() == 1 && list.get(0) == null) {
          logger.warn("cleanEntity: attribute \"" + key + "\" has a single null element");
          badKeys.add(key);
        }
      }
    }
    for (String key: badKeys) {
      logger.info("cleanEntity: removing bad attribute: \"" + key + "\"");
      entity.remove(key);
    }
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
  private String cleanTimestamp(String timestamp) {
    // Use the String.matches method to check if the timestamp ends with a non-numeric character
    if (timestamp.matches(".*[^\\d]$")) {
      timestamp = timestamp.substring(0, timestamp.length() - 1);
      logger.info("cleanTimestamp: ............................ corrected timestamp: " + timestamp);
    }

    return timestamp;
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

  /**
   * Converts a Java {@link Map} to a GraphQL mutation-friendly string format.
   * <p>
   * This method transforms a provided Map into a format compatible with GraphQL
   * mutations by first serializing it to JSON and then modifying the JSON output
   * to match GraphQL syntax (e.g., removing quotes around keys).
   * </p>
   * Nested maps and arrays can be handled by this method.
   * </p>
   * @param map The {@link Map} to be converted to a GraphQL mutation-friendly string.
   * @return A {@code String} representing the GraphQL-compatible format of the input map.
  */
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

    // The national_node attribute is needed by some Directory versions, but not by others.
    // To find out what the current Directory requires, we need to do a little bit of introspection
    // in the Directory's GrapgQL API.
    boolean includeNationalNode = isColumnInTable(countryCode, "CollectionFacts", "national_node");

    for (Map<String, String> factTable : factTablesBlock)
      try {
        if (!factTable.containsKey("national_node") && countryCode != null && !countryCode.isEmpty())
          factTable.put("national_node", countryCode);

        String factTableAttributeString = buildFactTableAttributeString(factTable, includeNationalNode);
        String graphqlCommand = buildUpdateFactTableGraphqlCommand(factTableAttributeString);
        logger.info("updateFactTablesBlock: graphqlCommand: " + graphqlCommand);
        JsonObject result = directoryCallsGraphql.runGraphqlCommand(getDatabaseEricEndpoint(countryCode), graphqlCommand);
        if (result == null) {
          logger.warn("updateFactTablesBlock: result is null with national_node attribute");
          return false;
        }

        logger.info("updateFactTablesBlock: result: " + result);
      } catch (Exception e) {
        logger.warn("login: Exception during fact deletion: " + Util.traceFromException(e));
        return false;
      }

    return true;
  }

  /**
   * Builds a GraphQL command for updating a Directory fact table with the attributes encoded in the provided string.
   *
   * @param factTableAttributeString The string representing the attributes for the fact table update.
   * @return The GraphQL command for updating the fact table.
   */
  private String buildUpdateFactTableGraphqlCommand(String factTableAttributeString) {
    String graphqlCommand = "mutation {\n" +
            "  insert( CollectionFacts: { " + factTableAttributeString + " } ) {\n" +
            "    message\n" +
            "  }\n" +
            "}";

    return graphqlCommand;
  }

  /**
   * Checks if a specific column exists in a Directory GraphQL table for a given country code.
   * The Directory GraphQL API may need to be called to complete this task.
   *
   * @param countryCode The country code used to determine the database endpoint. E.g. "DE".
   * @param tableName The name of the table to check for the column. E.g. "CollectionFacts".
   * @param columnName The name of the column to check for existence. E.g. "national_node".
   * @return true if the column exists in the specified table, false otherwise. Also returns
   *         false on error.
   */
  private boolean isColumnInTable(String countryCode, String tableName, String columnName) {
    logger.info("isColumnInTable: countryCode: " + countryCode + ", tableName: " + tableName + ", columnName: " + columnName);

    // Prevent the same column from being checked multiple times
    String hash = countryCode + tableName + columnName;
    if (columnInTableMap.containsKey(hash))
      return columnInTableMap.get(hash);

    String graphqlCommand = "{\n" +
            "  _schema {\n" +
            "    settings {\n" +
            "      key\n" +
            "      value\n" +
            "    }\n" +
            "    tables {\n" +
            "      name\n" +
            "      columns {\n" +
            "        name\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

    AtomicBoolean found = new AtomicBoolean(false);
    try {
      JsonObject result = directoryCallsGraphql.runGraphqlCommand(getDatabaseEricEndpoint(countryCode), graphqlCommand);
      JsonArray tableArray = result.get("_schema").getAsJsonObject().get("tables").getAsJsonArray();

      tableArray.forEach(jsonElement -> {
        JsonObject tables = jsonElement.getAsJsonObject();
        if (tables.get("name").getAsString().equals(tableName)) {
          JsonArray columns = tables.get("columns").getAsJsonArray();

          // Is columnName in the list of columns?
          for (JsonElement column : columns) {
            String foundColumnName = column.getAsJsonObject().get("name").getAsString();
            if (foundColumnName.equals(columnName)) {
              logger.info("isColumnInTable: foundColumnName: " + foundColumnName + " == columnName: " + columnName);
              found.set(true);
              break;
            }
          }
        }
      });

      columnInTableMap.put(hash, found.get());
      return found.get();
    } catch (Exception e) {
      logger.warn("isColumnInTable: exception: " + Util.traceFromException(e));
    }

    columnInTableMap.put(hash, false);
    return false;
  }

  /**
   * Builds a String representation of the attributes for updating a Directory fact table using GraphQL,
   * based on the provided map of key-value pairs.
   *
   * @param factTable The map containing key-value pairs representing the attributes for the fact table.
   * @param includeNationalNode Flag to indicate whether to include the national node attribute.
   * @return A string representation of the fact table attributes for updating.
   */
  private String buildFactTableAttributeString(Map<String, String> factTable, boolean includeNationalNode) {
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
              key.equals("national_node") && !includeNationalNode
      )
        continue; // including national_node causes problems with directory-playground.molgenis.net
      else if (
              key.equals("national_node") && includeNationalNode
      )
        transformedValue = wrapValueInHashWithAttribute("id", value);
      else if (
              key.equals("collection")
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

  /**
   * Wraps a given name/value pair in a format suitable for GraphQL.
   *
   * @param attributeName The name of the attribute.
   * @param value The value to be wrapped.
   * @return Formatted name/value pair as a String.
   */
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
      List<Map<String, Object>> collectionFactsList = directoryCallsGraphql.runGraphqlQueryReturnList(getDatabaseEricEndpoint(countryCode), "CollectionFacts", null, new ArrayList<>(List.of("id")));
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
      if (!deleteFactById(countryCode, factId))
        return false;

    return true;
  }

  protected boolean deleteFactById(String countryCode, String factId) {
    try {
      String graphqlCommand = "mutation {\n" +
              "  delete( CollectionFacts: { id: \"" + factId + "\" } ) {\n" +
              "    message\n" +
              "  }\n" +
              "}";

      JsonObject result = directoryCallsGraphql.runGraphqlCommand(getDatabaseEricEndpoint(countryCode), graphqlCommand);

      if (result == null) {
        logger.warn("deleteFactById: result is null");
        return false;
      }

      logger.info("login: result: " + result);
    } catch (Exception e) {
      logger.warn("deleteFactById: Exception during fact deletion: " + Util.traceFromException(e));
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

  /**
   * Retrieves the database endpoint for the ERIC database API based on the country code.
   *
   * @param countryCode The country code used to determine the database endpoint.
   * @return The database endpoint for the ERIC database API, or null if no valid endpoint is found.
   */
  private String getDatabaseEricEndpoint(String countryCode) {
    if (databaseEricEndpointMap.containsKey(countryCode))
      return databaseEricEndpointMap.get(countryCode);

    // There are several plausible endpoints, so try each one, until we find one that
    // actually exists.

    String databaseEricEndpoint = directoryEndpointsGraphql.getDatabaseEricEndpoint1() + countryCode + directoryEndpointsGraphql.getApiEndpoint();
    if (directoryCallsGraphql.endpointIsValidGraphql(databaseEricEndpoint)) {
      logger.info("getDatabaseEricEndpoint: ÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖ using " + databaseEricEndpoint + " for ERIC database API");
      databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
      return databaseEricEndpoint;
    }

    databaseEricEndpoint = directoryEndpointsGraphql.getDatabaseEricEndpoint2() + directoryEndpointsGraphql.getApiEndpoint();
    if (directoryCallsGraphql.endpointIsValidGraphql(databaseEricEndpoint)) {
      logger.info("getDatabaseEricEndpoint: ÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖ using " + databaseEricEndpoint + " for ERIC database API");
      databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
      return databaseEricEndpoint;
    }

    databaseEricEndpoint = directoryEndpointsGraphql.getDatabaseEricEndpoint3() + directoryEndpointsGraphql.getApiEndpoint();
    if (directoryCallsGraphql.endpointIsValidGraphql(databaseEricEndpoint)) {
      logger.info("getDatabaseEricEndpoint: ÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖÖ using " + databaseEricEndpoint + " for ERIC database API");
      databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
      return databaseEricEndpoint;
    }

    return null;
  }
}
