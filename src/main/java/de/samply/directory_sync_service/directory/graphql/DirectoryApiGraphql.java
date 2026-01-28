package de.samply.directory_sync_service.directory.graphql;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.ConvertDirectoryCollectionGetToCollections;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.model.Collections;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.converter.ConvertCollectionsToDirectoryCollectionPut;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The DirectoryApiGraphql class provides an interface for interacting with the Directory service.
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
   * Constructs a new instance.
   * If we are not in mocking mode, log in to the Directory.
   *
   * @param baseUrl The base URL of the Directory service.
   * @param mockDirectory If true, the instance operates in mock mode, returning fake data.
   * @param username The username for authenticating with the Directory.
   * @param password The password for authenticating with the Directory.
   */
  public DirectoryApiGraphql(String baseUrl, boolean mockDirectory, String username, String password) {
    super(mockDirectory);
    this.directoryCallsGraphql = new DirectoryCallsGraphql(baseUrl, username, password);
    this.directoryEndpointsGraphql = new DirectoryEndpointsGraphql();
    directoryEndpoints = new DirectoryEndpointsGraphql();
    this.username = username;
    this.password = password;

    logger.debug("DirectoryApiGraphql: constructed");
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

      JsonElement signin = result.get("signin");
      if (signin == null) {
        logger.warn("login: signin is null");
        logger.warn("login: result: " + Util.jsonStringFomObject(result));
        return false;
      }

      JsonElement tokenObject = signin.getAsJsonObject().get("token");
      if (tokenObject == null) {
        logger.warn("login: tokenObject is null");
        logger.warn("login: signin: " + Util.jsonStringFomObject(signin));
        return false;
      }

      String token = tokenObject.getAsString();
      if (token == null) {
        logger.warn("login: token is null");
        return false;
      }

      directoryCallsGraphql.setToken(token);
    } catch (Exception e) {
      logger.warn("login: exception: " + Util.traceFromException(e));
      return false;
    }

    logger.debug("DirectoryApiGraphql.login: log in successful");

    return true;
  }

  /**
   * Fetches the Biobank with the given {@code id}.
   *
   * @param id the ID of the Biobank to fetch.
   * @return either the Biobank or null if an error occurs
   */
  public Biobank fetchBiobank(BbmriEricId id) {
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
        logger.warn("fetchBiobank: no results from query");
        return biobank;
      }

      String biobankId = (String) item.get("id");

      if (!id.toString().equals(biobankId)) {
        logger.warn("fetchBiobank: id in item: " + biobankId + " does not match id: " + id);
        return null;
      }

      biobank.setAcronym((String) item.get("acronym"));
      biobank.setCapabilities((List<Map>) item.get("capabilities"));
      biobank.setContact((Map) item.get("contact"));
      biobank.setCountry((Map) item.get("country"));
      biobank.setDescription((String) item.get("description"));
      biobank.setHead((Map) item.get("head"));
      biobank.setId(biobankId);
      biobank.setJuridicalPerson((String) item.get("juridical_person"));
      biobank.setLatitude((String) item.get("latitude"));
      biobank.setLocation((String) item.get("location"));
      biobank.setLongitude((String) item.get("longitude"));
      biobank.setName((String) item.get("name"));
      biobank.setNetwork((List<Map>) item.get("network"));
      biobank.setUrl((String) item.get("url"));
    } catch (Exception e) {
      logger.warn("fetchBiobank: Exception during biobank import: " + Util.traceFromException(e));
      return null;
    }

    return biobank;
  }

  /**
   * Make API calls to the Directory to fill a Collections object with attributes
   * for all of the collections listed in collectionIds. The countryCode is not used.
   *
   * @param collections
   */
  public void fetchBasicCollectionData(Collections collections) {
    if (mockDirectory) {
      // Dummy return if we're in mock mode
      return;
    }

    login();

    for (String collectionId: collections.getCollectionIds()) {
      logger.debug("generateCollections: collectionId: " + collectionId);
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
        logger.warn("generateCollections: biobankList list is null");
        continue;
      }
      if (collectionsList.size() == 0) {
        logger.warn("generateCollections: collectionFactsList list is empty");
        continue;
      }

      Map<String, Object> collectionMap = collectionsList.get(0);

      if (collectionMap == null) {
        logger.warn("generateCollections: entity get item is null, does the collection exist in the Directory: " + collectionId);
        continue;
      }
      ConvertDirectoryCollectionGetToCollections.addCollectionFromMap(collections, collectionId, collectionMap);
    }

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

    // Convert Collections object into a DirectoryCollectionPut object. This is
    // really intended for use with the RESTful API, but it can be mutated to
    // work with the GraphQL interface as well.
    DirectoryCollectionPut directoryCollectionPut = ConvertCollectionsToDirectoryCollectionPut.convert(collections);
    if (directoryCollectionPut == null) {
      logger.warn("sendUpdatedCollections: Problem converting FHIR attributes to Directory attributes");
      return false;
    }
    logger.debug("sendUpdatedCollections: 1 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

    login();

    for (String collectionId: directoryCollectionPut.getCollectionIds()) {
      JsonObject result = null;
      try {
        logger.debug("sendUpdatedCollections: about to update collection: " + collectionId);

        Map<String, Object> entity = directoryCollectionPut.getEntity(collectionId);
        cleanEntity(entity);
        if (entity.containsKey("timestamp"))
          entity.put("timestamp", cleanTimestamp(entity.get("timestamp").toString()));
        insertMissingAttributesIntoEntity(directoryCollectionPut, entity);
        transformEntityForEmx2(entity);
        deleteUnknownFieldsFromEntity(extractCountryCodeFromBbmriEricId(collectionId), entity);
        String entityGraphql = mapToGraphQL(entity);
        String countryCode = extractCountryCodeFromBbmriEricId(collectionId);

        String graphqlCommand = "mutation {\n" +
                "  update (Collections: \n" +
                entityGraphql +
                "  ) { message }\n" +
                "}";

        result = directoryCallsGraphql.runGraphqlCommand(getDatabaseEricEndpoint(countryCode), graphqlCommand);
      } catch (Exception e) {
        logger.warn("sendUpdatedCollections: problem for collectionId: " + collectionId + ", exception: " + Util.traceFromException(e));
      }

      if (result == null) {
        logger.warn("sendUpdatedCollections: result is null for collectionId: " + collectionId + ", skipping");
        continue;
      }
    }

    return true;
  }

  /**
   * Deletes unknown fields from an entity.
   * <p>
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
    if (entity.containsKey("national_node") && !isColumnInTable(countryCode, "Collections", "national_node"))
      entity.remove("national_node");
    if (entity.containsKey("biobank_label") && !isColumnInTable(countryCode, "Collections", "biobank_label"))
      entity.remove("biobank_label");
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
    return json.replaceAll("\"([^\"]+)\":", "$1:");
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
        JsonObject result = directoryCallsGraphql.runGraphqlCommand(getDatabaseEricEndpoint(countryCode), graphqlCommand);
        if (result == null) {
          logger.warn("updateFactTablesBlock: result is null with national_node attribute");
          return false;
        }
      } catch (Exception e) {
        logger.warn("updateFactTablesBlock: Exception during fact updating: " + Util.traceFromException(e));
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

    boolean found = false;
    try {
      JsonObject result = directoryCallsGraphql.runGraphqlCommand(getDatabaseEricEndpoint(countryCode), graphqlCommand);
      JsonArray tableArray = result.get("_schema").getAsJsonObject().get("tables").getAsJsonArray();

      for (JsonElement jsonElement : tableArray) {
        JsonObject table = jsonElement.getAsJsonObject();
        String tableNameFound = table.get("name").getAsString();
        if (tableNameFound.equals(tableName)) {
          JsonArray columns = table.get("columns").getAsJsonArray();

          // Is columnName in the list of columns?
          for (JsonElement column : columns) {
            String foundColumnName = column.getAsJsonObject().get("name").getAsString();
            if (foundColumnName.equals(columnName)) {
              found = true;
              break;
            }
          }
          break;
        }
      }
    } catch (Exception e) {
      logger.warn("isColumnInTable: exception: " + Util.traceFromException(e));
    }

    columnInTableMap.put(hash, found);
    return found;
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
   * @param collectionId The ID of the collection to retrieve fact IDs for.
   * @return A list of fact IDs for the specified collection, or null if there is an issue retrieving the data. An empty list indicates that there are no more facts left to be retrieved.
   */
  @Override
  protected List<String> getNextPageOfFactIdsForCollection(String collectionId) {
    String countryCode = extractCountryCodeFromBbmriEricId(collectionId);
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
        logger.warn("getNextPageOfFactIdsForCollection: collectionFactsList is null for collectionId: " + collectionId + ", there may be a problem");
        return null;
      }
      if (collectionFactsList.size() == 0) {
        logger.debug("getNextPageOfFactIdsForCollection: collectionFactsList is empty for collectionId: " + collectionId + ", which is presumably unknown");
        return factIds;
      }

      for (Map<String, Object> item : collectionFactsList) {
        if (item == null) {
          logger.warn("getNextPageOfFactIdsForCollection: item is null");
          return null;
        }
        if (!item.containsKey("id")) {
          logger.warn("getNextPageOfFactIdsForCollection: id key missing from item");
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
      if (diseaseTypeList.size() == 0)
        // diseaseTypeList is empty for diagnosis, which is presumably unknown");
        return false;

      Map<String, Object> item = diseaseTypeList.get(0);

      if (item == null) {
        logger.warn("isValidIcdValue: first item in list of disease types is null!");
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
   * <p>
   * Uses a heuristic that looks through the available databases at the Directory and
   * builds an endpoint from the most plausible one.
   *
   * @param countryCode The country code used to determine the database endpoint.
   * @return The database endpoint for the ERIC database API, or null if no valid endpoint is found.
   */
  private String getDatabaseEricEndpoint(String countryCode) {
    if (databaseEricEndpointMap.containsKey(countryCode))
      return databaseEricEndpointMap.get(countryCode);

    List<String> databases = getDatabases();

    if (databases == null)  {
      logger.warn("getDatabaseEricEndpoint: databases is null");
      return null;
    }

    if (databases.size() == 0) {
      logger.warn("getDatabaseEricEndpoint: databases is empty");
      return null;
    }

    String database;
    String databaseEricEndpoint;

    // If there is a database called e.g. ERIC-DE, use that.
    database = "ERIC-" + countryCode;
    if (databases.contains(database)) {
      logger.debug("getDatabaseEricEndpoint: database " + database + " exists");
      databaseEricEndpoint = database + directoryEndpointsGraphql.getApiEndpoint();
      databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
      return databaseEricEndpoint;
    }

    // Look to see if there is a database that ends with the country code and use if found
    for (String db : databases) {
      if (db.endsWith("-" + countryCode)) {
        logger.debug("getDatabaseEricEndpoint: database " + db + " exists");
        databaseEricEndpoint = db + directoryEndpointsGraphql.getApiEndpoint();
        databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
        return databaseEricEndpoint;
      }
    }

    // If there is a database called BBMRI-ERIC, use that.
    database = "BBMRI-ERIC";
    if (databases.contains(database)) {
      logger.debug("getDatabaseEricEndpoint: database " + database + " exists");
      databaseEricEndpoint = database + directoryEndpointsGraphql.getApiEndpoint();
      databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
      return databaseEricEndpoint;
    }

    // If there is a database called ERIC, use that.
    database = "ERIC";
    if (databases.contains(database)) {
      logger.debug("getDatabaseEricEndpoint: database " + database + " exists");
      databaseEricEndpoint = database + directoryEndpointsGraphql.getApiEndpoint();
      databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
      return databaseEricEndpoint;
    }

    // If there is a database with "ERIC" in its name, use that.
    for (String db : databases) {
      if (db.contains("ERIC")) {
        logger.debug("getDatabaseEricEndpoint: database " + db + " exists");
        databaseEricEndpoint = db + directoryEndpointsGraphql.getApiEndpoint();
        databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
        return databaseEricEndpoint;
      }
    }

    // If there is a database called BBMRI, use that.
    database = "BBMRI";
    if (databases.contains(database)) {
      logger.debug("getDatabaseEricEndpoint: database " + database + " exists");
      databaseEricEndpoint = database + directoryEndpointsGraphql.getApiEndpoint();
      databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
      return databaseEricEndpoint;
    }

    // If there is a database with "BBMRI" in its name, use that.
    for (String db : databases) {
      if (db.contains("BBMRI")) {
        logger.debug("getDatabaseEricEndpoint: database " + db + " exists");
        databaseEricEndpoint = db + directoryEndpointsGraphql.getApiEndpoint();
        databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
        return databaseEricEndpoint;
      }
    }

    // We are scraping the barrel here. Pick a database whose name is not an already known
    // non-ERIC database.
    for (String db : databases) {
      if (!db.equals("pet store") && !db.equals("TestPet") && !db.equals("DirectoryOntologies") && !db.equals("_SYSTEM_")) { // non-ERIC DBs
        logger.debug("getDatabaseEricEndpoint: database " + db + " exists");
        databaseEricEndpoint = db + directoryEndpointsGraphql.getApiEndpoint();
        databaseEricEndpointMap.put(countryCode, databaseEricEndpoint);
        return databaseEricEndpoint;
      }
    }

    logger.warn("getDatabaseEricEndpoint: no database found for country code: " + countryCode);
    return null;
  }

  /**
   * Retrieves a list of database labels from the Directory.
   *
   * <p>This method sends a GraphQL query to fetch the available database schemas.
   * It parses the response and extracts the "label" field from each schema object.
   * If the response structure is unexpected or data is missing, warnings are logged
   * and the method returns {@code null}.
   *
   * @return a list of database labels, or {@code null} if an error occurs or the response
   *         does not contain the expected data structure
   */
  public List<String> getDatabases() {
    // Try to get all databases
    String graphqlCommand = "{ _schemas { label } }";

    JsonObject data = directoryCallsGraphql.runGraphqlCommand((new DirectoryEndpointsGraphql()).getApiEndpoint(), graphqlCommand);

    if (data == null) {
      logger.warn("getDatabases: data is null");
      return null;
    }

    if (!data.has("_schemas")) {
      logger.warn("getDatabases: data has no _schemas element");
      return null;
    }

    if (!data.get("_schemas").isJsonArray()) {
      logger.warn("getDatabases: data._schemas is not a JsonArray");
      return null;
    }

    JsonArray schemas = data.get("_schemas").getAsJsonArray();

    List<String> databases = new ArrayList<>();

    for (JsonElement schema : schemas) {
      if (!schema.isJsonObject()) {
        logger.warn("getDatabases: schema is not a JsonObject, skipping");
        continue;
      }

      if (!schema.getAsJsonObject().has("label")) {
        logger.warn("getDatabases: schema has no label element");
        return null;
      }

      String label = schema.getAsJsonObject().get("label").getAsString();
      databases.add(label);
    }

    return databases;
  }

}
