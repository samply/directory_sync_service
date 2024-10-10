package de.samply.directory_sync_service.directory.graphql;

import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryCalls;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Class for interacting with a Directory service via REST API calls.
 * <p>
 * It provides methods to perform HTTP GET, POST, PUT, and DELETE operations on a Directory service.
 * It handles authentication via a login method and manages session tokens for authorized requests.
 */
public class DirectoryCallsGraphql extends DirectoryCalls {
  /**
   * Constructs a DirectoryCallsRest object.
   * <p>
   * This constructor initializes the HTTP client, base URL, and credentials for interacting with the Directory service.
   * It also triggers the login process to authenticate and obtain a session token.
   *
   * @param baseUrl the base URL for the Directory service
   * @param username the username for Directory authentication
   * @param password the password for Directory authentication
   */
  public DirectoryCallsGraphql(String baseUrl, String username, String password) {
    super(baseUrl, username, password);
  }

  /**
   * Checks if an endpoint returns an error by sending an HTTP GET request to the endpoint and analyzing the response.
   *
   * @param endpoint The endpoint to check for errors.
   * @return true if the endpoint does not return an error, false otherwise.
   */
  public boolean endpointIsValidGraphql(String endpoint) {
    String url = urlCombine(baseUrl, endpoint);
    logger.info("endpointIsValidGraphql: url: " + url);
    HttpGet request = new HttpGet(url);

    String response = executeRequest(request);
    if (response == null) {
      logger.warn("endpointIsValidGraphql: HTTP response is null");
      return false;
    }

    try {
      // Use gson to turn the response into a JSON object
      JsonObject jsonResponse = gson.fromJson(response, JsonObject.class);
      if (jsonResponse == null) {
        logger.warn("endpointIsValidGraphql: jsonResponse is null");
        return false;
      }

      // Check if the response contains an error
      if (jsonResponse.has("errors")) {
        logger.warn("endpointIsValidGraphql: jsonResponse has the following errors:");
        JsonObject errors = jsonResponse.getAsJsonObject("errors");
        for (String error: errors.keySet())
          logger.warn("endpointIsValidGraphql: error: " + error + ": " + errors.get(error).toString());
        return false;
      }
    } catch (JsonSyntaxException e) {
      logger.warn("endpointIsValidGraphql: JsonSyntaxException: " + e.getMessage());
      logger.warn("endpointIsValidGraphql: response: " + response);
      return false;
    }

    return true;
  }

  /**
   * Runs a GraphQL command against the Directory API to retrieve data.
   *
   * Use this method if you want to run a query that should return a single result as a Map.
   *
   * This is not intended for running mutations.
   *
   * @param dataTableName The name of the data table to query. Must be a non-empty String.
   * @param filter The filter to apply to the query. Ignored if null or empty.
   * @param attributeNames The list of attribute names to include in the query. Must contain at least one value.
   * @return A Map containing the retrieved data, an empty map if no data is found, or null if there are issues with the query.
   */
  public Map<String, Object> runGraphqlQueryReturnMap(String endpoint, String dataTableName, String filter, List<String> attributeNames) {
    List<Map<String, Object>> retrievedList = runGraphqlQueryReturnList(endpoint, dataTableName, filter, attributeNames);
    if (retrievedList == null) {
      logger.warn("runGraphqlQueryReturnMap: biobankList list is null");
      return null;
    }
    if (retrievedList.size() == 0) {
      logger.info("runGraphqlQueryReturnMap: no results found");
      return new HashMap<>();
    }
    if (retrievedList.size() > 1)
      logger.warn("runGraphqlQueryReturnMap: expecting just a single result, found " + retrievedList.size() + " results");

    Map<String, Object> item = retrievedList.get(0);

    if (item == null) {
      logger.warn("runGraphqlQueryReturnMap: first element of biobankList is null");
      return null;
    }

    boolean missingAttribute = false;
    for (String attributeName : attributeNames)
      if (!item.containsKey(attributeName)) {
        logger.warn("runGraphqlQueryReturnMap: no " + attributeName + " element found");
        missingAttribute = true;
      }
    if (missingAttribute) {
      logger.warn("runGraphqlQueryReturnMap: one or more missing attributes in item: " + Util.jsonStringFomObject(item));
      return null;
    }

    return item;
  }

  /**
   * Runs a GraphQL query against the Directory API to retrieve data for a specific data table with a filter and list of attribute names.
   *
   * Use this method if you want to run a query that should return a list of results.
   *
   * This is not intended for running mutations.
   *
   * @param dataTableName The name of the data table to query. Must be a non-empty String.
   * @param filter The filter to apply to the query. Ignored if null or empty.
   * @param attributeNames The list of attribute names to include in the query. Must contain at least one value.
   * @return A list of maps containing the retrieved data, or null if there are issues with the query.
   */
  public List<Map<String, Object>> runGraphqlQueryReturnList(String endpoint, String dataTableName, String filter, List<String> attributeNames) {
    String grapqlCommand = buildGraphqlQueryString(dataTableName, filter, attributeNames);
    return runGraphqlQueryReturnList(endpoint, grapqlCommand);
  }

  /**
   * Runs a GraphQL command against the Directory API to retrieve data.
   *
   * Use this method if you want to run a query that should return a list of results.
   *
   * This is not intended for running mutations.
   *
   * @param grapqlCommand Full GraphQL command to be run.
   * @return A list of maps containing the retrieved data, or null if there are issues with the query.
   */
  public List<Map<String, Object>> runGraphqlQueryReturnList(String endpoint, String grapqlCommand) {
    JsonObject result = runGraphqlCommand(endpoint, grapqlCommand);

    if (result == null) {
      logger.warn("runGraphqlQueryReturnList: result is null");
      return null;
    }

    logger.info("runGraphqlQueryReturnList: result: " + result);

    Map<String, Object> resultMap = convertJsonObjectToMap(result);

    if (resultMap.isEmpty()) {
      logger.info("runGraphqlQueryReturnList: no matching data found");
      return new ArrayList<>();
    }
    if (resultMap.keySet().size() > 1) {
      // This shouldn't happen
      logger.warn("runGraphqlQueryReturnList: more than one data table name found");
      return null;
    }

    String dataTableName = resultMap.keySet().iterator().next();

    List<Map<String, Object>> retrievedList = (List<Map<String, Object>>) resultMap.get(dataTableName);

    return retrievedList;
  }

  /**
   * Builds a GraphQL query for a specific data table, filter, and list of attribute names.
   *
   * @param dataTableName The name of the data table to query. Must be a non-empty String.
   * @param filter The filter to apply to the query. Ignored if null or empty.
   * @param attributeNames The list of attribute names to include in the query. Must contain at least one value.
   * @return The constructed GraphQL query. Return null on error.
   */
  private String buildGraphqlQueryString(String dataTableName, String filter, List<String> attributeNames) {
    if (dataTableName == null || dataTableName.isEmpty()) {
      logger.warn("buildGraphqlQueryString: dataTableName is null or empty");
      return null;
    }
    if (attributeNames == null || attributeNames.isEmpty()) {
      logger.warn("buildGraphqlQueryString: attributeNames is null or empty");
      return null;
    }

    String bracketedFilter = "";
    if (filter != null && !filter.isEmpty() && !(filter.startsWith("(") && filter.endsWith(")")))
      bracketedFilter = "( " + filter + " )";

    String graphqlAttributes = "";
    if (attributeNames != null && !attributeNames.isEmpty())
      graphqlAttributes = attributeNames.stream().collect(Collectors.joining("\n"));

    String grapqlCommand = "query {\n" +
            "  " + dataTableName + bracketedFilter + " {\n" +
            "    " + graphqlAttributes +
            "  }\n" +
            "}";

    logger.info("buildGraphqlQueryString: grapqlCommand: " + grapqlCommand);

    return grapqlCommand;
  }

  /**
   * Executes a GraphQL command using the root endpoint and returns the result as a JsonObject.
   *
   * @param graphqlCommand The GraphQL command to execute.
   * @return The result of the GraphQL command as a JsonObject.
   */
  public JsonObject runGraphqlCommand(String graphqlCommand) {
    String endpoint = DirectoryEndpointsGraphql.getRootEndpoint();
    return runGraphqlCommand(endpoint, graphqlCommand);
  }

  /**
   * Executes a GraphQL command using a specified endpoint and returns the result as a JsonObject.
   *
   * @param endpoint The endpoint to use for the GraphQL command.
   * @param graphqlCommand The GraphQL command to execute.
   * @return The result of the GraphQL command as a JsonObject.
   */
  public JsonObject runGraphqlCommand(String endpoint, String graphqlCommand) {
    String url = urlCombine(baseUrl, endpoint);
    HttpPost request = new HttpPost(url);
    if (directoryCredentials.getToken() != null && !directoryCredentials.getToken().isEmpty())
      request.setHeader("x-molgenis-token", directoryCredentials.getToken());
    request.setHeader("Content-Type", "application/json");

    // Create the GraphQL body
    String cleanedCommand = escapeQuotes(removeNewlines(graphqlCommand));
    String wrappedCommand = wrapCommandInQuery(cleanedCommand);
    logger.info("runGraphqlCommand: wrappedCommand: " + wrappedCommand);
    StringEntity entity = new StringEntity(wrappedCommand, UTF_8);
    request.setEntity(entity);

    String response = executeRequest(request);

    if (response == null) {
      logger.warn("runGraphqlCommand: HTTP response is null");
      return null;
    }

    // Parse the response (GraphQL responses typically have a "data" field)
    JsonObject jsonResponse = gson.fromJson(response, JsonObject.class);
    JsonObject data = jsonResponse.getAsJsonObject("data");

    if (data == null) {
      logger.warn("graphql: Error in GraphQL response: " + jsonResponse.toString());
      return null;
    }

    return data;
  }

  /**
   * Take a GraphQL string as an argument and returns a JSON string
   * wrapping the command in a query.
   *
   * @param graphqlCommand The GraphQL command to wrap in a query.
   * @return The wrapped GraphQL command as a string.
   */
  private String wrapCommandInQuery(String graphqlCommand) {
    String query = "{ \"query\": \"" + graphqlCommand + "\" }";
    return query;
  }

  /**
   * Escapes double quotes in a string by adding backslashes.
   *
   * @param str The string to escape double quotes in.
   * @return The string with escaped double quotes.
   */
  private String escapeQuotes(String str) {
    return str.replace("\"", "\\\"");
  }

  /**
   * Removes newlines from a string by replacing them with spaces.
   *
   * @param str The string to remove newlines from.
   * @return The string with newlines removed.
   */
  private String removeNewlines(String str) {
    return str.replace("\n", " ").replace("\r", " ");
  }

  /**
   * Converts a JsonObject into a Map<String, Object>.
   *
   * @param jsonObject The JsonObject to convert.
   * @return The converted Map<String, Object>.
   */
  public Map<String, Object> convertJsonObjectToMap(JsonObject jsonObject) {
    return gson.fromJson(jsonObject, new TypeToken<Map<String, Object>>() {}.getType());
  }
}
