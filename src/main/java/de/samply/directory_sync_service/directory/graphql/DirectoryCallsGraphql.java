package de.samply.directory_sync_service.directory.graphql;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryCalls;
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
      logger.warn("runGraphqlQueryReturnMap: no results found");
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
    String graphqlCommand = buildGraphqlQueryString(dataTableName, filter, attributeNames);
    return runGraphqlQueryReturnList(endpoint, graphqlCommand);
  }

  /**
   * Runs a GraphQL command against the Directory API to retrieve data.
   *
   * Use this method if you want to run a query that should return a list of results.
   *
   * This is not intended for running mutations.
   *
   * @param graphqlCommand Full GraphQL command to be run.
   * @return A list of maps containing the retrieved data, or null if there are issues with the query.
   */
  public List<Map<String, Object>> runGraphqlQueryReturnList(String endpoint, String graphqlCommand) {
    JsonObject result = runGraphqlCommand(endpoint, graphqlCommand);

    if (result == null) {
      logger.warn("runGraphqlQueryReturnList: result is null");
      return null;
    }

    Map<String, Object> resultMap = convertJsonObjectToMap(result);

    if (resultMap.isEmpty()) {
      logger.debug("runGraphqlQueryReturnList: no matching data found");
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

    String graphqlCommand = "query {\n" +
            "  " + dataTableName + bracketedFilter + " {\n" +
            "    " + graphqlAttributes +
            "  }\n" +
            "}";

    return graphqlCommand;
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

    // Create the GraphQL body
    String cleanedCommand = escapeQuotes(removeNewlines(graphqlCommand));
    String wrappedCommand = wrapCommandInQuery(cleanedCommand);
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
      logger.warn("graphql: Error in GraphQL response: " + jsonResponse);
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
