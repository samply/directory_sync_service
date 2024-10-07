package de.samply.directory_sync_service.directory.graphql;

import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import de.samply.directory_sync_service.directory.DirectoryCalls;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import java.util.Map;

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
