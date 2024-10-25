package de.samply.directory_sync_service.directory.rest;

import com.google.gson.Gson;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.DirectoryCalls;
import de.samply.directory_sync_service.directory.DirectoryCredentials;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Class for interacting with a Directory service via REST API calls.
 * <p>
 * It provides methods to perform HTTP GET, POST, PUT, and DELETE operations on a Directory service.
 * It handles authentication via a login method and manages session tokens for authorized requests.
 */
public class DirectoryCallsRest extends DirectoryCalls {
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
  public DirectoryCallsRest(String baseUrl, String username, String password) {
    super(baseUrl, username, password);
  }

  /**
   * Logs in to the Directory, using local credentials.
   * Updates the token in the directory credentials upon successful login.
   */
  public boolean login() {
    DirectoryCredentials.LoginResponse loginResponse = (DirectoryCredentials.LoginResponse) post(new DirectoryEndpointsRest().getLoginEndpoint(), DirectoryCredentials.LoginResponse.class, directoryCredentials.generateLoginCredentials());
    if (loginResponse == null) {
      logger.error("login: failed to log in to Directory");
      return false;
    } else
      directoryCredentials.setToken(loginResponse.token);
    return true;
  }

  /**
   * Sends a GET request to the Directory service.
   *
   * @param commandUrl the URL path to send the GET request to
   * @param c          the class type to which the response should be deserialized
   * @return the response body deserialized into an object of type {@code c}, or {@code null} if the response is empty
   */
  public Object get(String commandUrl, Class c) {
    HttpGet request = buildGetRequest(commandUrl);
    String response = executeRequest(request);
    if (response == null)
      return null;
    Object body = gson.fromJson(response, c);

    return body;
  }

  /**
   * Sends a POST request to the Directory service and deserializes the response.
   *
   * @param commandUrl the URL path to send the POST request to
   * @param c          the class type to which the response should be deserialized
   * @param o          the request body object to be sent
   * @return the response body deserialized into an object of type {@code c}, or {@code null} if the response is empty
   */
  public Object post(String commandUrl, Class c, Object o) {
    String response = post(commandUrl, o);
    if (response == null)
      return null;
    Object body = gson.fromJson(response, c);

    return body;
  }

  /**
   * Sends a POST request to the Directory service.
   *
   * @param commandUrl the URL path to send the POST request to
   * @param o          the request body object to be sent
   * @return the response as a {@code String}, or {@code null} if the response is empty
   */
  public String post(String commandUrl, Object o) {
    HttpPost request = buildPostRequest(commandUrl, o);
    String response = executeRequest(request);

    return response;
  }

  /**
   * Sends a PUT request to the Directory service.
   *
   * @param commandUrl the URL path to send the PUT request to
   * @param o          the request body object to be sent
   * @return the response as a {@code String}, or {@code null} if the response is empty
   */
  public String put(String commandUrl, Object o) {
    HttpPut request = buildPutRequest(commandUrl, o);
    String response = executeRequest(request);

    return response;
  }

  /**
   * Sends a DELETE request to the Directory service with a request body.
   *
   * @param commandUrl the URL path to send the DELETE request to
   * @param o          the request body object to be sent
   * @return the response as a {@code String}, or {@code null} if the response is empty
   */
  public String delete(String commandUrl, Object o) {
    HttpDeleteWithBody request = buildDeleteRequest(commandUrl, o);
    String response = executeRequest(request);

    return response;
  }

  /**
   * Builds an HTTP GET request with the session token included in the header.
   *
   * @param commandUrl the URL path for the GET request
   * @return an {@code HttpGet} object configured with the necessary headers
   */
  private HttpGet buildGetRequest(String commandUrl) {
    HttpGet request = buildTokenlessGetRequest(commandUrl);
    request.setHeader("x-molgenis-token", directoryCredentials.getToken());
    return request;
  }

  /**
   * Builds an HTTP GET request without including the session token in the header.
   *
   * @param commandUrl the URL path for the GET request
   * @return an {@code HttpGet} object configured with the necessary headers
   */
  private HttpGet buildTokenlessGetRequest(String commandUrl) {
    HttpGet request = new HttpGet(urlCombine(baseUrl, commandUrl));
    request.setHeader("Accept", "application/json");
    return request;
  }

  /**
   * Builds an HTTP POST request with the session token and request body.
   *
   * @param commandUrl the URL path for the POST request
   * @param o          the object to be serialized into the request body
   * @return an {@code HttpPost} object configured with the necessary headers and body
   */
  private HttpPost buildPostRequest(String commandUrl, Object o) {
    StringEntity entity = objectToStringEntity(o);
    HttpPost request = new HttpPost(urlCombine(baseUrl, commandUrl));
    request.setHeader("x-molgenis-token", directoryCredentials.getToken());
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(entity);
    return request;
  }

  /**
   * Builds an HTTP PUT request with the session token and request body.
   *
   * @param commandUrl the URL path for the PUT request
   * @param o          the object to be serialized into the request body
   * @return an {@code HttpPut} object configured with the necessary headers and body
   */
  private HttpPut buildPutRequest(String commandUrl, Object o) {
    StringEntity entity = objectToStringEntity(o);
    HttpPut request = new HttpPut(urlCombine(baseUrl, commandUrl));
    request.setHeader("x-molgenis-token", directoryCredentials.getToken());
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(entity);
    return request;
  }

  /**
   * Builds an HTTP DELETE request with a request body.
   *
   * @param commandUrl the URL path for the DELETE request
   * @param o          the object to be serialized into the request body
   * @return an {@code HttpDeleteWithBody} object configured with the necessary headers and body
   */
  private HttpDeleteWithBody buildDeleteRequest(String commandUrl, Object o) {
    StringEntity entity = objectToStringEntity(o);
    HttpDeleteWithBody request = new HttpDeleteWithBody(urlCombine(baseUrl, commandUrl));
    request.setHeader("x-molgenis-token", directoryCredentials.getToken());
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(entity);
    return request;
  }

  /**
   * Converts an object to a JSON string and wraps it in a {@code StringEntity}.
   *
   * @param o the object to convert
   * @return a {@code StringEntity} containing the serialized JSON
   */
  private StringEntity objectToStringEntity(Object o) {
    String jsonBody = gson.toJson(o);
    StringEntity entity = new StringEntity(jsonBody, UTF_8);

    return entity;
  }

  /**
   * Custom HTTP DELETE request with a request body support.
   * Used for sending delete requests with a request body to the Directory service.
   * <p>
   * This class extends {@code HttpEntityEnclosingRequestBase} to allow sending a body with a DELETE request.
   */
  class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase {
    public static final String METHOD_NAME = "DELETE";

    public HttpDeleteWithBody(final String uri) {
      super();
      setURI(URI.create(uri));
    }

    @Override
    public String getMethod() {
      return METHOD_NAME;
    }
  }
}
