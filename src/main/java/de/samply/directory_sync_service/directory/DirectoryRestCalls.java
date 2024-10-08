package de.samply.directory_sync_service.directory;

import com.google.gson.Gson;
import de.samply.directory_sync_service.Util;
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
public class DirectoryRestCalls {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryRestCalls.class);
  private final Gson gson = new Gson();
  private final CloseableHttpClient httpClient = HttpClients.createDefault();
  private final String baseUrl;
  private DirectoryCredentials directoryCredentials;

  /**
   * Constructs a DirectoryRestCalls object.
   * <p>
   * This constructor initializes the HTTP client, base URL, and credentials for interacting with the Directory service.
   * It also triggers the login process to authenticate and obtain a session token.
   *
   * @param baseUrl the base URL for the Directory service
   * @param username the username for Directory authentication
   * @param password the password for Directory authentication
   */
  public DirectoryRestCalls(String baseUrl, String username, String password) {
    this.baseUrl = baseUrl.replaceFirst("/*$", "");
    this.directoryCredentials = new DirectoryCredentials(username, password);
  }

  /**
   * Checks if a given REST endpoint exists by sending an OPTIONS request.
   *
   * @param endpoint the URL of the REST endpoint to check
   * @return true if the endpoint exists, false otherwise
   */
  public boolean endpointExists(String endpoint) {
    String url = urlCombine(baseUrl, endpoint);
    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpOptions request = new HttpOptions(url);

    boolean returnStatus = true;
    try {
      HttpResponse response = httpClient.execute(request);

      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != 200 && statusCode != 204) {
        // The endpoint neither exists nor has the server responded with allowed methods.
        returnStatus = false;
      }
    } catch (IOException e) {
      logger.warn("doesEndpointExist: entity get exception: URI: " + request.getURI().toString() + ", error: " +  Util.traceFromException(e));
      returnStatus = false;
    } finally {
      try {
        httpClient.close();
      } catch (IOException e) {
        logger.warn("doesEndpointExist: entity get exception: URI: " + request.getURI().toString() + ", error: " +  Util.traceFromException(e));
      }
    }

    return returnStatus;
  }

  /**
   * Logs in to the Directory, using local credentials.
   * Updates the token in the directory credentials upon successful login.
   */
  public boolean login() {
    DirectoryCredentials.LoginResponse loginResponse = (DirectoryCredentials.LoginResponse) post(DirectoryRestEndpoints.getLoginEndpoint(), DirectoryCredentials.LoginResponse.class, directoryCredentials.generateLoginCredentials());
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
   * Combines two URL parts, ensuring that there is exactly one slash between them.
   *
   * @param url1 the first part of the URL
   * @param url2 the second part of the URL
   * @return the combined URL as a {@code String}
   */
  private static String urlCombine(String url1, String url2) {
    if (url1.endsWith("/") && url2.startsWith("/")) {
      return url1 + url2.substring(1);
    } else if (url1.endsWith("/") || url2.startsWith("/")) {
      return url1 + url2;
    } else {
      return url1 + "/" + url2;
    }
  }

  /**
   * Executes an HTTP request and returns the response as a string.
   * <p>
   * Logs any exceptions or HTTP errors that occur during the request.
   * </p>
   *
   * @param request the HTTP request to execute
   * @return the response body as a {@code String}, or {@code null} if an error occurs
   */
  private String executeRequest(HttpUriRequest request) {
    String result = null;
    try {
      CloseableHttpResponse response = httpClient.execute(request);
      if (response.getStatusLine().getStatusCode() < 300) {
        HttpEntity httpEntity = response.getEntity();
        result = EntityUtils.toString(httpEntity);
      } else if (response.getStatusLine().getStatusCode() == 404) {
        logger.warn("executeRequest: entity get HTTP error (not found): URI: " + request.getURI().toString() + ", error: " + Integer.toString(response.getStatusLine().getStatusCode()));
      } else
        logger.warn("executeRequest: entity get HTTP error: URI: " + request.getURI().toString() + ", error: " +  Integer.toString(response.getStatusLine().getStatusCode()));
    } catch (IOException e) {
      logger.warn("executeRequest: entity get exception: URI: " + request.getURI().toString() + ", error: " +  Util.traceFromException(e));
    } catch (Exception e) {
      logger.warn("executeRequest: unknown exception: URI: " + request.getURI().toString() + ", error: " +  Util.traceFromException(e));
    }

    return result;
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
