package de.samply.directory_sync_service.directory;

import com.google.gson.Gson;
import de.samply.directory_sync_service.Util;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DirectoryRest {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryRest.class);
  private final Gson gson = new Gson();
  private final CloseableHttpClient httpClient;
  private final String baseUrl;
  private DirectoryCredentials directoryCredentials;

  public DirectoryRest(CloseableHttpClient httpClient, String baseUrl, String username, String password) {
    this.httpClient = httpClient;
    this.baseUrl = baseUrl.replaceFirst("/*$", "");
    this.directoryCredentials = new DirectoryCredentials(username, password);
    login();
  }

  public void login() {
    DirectoryCredentials.LoginResponse loginResponse = (DirectoryCredentials.LoginResponse) post("/api/v1/login", DirectoryCredentials.LoginResponse.class, directoryCredentials.generateLoginCredentials());
    if (loginResponse != null)
      directoryCredentials.setToken(loginResponse.token);
  }

  public Object get(String commandUrl, Class c) {
    HttpGet request = buildGetRequest(commandUrl);
    String response = executeRequest(request);
    if (response == null)
      return null;
    Object body = gson.fromJson(response, c);

    return body;
  }

  public Object post(String commandUrl, Class c, Object o) {
    String response = post(commandUrl, o);
    if (response == null)
      return null;
    Object body = gson.fromJson(response, c);

    return body;
  }

  public String post(String commandUrl, Object o) {
    HttpPost request = buildPostRequest(commandUrl, o);
    String response = executeRequest(request);

    return response;
  }

  public String put(String commandUrl, Object o) {
    HttpPut request = buildPutRequest(commandUrl, o);
    String response = executeRequest(request);

    return response;
  }

  public String delete(String commandUrl, Object o) {
    HttpDeleteWithBody request = buildDeleteRequest(commandUrl, o);
    String response = executeRequest(request);

    return response;
  }

  private HttpGet buildGetRequest(String commandUrl) {
    HttpGet request = buildTokenlessGetRequest(commandUrl);
    request.setHeader("x-molgenis-token", directoryCredentials.getToken());
    return request;
  }

  private HttpGet buildTokenlessGetRequest(String commandUrl) {
    HttpGet request = new HttpGet(urlCombine(baseUrl, commandUrl));
    request.setHeader("Accept", "application/json");
    return request;
  }

  private HttpPost buildPostRequest(String commandUrl, Object o) {
    StringEntity entity = objectToStringEntity(o);
    HttpPost request = new HttpPost(urlCombine(baseUrl, commandUrl));
    request.setHeader("x-molgenis-token", directoryCredentials.getToken());
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(entity);
    return request;
  }

  private HttpPut buildPutRequest(String commandUrl, Object o) {
    StringEntity entity = objectToStringEntity(o);
    HttpPut request = new HttpPut(urlCombine(baseUrl, commandUrl));
    request.setHeader("x-molgenis-token", directoryCredentials.getToken());
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(entity);
    return request;
  }

  private HttpDeleteWithBody buildDeleteRequest(String commandUrl, Object o) {
    StringEntity entity = objectToStringEntity(o);
    HttpDeleteWithBody request = new HttpDeleteWithBody(urlCombine(baseUrl, commandUrl));
    request.setHeader("x-molgenis-token", directoryCredentials.getToken());
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(entity);
    return request;
  }

  private StringEntity objectToStringEntity(Object o) {
    String jsonBody = gson.toJson(o);
    StringEntity entity = new StringEntity(jsonBody, UTF_8);

    return entity;
  }

  // method  to combine URIs, ensuring that they are appended with a single slash between them
  private static String urlCombine(String url1, String url2) {
    if (url1.endsWith("/") && url2.startsWith("/")) {
      return url1 + url2.substring(1);
    } else if (url1.endsWith("/") || url2.startsWith("/")) {
      return url1 + url2;
    } else {
      return url1 + "/" + url2;
    }
  }

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
