package de.samply.directory_sync_service.directory;

import com.google.gson.Gson;
import de.samply.directory_sync_service.Util;
import io.vavr.control.Either;
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
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class DirectoryRest {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryRest.class);
  private static final Gson gson = new Gson();

  public static Object get(CloseableHttpClient httpClient, String token, String url, Class c) {
    HttpGet request = buildGetRequest(token, url);
    String response = executeRequest(httpClient, request);
    if (response == null)
      return null;
    Object body = gson.fromJson(response, c);

    return body;
  }

  public static Object post(CloseableHttpClient httpClient, String url, Class c, Object o) {
    return post(httpClient, null, url, c, o);
  }

  public static Object post(CloseableHttpClient httpClient, String token, String url, Class c, Object o) {
    String response = post(httpClient, token, url, o);
    if (response == null)
      return null;
    Object body = gson.fromJson(response, c);

    return body;
  }

  public static String post(CloseableHttpClient httpClient, String token, String url, Object o) {
    HttpPost request = buildPostRequest(token, url, o);
    String response = executeRequest(httpClient, request);

    return response;
  }

  public static String put(CloseableHttpClient httpClient, String token, String url, Object o) {
    HttpPut request = buildPutRequest(token, url, o);
    String response = executeRequest(httpClient, request);

    return response;
  }

  public static String delete(CloseableHttpClient httpClient, String token, String url, Object o) {
    HttpDeleteWithBody request = buildDeleteRequest(token, url, o);
    String response = executeRequest(httpClient, request);

    return response;
  }

  private static HttpGet buildGetRequest(String token, String url) {
    HttpGet request = new HttpGet(url);
    if (token != null)
      request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    return request;
  }

  private  static HttpPost buildPostRequest(String token, String url, Object o) {
    StringEntity entity = objectToStringEntity(o);
    HttpPost request = new HttpPost(url);
    if (token != null)
      request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(entity);
    return request;
  }

  private static HttpPut buildPutRequest(String token, String url, Object o) {
    StringEntity entity = objectToStringEntity(o);
    HttpPut request = new HttpPut(url);
    if (token != null)
      request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(entity);
    return request;
  }

  private static HttpDeleteWithBody buildDeleteRequest(String token, String url, Object o) {
    StringEntity entity = objectToStringEntity(o);
    HttpDeleteWithBody request = new HttpDeleteWithBody(url);
    if (token != null)
      request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(entity);
    return request;
  }

  private static StringEntity objectToStringEntity(Object o) {
    String jsonBody = gson.toJson(o);
    StringEntity entity = new StringEntity(jsonBody, UTF_8);

    return entity;
  }

  private static String executeRequest(CloseableHttpClient httpClient, HttpUriRequest request) {
    String result = null;
    try {
      CloseableHttpResponse response = httpClient.execute(request);
      if (response.getStatusLine().getStatusCode() < 300) {
        HttpEntity httpEntity = response.getEntity();
        result = EntityUtils.toString(httpEntity);
      } else if (response.getStatusLine().getStatusCode() == 404) {
        logger.warn("executeRequest: entity get HTTP error (not found): " + Integer.toString(response.getStatusLine().getStatusCode()));
      } else
        logger.warn("executeRequest: entity get HTTP error: " + Integer.toString(response.getStatusLine().getStatusCode()));
    } catch (IOException e) {
      logger.warn("executeRequest: entity get exception: " + Util.traceFromException(e));
    } catch (Exception e) {
      logger.warn("executeRequest: unknown exception: " + Util.traceFromException(e));
    }

    return result;
  }

  /**
   * Custom HTTP DELETE request with a request body support.
   * Used for sending delete requests with a request body to the Directory service.
   */
  static class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase {
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
