package de.samply.directory_sync_service.directory;

import com.google.gson.Gson;
import de.samply.directory_sync_service.Util;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Class for interacting with a Directory service via REST API calls.
 * <p>
 * It provides methods to perform HTTP GET, POST, PUT, and DELETE operations on a Directory service.
 * It handles authentication via a login method and manages session tokens for authorized requests.
 */
public abstract class DirectoryCalls {
  protected static final Logger logger = LoggerFactory.getLogger(DirectoryCalls.class);
  protected final Gson gson = new Gson();
  protected final CloseableHttpClient httpClient = HttpClients.createDefault();
  protected final String baseUrl;
  public DirectoryCredentials directoryCredentials;

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
  public DirectoryCalls(String baseUrl, String username, String password) {
    this.baseUrl = baseUrl.replaceFirst("/*$", "");
    this.directoryCredentials = new DirectoryCredentials(username, password);
  }

  public void setToken(String token) {
    this.directoryCredentials.setToken(token);
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
  protected String executeRequest(HttpUriRequest request) {
    String result = null;
    try {
      CloseableHttpResponse response = httpClient.execute(request);
      if (response.getStatusLine().getStatusCode() < 300) {
        HttpEntity httpEntity = response.getEntity();
        if (httpEntity == null)
          logger.warn("executeRequest: entity is null" );
        else
          result = EntityUtils.toString(httpEntity);
      } else if (response.getStatusLine().getStatusCode() == 404) {
        logger.warn("executeRequest: entity get HTTP error (not found): URI: " + request.getURI().toString() + ", error: " + response.getStatusLine().getStatusCode());
      } else {
        logger.warn("executeRequest: entity get HTTP error: URI: " + request.getURI().toString() + ", error: " + response.getStatusLine().getStatusCode() + response.getStatusLine().getReasonPhrase());
        for (Header header: request.getAllHeaders())
          logger.warn("executeRequest: header: |" + header.toString() + "|");
      }
    } catch (IOException e) {
      logger.warn("executeRequest: entity get exception: URI: " + request.getURI().toString() + ", error: " +  Util.traceFromException(e));
    } catch (Exception e) {
      logger.warn("executeRequest: unknown exception: URI: " + request.getURI().toString() + ", error: " +  Util.traceFromException(e));
    }

    return result;
  }

  /**
   * Combines two URL parts, ensuring that there is exactly one slash between them.
   *
   * @param url1 the first part of the URL
   * @param url2 the second part of the URL
   * @return the combined URL as a {@code String}
   */
  public static String urlCombine(String url1, String url2) {
    if (url1.endsWith("/") && url2.startsWith("/")) {
      return url1 + url2.substring(1);
    } else if (url1.endsWith("/") || url2.startsWith("/")) {
      return url1 + url2;
    } else {
      return url1 + "/" + url2;
    }
  }
}
