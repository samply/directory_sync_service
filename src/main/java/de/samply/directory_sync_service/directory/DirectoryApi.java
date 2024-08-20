package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.model.StarModelData;
import de.samply.directory_sync_service.Util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.INFORMATION;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueType.NOTFOUND;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import de.samply.directory_sync_service.directory.model.BbmriEricId;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import io.vavr.control.Either;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import java.net.URI;

public class DirectoryApi {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryApi.class);

  private final CloseableHttpClient httpClient;
  private final String baseUrl;
  private final String token;
  private final Gson gson = new Gson();
  private String username;
  private String password;

  // Setting this variable to true will prevent any contact being made to the Directory.
  // All public methods will return feasible fake results.
  private boolean mockDirectory = false;

  private DirectoryApi(CloseableHttpClient httpClient, String baseUrl, String token, boolean mockDirectory) {
    this.httpClient = Objects.requireNonNull(httpClient);
    this.baseUrl = Objects.requireNonNull(baseUrl);
    this.mockDirectory = mockDirectory;
    if (mockDirectory)
      // if we are mocking, then we don't need to check token, because it won't get used anyway.
      this.token = token;
    else {
      if (token == null)
        logger.warn("No token provided, directory operations will not be logged in.");
      this.token = Objects.requireNonNull(token);
    }
  }

/*
  public static Either<OperationOutcome, DirectoryApi> createWithLogin(
          CloseableHttpClient httpClient,
          String baseUrl,
          String username,
          String password,
          boolean mockDirectory) {
    return login(httpClient, baseUrl.replaceFirst("/*$", ""), username, password, mockDirectory)
            .map(response -> createWithToken(httpClient, baseUrl, response.token, mockDirectory).setUsernameAndPassword(username, password));
  }
*/

  public static Either<OperationOutcome, DirectoryApi> createWithLogin(
          CloseableHttpClient httpClient,
          String baseUrl,
          String username,
          String password,
          boolean mockDirectory) {

    // Clean up the baseUrl
    String cleanedBaseUrl = baseUrl.replaceFirst("/*$", "");

    logger.info("createWithLogin: cleanedBaseUrl: " + cleanedBaseUrl);

    // Perform the login operation and get the result
    Either<OperationOutcome, LoginResponse> loginResult = login(httpClient, cleanedBaseUrl, username, password, mockDirectory);

    // If login is successful, map the result to a DirectoryApi, otherwise return the error
    Either<OperationOutcome, DirectoryApi> finalResult = loginResult.map(response -> {
      logger.info("createWithLogin: log in to Directory apparently succeeded");
      if (response.token == null)
        logger.warn("createWithLogin: response.token is null");
      // Create the DirectoryApi with the token and set the username and password
      DirectoryApi api = createWithToken(httpClient, cleanedBaseUrl, response.token, mockDirectory);
      return api.setUsernameAndPassword(username, password);
    });

    logger.warn("createWithLogin: log in to Directory failed: " + finalResult);

    return finalResult;
  }

  private static Either<OperationOutcome, LoginResponse> login(CloseableHttpClient httpClient,
      String baseUrl,
      String username, String password, boolean mockDirectory) {
    if (mockDirectory)
      // Don't try logging in if we are mocking
      return Either.right(new LoginResponse());
    HttpPost request = loginRequest(baseUrl, username, password);
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      return Either.right(decodeLoginResponse(response));
    } catch (IOException e) {
      return Either.left(error("login", e.getMessage()));
    }
  }

  /**
   * Store username and password internally, to allow relogin where necessary.
   *
   * @param username
   * @param password
   * @return
   */
  private DirectoryApi setUsernameAndPassword(String username, String password) {
    this.username = username;
    this.password = password;

    return this;
  }

  /**
   * Log back in to the Directory. This is typically used in situations where there has
   * been a long pause since the last API call to the Directory. It returns a fresh
   * DirectoryApi object, which you should use to replace the existing one.
   *
   * Returns null if something goes wrong.
   *
   * @return new DirectoryApi object.
   */
  public DirectoryApi relogin() {
    logger.info("relogin: logging back in");
    HttpPost request = loginRequest(baseUrl, username, password);

    if (mockDirectory)
      // In a mocking situation, don't try to log back in. We can safely
      // return the old DirectoryApi object because nothing will have changed.
      return this;

    String token = null;
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      LoginResponse loginResponse = decodeLoginResponse(response);
      token = loginResponse.token;
      if (token == null) {
        logger.warn("relogin: got a null token back from the Directory, returning null.");
        return null;
      }
    } catch (IOException e) {
      logger.warn("relogin: exception: " + Util.traceFromException(e));
      return null;
    }

    return new DirectoryApi(httpClient, baseUrl.replaceFirst("/*$", ""), token, mockDirectory).setUsernameAndPassword(username, password);
  }

  private static HttpPost loginRequest(String baseUrl, String username, String password) {
    HttpPost request = new HttpPost(baseUrl + "/api/v1/login");
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(encodeLoginCredentials(username, password));
    return request;
  }

  private static StringEntity encodeLoginCredentials(String username, String password) {
    return new StringEntity(new Gson().toJson(new LoginCredentials(username, password)), UTF_8);
  }

  private static LoginResponse decodeLoginResponse(CloseableHttpResponse tokenResponse)
      throws IOException {
    String body = EntityUtils.toString(tokenResponse.getEntity(), UTF_8);
    return new Gson().fromJson(body, LoginResponse.class);
  }

  public static DirectoryApi createWithToken(CloseableHttpClient httpClient, String baseUrl,
                                             String token) {
    return createWithToken(httpClient, baseUrl, token, false);
  }

  public static DirectoryApi createWithToken(CloseableHttpClient httpClient, String baseUrl,
                                             String token, boolean mockDirectory) {
    return new DirectoryApi(httpClient, baseUrl.replaceFirst("/*$", ""), token, mockDirectory);
  }

  private static OperationOutcome error(String action, String message) {
    OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue().setSeverity(ERROR).setDiagnostics(errorMsg(action, message));
    return outcome;
  }

  private static String errorMsg(String action, String message) {
    return String.format("Error in BBMRI Directory response for %s, cause: %s", action,
        message);
  }

  private static OperationOutcome biobankNotFound(BbmriEricId id) {
    OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue()
        .setSeverity(INFORMATION)
        .setCode(NOTFOUND)
        .setDiagnostics(String.format("No Biobank in Directory with id `%s`.", id));
    return outcome;
  }

  private static OperationOutcome updateSuccessful(int number) {
    OperationOutcome outcome = new OperationOutcome();
    outcome.addIssue()
        .setSeverity(INFORMATION)
        .setDiagnostics(String.format("Successful update of %d collection size values.", number));
    return outcome;
  }

  /**
   * Fetches the Biobank with the given {@code id}.
   *
   * @param id the ID of the Biobank to fetch.
   * @return either the Biobank or an error
   */
  public Either<OperationOutcome, Biobank> fetchBiobank(BbmriEricId id) {
    try (CloseableHttpResponse response = httpClient.execute(fetchBiobankRequest(id))) {
      if (response.getStatusLine().getStatusCode() == 200) {
        String payload = EntityUtils.toString(response.getEntity(), UTF_8);
        return Either.right(gson.fromJson(payload, Biobank.class));
      } else if (response.getStatusLine().getStatusCode() == 404) {
        return Either.left(biobankNotFound(id));
      } else {
        String message = EntityUtils.toString(response.getEntity(), UTF_8);
        return Either.left(error(id.toString(), message));
      }
    } catch (IOException e) {
      return Either.left(error(id.toString(), e.getMessage()));
    }
  }

  private HttpGet fetchBiobankRequest(BbmriEricId id) {
    HttpGet request = new HttpGet(
        baseUrl + "/api/v2/eu_bbmri_eric_" + id.getCountryCode() + "_biobanks/" + id);
    request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    return request;
  }

  /**
   * Send the collection sizes to the Directory.
   * <p>
   * Push the counts back to the Directory. You need 'update data' permission on entity type
   * 'Collections' at the Directory in order for this to work.
   *
   * @param countryCode        the country code of the endpoint of the national node, e.g. Germany
   * @param collectionSizeDtos the individual collection sizes. note that all collection must share
   *                           the given {@code countryCode}
   * @return an outcome, either successful or an error
   */
  public OperationOutcome updateCollectionSizes(String countryCode,
      List<CollectionSizeDto> collectionSizeDtos) {

    HttpPut request = updateCollectionSizesRequest(countryCode, collectionSizeDtos);

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      if (response.getStatusLine().getStatusCode() < 300) {
        return updateSuccessful(collectionSizeDtos.size());
      } else {
        return error("collection size update status code " + response.getStatusLine().getStatusCode(), EntityUtils.toString(response.getEntity(), UTF_8));
      }
    } catch (IOException e) {
      return error("collection size update exception", e.getMessage());
    }
  }

  private HttpPut updateCollectionSizesRequest(String countryCode,
      List<CollectionSizeDto> collectionSizeDtos) {
    HttpPut request = new HttpPut(
        baseUrl + "/api/v2/eu_bbmri_eric_collections/size");
    request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(new StringEntity(gson.toJson(new EntitiesDto<>(collectionSizeDtos)), UTF_8));
    return request;
  }

  /**
   * Make a call to the Directory to get all Collection IDs for the supplied {@code countryCode}.
   *
   * @param countryCode the country code of the endpoint of the national node, e.g. DE
   * @return all the Collections for the national node. E.g. "DE" will return all German collections
   */
  public Either<OperationOutcome, Set<BbmriEricId>> listAllCollectionIds(String countryCode) {
    return fetchIdItems(listAllCollectionIdsRequest(countryCode), "list collection ids")
        .map(i -> i.items.stream()
            .map(e -> e.id)
            .map(BbmriEricId::valueOf)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet()));
  }

  private HttpGet listAllCollectionIdsRequest(String countryCode) {
    // If you simply specify "attrs=id", you will only get the first 100
    // IDs. Setting "start" to 0 and "num" its maximum allowed value
    // gets them all. Note that in the current Directory implementation
    // (12.10.2021), the maximum allowed value of "num" is 10000.
    // TODO: to really get all collections, we have to implement paging
    HttpGet request = new HttpGet(
        baseUrl + "/api/v2/eu_bbmri_eric_collections?attrs=id&start=0&num=10000&q=country=="
            + countryCode);
    request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    return request;
  }

  /**
   * Make API calls to the Directory to fill a DirectoryCollectionGet object containing attributes
   * for all of the collections listed in collectionIds. The countryCode is used solely for
   * constructing the URL for the API call.
   * 
   * @param countryCode E.g. "DE".
   * @param collectionIds IDs of the collections whose data will be harvested.
   * @return
   */
  public Either<OperationOutcome, DirectoryCollectionGet> fetchCollectionGetOutcomes(String countryCode, List<String> collectionIds) {
    DirectoryCollectionGet directoryCollectionGet = new DirectoryCollectionGet(); // for all collections retrieved from Directory
    directoryCollectionGet.init(); 
    for (String collectionId: collectionIds) {
      try {
        HttpGet request = fetchCollectionsRequest(countryCode, collectionId);

        if (mockDirectory) {
          // Dummy return if we're in mock mode
          directoryCollectionGet.setMockDirectory(true);
          return Either.right(directoryCollectionGet);
        }

        CloseableHttpResponse response = httpClient.execute(request);
        if (response.getStatusLine().getStatusCode() < 300) {
          HttpEntity httpEntity = response.getEntity();
          String json = EntityUtils.toString(httpEntity);
          DirectoryCollectionGet singleDirectoryCollectionGet = gson.fromJson(json, DirectoryCollectionGet.class);
          Map item = singleDirectoryCollectionGet.getItemZero(); // assume that only one collection matches collectionId
          if (item == null)
            	return Either.left(error("fetchCollectionGetOutcomes: entity get item is null, does the collection exist in the Directory: ", collectionId));
          directoryCollectionGet.getItems().add(item);
        } else
          return Either.left(error("fetchCollectionGetOutcomes: entity get HTTP error", Integer.toString(response.getStatusLine().getStatusCode())));
      } catch (IOException e) {
          return Either.left(error("fetchCollectionGetOutcomes: entity get exception", Util.traceFromException(e)));
      } catch (Exception e) {
          return Either.left(error("fetchCollectionGetOutcomes: unknown exception", Util.traceFromException(e)));
      }
    }

    return Either.right(directoryCollectionGet);
  }

  private HttpGet fetchCollectionsRequest(String countryCode, String collectionId) {
    String url = buildCollectionApiUrl(countryCode) + "?q=id==%22" + collectionId  + "%22";

    logger.info("DirectoryApi.fetchCollectionsRequest: url=" + url);

    HttpGet request = new HttpGet(url);
    request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");

    logger.info("DirectoryApi.fetchCollectionsRequest: request successfully built");

    return request;
  }

  /**
   * Send aggregated collection information to the Directory.
   *
   * @param directoryCollectionPut Summary information about one or more collections
   * @return an outcome, either successful or an error
   */
  public OperationOutcome updateEntities(DirectoryCollectionPut directoryCollectionPut) {
    logger.info("DirectoryApi.updateEntities: entered");

    HttpPut request = updateEntitiesRequest(directoryCollectionPut);

    logger.info("DirectoryApi.updateEntities: url=" + request.getURI());

    if (mockDirectory)
      // Dummy return if we're in mock mode
      return updateSuccessful(directoryCollectionPut.size());

    logger.info("DirectoryApi.updateEntities: try things");

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      logger.info("DirectoryApi.updateEntities: well, now Im in a try statement!");
      if (response.getStatusLine().getStatusCode() < 300) {
        logger.info("DirectoryApi.updateEntities: status code: " + response.getStatusLine().getStatusCode());
        return updateSuccessful(directoryCollectionPut.size());
      } else {
        logger.info("DirectoryApi.updateEntities: returning an error");
        return error("entity update status code " + response.getStatusLine().getStatusCode(), EntityUtils.toString(response.getEntity(), UTF_8));
      }
    } catch (IOException e) {
      logger.info("DirectoryApi.updateEntities: returning an exception: " + Util.traceFromException(e));
      return error("entity update exception", e.getMessage());
    }
  }

  private HttpPut updateEntitiesRequest(DirectoryCollectionPut directoryCollectionPut) {
    HttpPut request = new HttpPut(buildCollectionApiUrl(directoryCollectionPut.getCountryCode()));
    request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    logger.info("updateEntitiesRequest: directoryCollectionPut: " + gson.toJson(directoryCollectionPut));
    request.setEntity(new StringEntity(gson.toJson(directoryCollectionPut), UTF_8));
    return request;
  }

  /**
   * Updates the Star Model data in the Directory service based on the provided StarModelInputData.
   * 
   * Before sending any star model data to the Directory, the original
   * star model data for all known collections will be deleted from the
   * Directory.
   *
   * @param starModelInputData The input data for updating the Star Model.
   * @return An OperationOutcome indicating the success or failure of the update.
   */
  public OperationOutcome updateStarModel(StarModelData starModelInputData) {
    // Get rid of previous star models first. This is necessary, because:
    // 1. A new star model may be decomposed into different hypercubes.
    // 2. The new fact IDs may be different from the old ones.
    // 3. We will be using a POST and it will return an error if we try
    //    to overwrite an existing fact.
    OperationOutcome deleteOutcome = deleteStarModel(starModelInputData);
    if (deleteOutcome.getIssue().size() > 0) {
      logger.warn("updateStarModel: Problem deleting star models");
      return deleteOutcome;
    }

    String countryCode = starModelInputData.getCountryCode();
    List<Map<String, String>> factTables = starModelInputData.getFactTables();
    int blockSize = 1000;

    // Break the fact table into blocks of 1000 before sending to the Directory.
    // This is the maximum number of facts allowed per Directory API call.
    for (int i = 0; i < factTables.size(); i += blockSize) {
      List<Map<String, String>> factTablesBlock = factTables.subList(i, Math.min(i + blockSize, factTables.size()));

      // Now push the new data
      HttpPost request = updateStarModelRequestBlock(countryCode, factTablesBlock);

      if (mockDirectory)
        // Dummy return if we're in mock mode
        return updateSuccessful(starModelInputData.getFactCount());

      try (CloseableHttpResponse response = httpClient.execute(request)) {
        if (response.getStatusLine().getStatusCode() >= 300)
          return error("entity update status code " + response.getStatusLine().getStatusCode(), EntityUtils.toString(response.getEntity(), UTF_8));
      } catch (IOException e) {
        return error("entity update exception", e.getMessage());
      }
    }

    return updateSuccessful(starModelInputData.getFactCount());
  }

  /**
   * Constructs an HTTP POST request for updating Star Model data based on the provided StarModelInputData.
   *
   * @param countryCode
   * @param factTablesBlock
   * @return An HttpPost request object.
   */
  private HttpPost updateStarModelRequestBlock(String countryCode, List<Map<String, String>> factTablesBlock) {
    HttpPost request = new HttpPost(buildApiUrl(countryCode, "facts"));
    // Directory likes to have its data wrapped in a map with key "entities".
    Map<String,Object> body = new HashMap<String,Object>();
    body.put("entities", factTablesBlock);
    String jsonBody = gson.toJson(body);
    request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(new StringEntity(jsonBody, UTF_8));
    return request;
  }

  /**
   * Deletes existing star models from the Directory service for each of the collection IDs in the supplied StarModelInputData object.
   *
   * @param starModelInputData The input data for deleting existing star models.
   * @return An OperationOutcome indicating the success or failure of the deletion.
   */
  private OperationOutcome deleteStarModel(StarModelData starModelInputData) {
    String apiUrl = buildApiUrl(starModelInputData.getCountryCode(), "facts");

    if (mockDirectory)
      // Dummy return if we're in mock mode
      return new OperationOutcome();

    try {
      for (String collectionId: starModelInputData.getInputCollectionIds()) {
        List<String> factIds;
        // Loop until no more facts are left in the Directory.
        // We need to do things this way, because the Directory implements paging
        // and a single pass may not get all facts.
        do {
          // First get a list of fact IDs for this collection
          Map factWrapper = fetchFactWrapperByCollection(apiUrl, collectionId);
          if (factWrapper == null)
            return error("deleteStarModel: Problem getting facts for collection, factWrapper == null, collectionId=", collectionId);
          if (!factWrapper.containsKey("items"))
            return error("deleteStarModel: Problem getting facts for collection, no item key present: ", collectionId);
          List<Map<String, String>> facts = (List<Map<String, String>>) factWrapper.get("items");
          if (facts.size() == 0)
            break;
          factIds = facts.stream()
            .map(map -> map.get("id"))
            .collect(Collectors.toList());

          // Take the list of fact IDs and delete all of the corresponding facts
          // at the Directory.
          OperationOutcome deleteOutcome = deleteFactsByIds(apiUrl, factIds);
          if (deleteOutcome.getIssue().size() > 0)
            return deleteOutcome;
        } while (true);
      }
    } catch(Exception e) {
      return error("deleteStarModel: Exception during delete", Util.traceFromException(e));
    }

    return new OperationOutcome();
  }

  /**
   * Fetches the fact wrapper object by collection from the Directory service.
   *
   * @param apiUrl        The base URL for the Directory API.
   * @param collectionId  The ID of the collection for which to fetch the fact wrapper.
   * @return A Map representing the fact wrapper retrieved from the Directory service.
   */
  public Map fetchFactWrapperByCollection(String apiUrl, String collectionId) {
    Map body = null;
    try {
      HttpGet request = fetchFactWrapperByCollectionRequest(apiUrl, collectionId);

      CloseableHttpResponse response = httpClient.execute(request);
      if (response.getStatusLine().getStatusCode() < 300) {
        HttpEntity httpEntity = response.getEntity();
        String json = EntityUtils.toString(httpEntity);
        body = gson.fromJson(json, Map.class);
      } else
        logger.warn("fetchFactWrapperByCollection: entity get HTTP error: " + Integer.toString(response.getStatusLine().getStatusCode()) + ", apiUrl=" + apiUrl + ", collectionId=" + collectionId);
    } catch (IOException e) {
      logger.warn("fetchFactWrapperByCollection: entity get exception: " + Util.traceFromException(e));
    } catch (Exception e) {
      logger.warn("fetchFactWrapperByCollection: unknown exception: " + Util.traceFromException(e));
    }

    return body;
  }

  /**
   * Constructs an HTTP GET request for fetching the fact wrapper object by collection from the Directory service.
   *
   * @param apiUrl        The base URL for the Directory API.
   * @param collectionId  The ID of the collection for which to fetch the fact wrapper.
   * @return An HttpGet request object.
   */
  private HttpGet fetchFactWrapperByCollectionRequest(String apiUrl, String collectionId) {
    String url = apiUrl + "?q=collection==%22" + collectionId + "%22";
    logger.info("fetchFactWrapperByCollectionRequest: url=" + url);
    HttpGet request = new HttpGet(url);
    request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    return request;
  }

  public void runTestQuery() {
    try {
      String url = "https://bbmritestnn.gcc.rug.nl/api/v2/eu_bbmri_eric_DE_collections?q=id==%22bbmri-eric:ID:DE_DKFZ_TEST:collection:Test1%22";
      logger.info("runTestQuery: url=" + url);
      HttpGet request = new HttpGet(url);
      request.setHeader("x-molgenis-token", token);
      request.setHeader("Accept", "application/json");
      request.setHeader("Content-type", "application/json");

      CloseableHttpResponse response = httpClient.execute(request);
      if (response.getStatusLine().getStatusCode() < 300) {
        HttpEntity httpEntity = response.getEntity();
        String json = EntityUtils.toString(httpEntity);
        logger.info("runTestQuery: SUCCESS, json=" + json);
      } else
        logger.warn("runTestQuery: FAILURE, entity get HTTP error: " + Integer.toString(response.getStatusLine().getStatusCode()));
    } catch (IOException e) {
      logger.warn("runTestQuery: FAILURE, entity get exception: " + Util.traceFromException(e));
    } catch (Exception e) {
      logger.warn("runTestQuery: FAILURE, unknown exception: " + Util.traceFromException(e));
    }
  }

  /**
   * Deletes facts from the Directory service based on a list of fact IDs.
   *
   * @param apiUrl    The base URL for the Directory API.
   * @param factIds   The list of fact IDs to be deleted.
   * @return An OperationOutcome indicating the success or failure of the deletion.
   */
  public OperationOutcome deleteFactsByIds(String apiUrl, List<String> factIds) {
    if (factIds.size() == 0)
      // Nothing to delete
      return new OperationOutcome();

    HttpDeleteWithBody request = deleteFactsByIdsRequest(apiUrl, factIds);

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      if (response.getStatusLine().getStatusCode() < 300) {
        return new OperationOutcome();
      } else {
        return error("entity delete status code " + response.getStatusLine().getStatusCode(), EntityUtils.toString(response.getEntity(), UTF_8));
      }
    } catch (IOException e) {
      return error("entity delete exception", e.getMessage());
    }
  }

  /**
   * Constructs an HTTP DELETE request with a request body for deleting facts by IDs from the Directory service.
   *
   * @param apiUrl    The base URL for the Directory API.
   * @param factIds   The list of fact IDs to be deleted.
   * @return An HttpDeleteWithBody request object.
   */
  private HttpDeleteWithBody deleteFactsByIdsRequest(String apiUrl, List<String> factIds) {
    HttpDeleteWithBody request = new HttpDeleteWithBody(apiUrl);
    // Directory likes to have its delete data wrapped in a map with key "entityIds".
    Map<String,Object> body = new HashMap<String,Object>();
    body.put("entityIds", factIds);
    String jsonBody = gson.toJson(body);
    request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    request.setEntity(new StringEntity(jsonBody, UTF_8));
    return request;
  }

  /**
   * Custom HTTP DELETE request with a request body support.
   * Used for sending delete requests with a request body to the Directory service.
   */
  class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase {
    public static final String METHOD_NAME = "DELETE";

    public HttpDeleteWithBody() {
        super();
    }

    public HttpDeleteWithBody(final URI uri) {
        super();
        setURI(uri);
    }

    public HttpDeleteWithBody(final String uri) {
        super();
        setURI(URI.create(uri));
    }

    @Override
    public String getMethod() {
        return METHOD_NAME;
    }
  }
  
  /**
   * Collects diagnosis corrections from the Directory.
   * 
   * It checks with the Directory if the diagnosis codes are valid ICD values and corrects them if necessary.
   * 
   * Two levels of correction are possible:
   * 
   * 1. If the full code is not correct, remove the number after the period and try again. If the new truncated code is OK, use it to replace the existing diagnosis.
   * 2. If that doesn't work, replace the existing diagnosis with null.
   *
   * @param diagnoses A string map containing diagnoses to be corrected.
   */
  public void collectDiagnosisCorrections(Map<String, String> diagnoses) {
    int diagnosisCounter = 0; // for diagnostics only
    int invalidIcdValueCounter = 0;
    int correctedIcdValueCounter = 0;
    for (String diagnosis: diagnoses.keySet()) {
      if (diagnosisCounter%500 == 0)
        logger.info("__________ collectDiagnosisCorrections: diagnosisCounter: " + diagnosisCounter + ", total diagnoses: " + diagnoses.size());
      if (!isValidIcdValue(diagnosis)) {
        invalidIcdValueCounter++;
        String diagnosisCategory = diagnosis.split("\\.")[0];
        if (isValidIcdValue(diagnosisCategory)) {
          correctedIcdValueCounter++;
          diagnoses.put(diagnosis, diagnosisCategory);
        } else
          diagnoses.put(diagnosis, null);
      }
      diagnosisCounter++;
    }

    logger.info("__________ collectDiagnosisCorrections: invalidIcdValueCounter: " + invalidIcdValueCounter + ", correctedIcdValueCounter: " + correctedIcdValueCounter);
  }

  /**
   * Checks if a given diagnosis code is a valid ICD value by querying the Directory service.
   *
   * @param diagnosis The diagnosis code to be validated.
   * @return true if the diagnosis code is a valid ICD value, false if not, or if an error condition was encountered.
   */
  private boolean isValidIcdValue(String diagnosis) {
    String url = baseUrl + "/api/v2/eu_bbmri_eric_disease_types?q=id=='" + diagnosis + "'";
    try {
      HttpGet request = isValidIcdValueRequest(url);
      CloseableHttpResponse response = httpClient.execute(request);
      if (response.getStatusLine().getStatusCode() < 300) {
        HttpEntity httpEntity = response.getEntity();
        String json = EntityUtils.toString(httpEntity);
        Map body = gson.fromJson(json, Map.class);
        if (body.containsKey("total")) {
          Object total = body.get("total");
          if (total instanceof Double) {
            Integer intTotal = ((Double) total).intValue();
            if (intTotal > 0)
              return true;
          }
        }
      } else
        logger.warn("ICD validation get HTTP error; " + Integer.toString(response.getStatusLine().getStatusCode()));
    } catch (IOException e) {
        logger.warn("ICD validation get exception: " + Util.traceFromException(e));
    } catch (Exception e) {
        logger.warn("ICD validation, unknown exception: " + Util.traceFromException(e));
    }

    return false;
  }

  /**
   * Constructs an HTTP GET request for validating an ICD value against the Directory service.
   *
   * @param url The URL for validating the ICD value.
   * @return An HttpGet request object.
   */
  private HttpGet isValidIcdValueRequest(String url) {
    HttpGet request = new HttpGet(url);
    request.setHeader("x-molgenis-token", token);
    request.setHeader("Accept", "application/json");
    request.setHeader("Content-type", "application/json");
    return request;
  }

  private String buildCollectionApiUrl(String countryCode) {
    return buildApiUrl(countryCode, "collections");
  }

  /**
   * Create a URL for a specific Directory API endpoint.
   * 
   * @param countryCode a code such as "DE" specifying the country the URL should address. May be null.
   * @param function specifies the type of the endpoint, e.g. "collections".
   * @return
   */
  private String buildApiUrl(String countryCode, String function) {
    String countryCodeInsert = "";
    if (countryCode != null && !countryCode.isEmpty())
      countryCodeInsert = countryCode + "_";
    String collectionApiUrl = baseUrl + "/api/v2/eu_bbmri_eric_" + countryCodeInsert + function;

    return collectionApiUrl;
  }

  private Either<OperationOutcome, ItemsDto<IdDto>> fetchIdItems(HttpGet request, String action) {
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      if (response.getStatusLine().getStatusCode() == 200) {
        return Either.right(decodeIdItems(response));
      } else {
        return Either.left(error(action, EntityUtils.toString(response.getEntity(), UTF_8)));
      }
    } catch (IOException e) {
      return Either.left(error(action, e.getMessage()));
    }
  }

  private ItemsDto<IdDto> decodeIdItems(CloseableHttpResponse response) throws IOException {
    String payload = EntityUtils.toString(response.getEntity(), UTF_8);
    return gson.fromJson(payload, new TypeToken<ItemsDto<IdDto>>() {
    }.getType());
  }

  static class LoginCredentials {

    String username, password;

    LoginCredentials(String username, String password) {
      this.username = username;
      this.password = password;
    }
  }

  static class LoginResponse {

    String username, token;

    LoginResponse() {
    }
  }

  private static class EntitiesDto<T> {

    public EntitiesDto(List<T> entities) {
      this.entities = entities;
    }

    List<T> entities;
  }

  static class CollectionSizeDto {

    private final String id;
    private final int size;

    public CollectionSizeDto(BbmriEricId id, int size) {
      this.id = id.toString();
      this.size = size;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CollectionSizeDto that = (CollectionSizeDto) o;
      return size == that.size && id.equals(that.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, size);
    }
  }

  private static class ItemsDto<T> {

    List<T> items;
  }

  private static class IdDto {

    String id;
  }
}
