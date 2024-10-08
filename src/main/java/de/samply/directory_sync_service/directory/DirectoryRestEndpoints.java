package de.samply.directory_sync_service.directory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.ArrayList;

/**
 * Utility class that constructs various REST endpoints for the Directory API.
 */
public class DirectoryRestEndpoints {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryRestEndpoints.class);
  private static final String ENDPOINT_DISEASE_TYPE = "/api/v2/eu_bbmri_eric_disease_types";
  private static final String ENDPOINT_LOGIN = "/api/v1/login";
  private static final String ENDPOINT_FUNCTION = "/api/v2/eu_bbmri_eric_";

  /**
   * Constructs the URL for accessing the login endpoint of the Directory API.
   *
   * @return the constructed biobank API URL.
   */
  public static String getLoginEndpoint() {
    return ENDPOINT_LOGIN;
  }

  /**
   * Constructs the URL for accessing the disease type (ICD10) endpoint of the Directory API.
   *
   * @return the constructed biobank API URL.
   */
  public static String getDiseaseTypeEndpoint() {
    return ENDPOINT_DISEASE_TYPE;
  }

  /**
   * Constructs the URL for accessing the biobank endpoint of the Directory API based on the country code.
   *
   * @param countryCode The country code (e.g., "DE").
   * @return the constructed biobank API URL.
   */
  public static String getBiobankEndpoint(String countryCode) {
    return buildFunctionEndpoint(countryCode, "biobanks");
  }

  /**
   * Constructs the URL for accessing the collection endpoint of the Directory API based on the country code.
   *
   * @param countryCode The country code (e.g., "DE").
   * @return the constructed collection API URL.
   */
  public static String getCollectionEndpoint(String countryCode) {
    return buildFunctionEndpoint(countryCode, "collections");
  }

  /**
   * Constructs the URL for accessing the facts endpoint of the Directory API based on the country code.
   *
   * @param countryCode The country code (e.g., "DE").
   * @return the constructed facts API URL.
   */
  public static String getFactEndpoint(String countryCode) {
    return buildFunctionEndpoint(countryCode, "facts");
  }

  /**
   * Retrieves a list of all available REST endpoints in the Directory API that don't depend on a specific country.
   *
   * @return a list of endpoints.
   */
  public static List<String> getAllEndpoints() {
    List<String> endpoints = new ArrayList<>();
    endpoints.add(getLoginEndpoint());
    endpoints.add(getDiseaseTypeEndpoint());

    return endpoints;
  }

  /**
   * Create a URL for a specific Directory API endpoint.
   *
   * @param countryCode a code such as "DE" specifying the country the URL should address. May be null.
   * @param function specifies the type of the endpoint, e.g. "collections".
   * @return
   */
  private static String buildFunctionEndpoint(String countryCode, String function) {
    String countryCodeInsert = "";
    if (countryCode != null && !countryCode.isEmpty())
      countryCodeInsert = countryCode + "_";
    String collectionApiUrl = ENDPOINT_FUNCTION + countryCodeInsert + function;

    return collectionApiUrl;
  }
}
