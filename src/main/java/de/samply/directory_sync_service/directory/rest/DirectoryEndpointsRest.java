package de.samply.directory_sync_service.directory.rest;

import de.samply.directory_sync_service.directory.DirectoryEndpoints;

/**
 * Utility class that constructs various REST endpoints for the Directory API.
 */
public class DirectoryEndpointsRest extends DirectoryEndpoints {
  private static final String ENDPOINT_LOGIN = "/api/v1/login";
  private static final String ENDPOINT_DISEASE_TYPE = "/api/v2/eu_bbmri_eric_disease_types";
  private static final String ENDPOINT_FUNCTION = "/api/v2/eu_bbmri_eric_";

  /**
   * Constructs the URL for accessing the login endpoint of the Directory API.
   *
   * @return the constructed biobank API URL.
   */
  @Override
  public String getLoginEndpoint() {
    return ENDPOINT_LOGIN;
  }

  /**
   * Constructs the URL for accessing the disease type (ICD10) endpoint of the Directory API.
   *
   * @return the constructed biobank API URL.
   */
  public String getDiseaseTypeEndpoint() {
    return ENDPOINT_DISEASE_TYPE;
  }

  /**
   * Constructs the URL for accessing the biobank endpoint of the Directory API based on the country code.
   *
   * @param countryCode The country code (e.g., "DE").
   * @return the constructed biobank API URL.
   */
  public String getBiobankEndpoint(String countryCode) {
    return buildFunctionEndpoint(countryCode, "biobanks");
  }

  /**
   * Constructs the URL for accessing the collection endpoint of the Directory API based on the country code.
   *
   * @param countryCode The country code (e.g., "DE").
   * @return the constructed collection API URL.
   */
  public String getCollectionEndpoint(String countryCode) {
    return buildFunctionEndpoint(countryCode, "collections");
  }

  /**
   * Constructs the URL for accessing the facts endpoint of the Directory API based on the country code.
   *
   * @param countryCode The country code (e.g., "DE").
   * @return the constructed facts API URL.
   */
  public String getFactEndpoint(String countryCode) {
    return buildFunctionEndpoint(countryCode, "facts");
  }

  /**
   * Create a URL for a specific Directory API endpoint.
   *
   * @param countryCode a code such as "DE" specifying the country the URL should address. May be null.
   * @param function specifies the type of the endpoint, e.g. "collections".
   * @return
   */
  private String buildFunctionEndpoint(String countryCode, String function) {
    String countryCodeInsert = "";
    if (countryCode != null && !countryCode.isEmpty())
      countryCodeInsert = countryCode + "_";

    return ENDPOINT_FUNCTION + countryCodeInsert + function;
  }
}
