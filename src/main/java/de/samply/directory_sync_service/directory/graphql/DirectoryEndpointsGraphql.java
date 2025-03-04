package de.samply.directory_sync_service.directory.graphql;

import de.samply.directory_sync_service.directory.DirectoryEndpoints;

/**
 * Utility class that constructs various GraphQL endpoints for the Directory API.
 */
public class DirectoryEndpointsGraphql extends DirectoryEndpoints {
  private static final String ENDPOINT_API = "/api/graphql";
  private static final String ENDPOINT_LOGIN = ENDPOINT_API;
  private static final String ENDPOINT_DATABASE_DIRECTORY_ONTOLOGIES = "/DirectoryOntologies/api/graphql";

  /**
   * Constructs the URL for accessing the GraphQL endpoint of the Directory API.
   *
   * @return the constructed biobank API URL.
   */
  public String getApiEndpoint() {
    return ENDPOINT_API;
  }

  /**
   * Constructs the URL for accessing the DirectoryOntologies database endpoint of the Directory API.
   * This is only used for disease types (ICD10) in our application.
   *
   * @return the constructed biobank API URL.
   */
  public String getDatabaseDirectoryOntologiesEndpoint() {
    return ENDPOINT_DATABASE_DIRECTORY_ONTOLOGIES;
  }

  /**
   * Constructs the URL for accessing the login endpoint of the Directory API.
   *
   * @return the constructed biobank API URL.
   */
  @Override
  public String getLoginEndpoint() {
    return ENDPOINT_LOGIN;
  }
}
