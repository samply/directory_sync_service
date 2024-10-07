package de.samply.directory_sync_service.directory.graphql;

import de.samply.directory_sync_service.directory.DirectoryEndpoints;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that constructs various GraphQL endpoints for the Directory API.
 */
public class DirectoryEndpointsGraphql extends DirectoryEndpoints {
  private static final String ENDPOINT_ROOT = "/api/graphql";
  private static final String ENDPOINT_DATABASE_ERIC = "/ERIC/api/graphql";
  private static final String ENDPOINT_DATABASE_DIRECTORY_ONTOLOGIES = "/DirectoryOntologies/api/graphql";

  /**
   * Constructs the URL for accessing the ERIC database endpoint of the Directory API.
   * This is the route to the data you will be storing, in particular, biobanks and collections.
   * Facts (star model) may also be stored here, I am not sure yet.
   *
   * @return the constructed biobank API URL.
   */
  public static String getDatabaseEricEndpoint() {
    return ENDPOINT_DATABASE_ERIC;
  }

  /**
   * Constructs the URL for accessing the DirectoryOntologies database endpoint of the Directory API.
   * This is only used for disease types (ICD10) in our application.
   *
   * @return the constructed biobank API URL.
   */
  public static String getDatabaseDirectoryOntologiesEndpoint() {
    return ENDPOINT_DATABASE_DIRECTORY_ONTOLOGIES;
  }

  /**
   * Constructs the URL for accessing the root endpoint of the Directory API.
   * I am not sure if this is needed.
   *
   * @return the constructed biobank API URL.
   */
  public static String getRootEndpoint() {
    return ENDPOINT_ROOT;
  }

  /**
   * Retrieves a list of all available REST endpoints in the Directory API that don't depend on a specific country.
   *
   * @return a list of endpoints.
   */
  public List<String> getAllEndpoints() {
    List<String> endpoints = new ArrayList<>();
    endpoints.add(getDatabaseEricEndpoint());
    endpoints.add(getDatabaseDirectoryOntologiesEndpoint());
    endpoints.add(getRootEndpoint());

    return endpoints;
  }
}
