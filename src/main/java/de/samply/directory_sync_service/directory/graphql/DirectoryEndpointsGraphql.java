package de.samply.directory_sync_service.directory.graphql;

import de.samply.directory_sync_service.directory.DirectoryEndpoints;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class that constructs various GraphQL endpoints for the Directory API.
 */
public class DirectoryEndpointsGraphql extends DirectoryEndpoints {
  private static final String ENDPOINT_LOGIN = "/api/graphql";
  private static final String ENDPOINT_DATABASE_ERIC = "/ERIC/api/graphql";
  private static final String ENDPOINT_DATABASE_DIRECTORY_ONTOLOGIES = "/DirectoryOntologies/api/graphql";

  /**
   * Constructs the URL for accessing the ERIC database endpoint of the Directory API.
   * This is the route to the data you will be storing, in particular, biobanks and collections.
   * Facts (star model) may also be stored here, I am not sure yet.
   *
   * @return the constructed biobank API URL.
   */
  public String getDatabaseEricEndpoint() {
    return ENDPOINT_DATABASE_ERIC;
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
