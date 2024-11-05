package de.samply.directory_sync_service.directory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that constructs various REST endpoints for the Directory API.
 */
public abstract class DirectoryEndpoints {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryEndpoints.class);

  /**
   * Constructs the URL for accessing the login endpoint of the Directory API.
   *
   * @return the constructed biobank API URL.
   */
  public abstract String getLoginEndpoint();
}
