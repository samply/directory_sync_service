package de.samply.directory_sync_service.directory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Utility class that constructs various REST endpoints for the Directory API.
 */
public abstract class DirectoryEndpoints {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryEndpoints.class);

  /**
   * Retrieves a list of all available REST endpoints in the Directory API that don't depend on a specific country.
   *
   * @return a list of endpoints.
   */
  public abstract List<String> getAllEndpoints();
}
