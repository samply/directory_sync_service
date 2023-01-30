package de.samply.directory_sync_service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/** Contains the default mappings. */
@Service
public class DirectorySyncLauncher {
  private static Logger logger = LogManager.getLogger(DirectorySyncLauncher.class);

  @Autowired
  Configuration configuration;

  /** Starts the transformation. */
  public void run() throws Exception {
    logger.info("Starting Directory sync");

    String directoryUserName = configuration.getDirectoryUserName();
    String directoryUserPass = configuration.getDirectoryUserPass();
    if (directoryUserName == null || directoryUserName.isEmpty() || directoryUserPass == null || directoryUserPass.isEmpty()) {
      logger.warn("Directory user name or pass code is empty, will *not* perform Directory sync");
      return;
    }

    String timerCron = configuration.getTimerCron();

    new DirectorySyncJobScheduler("directorySync").jobStart(timerCron, configuration);
  }
}
