package de.samply.directory_sync_service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Reads the contents of the configuration file and packs them into a hash.
 *
 * These properties are cached, i.e. the read operation only happens once.
 * This means that this functionality is not applicable to a situation
 * where properties change dynamically.
 */
public class DirectorySyncConfig {
    private static final String configFilename = "/etc/bridgehead/directory_sync.conf";
    private static HashMap<String, String> hashMapProp = null;

    /**
     * Pulls the parameters needed by Directory sync from a Java parameter file
     * and returns as a HashMap.
     *
     * @return HashMap of properties
     */
    public static HashMap<String, String> getProperties() {
        if (hashMapProp != null)
            return hashMapProp;

        try (InputStream input = new FileInputStream(configFilename)) {
            Properties prop = new Properties();

            prop.load(input);

            hashMapProp = convertPropertiesToHashMap(prop);

            return hashMapProp;
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return null;
    }

    /**
     *
     * @param prop Properties list
     * @return HashMap of properties
     */
    private static HashMap<String, String> convertPropertiesToHashMap(Properties prop) {
        Map mapProp = prop;
        Map<String, String> mapPropTyped = (Map<String, String>) mapProp;
        return new HashMap<>(mapPropTyped);
    }
}
