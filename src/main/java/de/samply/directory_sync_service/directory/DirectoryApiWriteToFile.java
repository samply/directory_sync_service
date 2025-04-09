package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.BbmriEricId;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


/**
 * The DirectoryApiWriteToFile class allows you to dump Directory-related data to files,
 * instead of sending it to a Directory instance. This is mainly useful for testing purposes.
 */
public class DirectoryApiWriteToFile extends DirectoryApi {
  private String directoryOutputDirectory;
  private String factTableString = null;
  private String entityTableString = null;

  /**
   * Constructs a new DirectoryApiWriteToFile instance.
   * If directoryOutputDirectory is null, data will simply be placed into memory,
   * and can be retrieved using the getFactTableString() and getEntityTableString() methods.
   *
   * @param directoryOutputDirectory
   */
  public DirectoryApiWriteToFile(String directoryOutputDirectory) {
    super(null, false, null, null);
    this.directoryOutputDirectory = directoryOutputDirectory;
  }

  public String getFactTableString() {
    return factTableString;
  }

  public String getEntityTableString() {
    return entityTableString;
  }

  /**
   * Log in to the Directory. You can log in as many times as you like.
   *
   * This is a dummy implementation that always returns true. No login
   * is needed to write to file.
   */
  public boolean login() {
    return true;
  }

  /**
   * Fetches the Biobank with the given {@code id}.
   *
   * Returns a dummy value.
   *
   * @param id the ID of the Biobank to fetch.
   * @return either the Biobank or null if an error occurs
   */
  public Biobank fetchBiobank(BbmriEricId id) {
    return new Biobank();
  }

  /**
   * More or less a dummy operator.
   * 
   * @param countryCode E.g. "DE".
   * @param collectionIds IDs of the collections whose data will be harvested.
   * @return
   */
  public DirectoryCollectionGet fetchCollectionGetOutcomes(String countryCode, List<String> collectionIds) {
    DirectoryCollectionGet directoryCollectionGet = new DirectoryCollectionGet(); // for all collections retrieved from Directory
    directoryCollectionGet.init();

    if (mockDirectory)
      // Dummy return if we're in mock mode
      directoryCollectionGet.setMockDirectory(true);

    return directoryCollectionGet;
  }

  /**
   * Deposits aggregated collection information into a file.
   *
   * @param directoryCollectionPut Summary information about one or more collections
   * @return an outcome, either successful or null
   */
  public boolean updateEntities(DirectoryCollectionPut directoryCollectionPut) {
    if (mockDirectory) {
      // Dummy return if we're in mock mode
      return true;
    }

    List<Map<String, Object>> entities = new ArrayList<Map<String, Object>>();
    for (String collectionId: directoryCollectionPut.getCollectionIds()) {
      logger.debug("updateEntities: about to update collection: " + collectionId);

      Map<String, Object> entity = directoryCollectionPut.getEntity(collectionId);
      cleanEntity(entity);
      if (entity.containsKey("timestamp"))
        entity.put("timestamp", cleanTimestamp(entity.get("timestamp").toString()));
      insertMissingAttributesIntoEntity(directoryCollectionPut, entity);
      transformEntityForEmx2(entity);
      entities.add(entity);
    }
    writeEntitiesToFile(entities);

    return true;
  }

  /**
   * Removes keys from the given collection if the corresponding value is an empty list or a list with a single null element.
   *
   * @param entity The collection to clean.
   */
  private void cleanEntity(Map<String, Object> entity) {
    List<String> badKeys = new ArrayList<String>(); // List of keys to remove>
    for (String key: entity.keySet()) {
      if (entity.get(key) instanceof List list) {
        if (list.size() == 0) {
          logger.debug("cleanEntity: attribute \"" + key + "\" is an empty list");
          badKeys.add(key);
        }
        if (list.size() == 1 && list.get(0) == null) {
          logger.debug("cleanEntity: attribute \"" + key + "\" has a single null element");
          badKeys.add(key);
        }
      }
    }
    for (String key: badKeys) {
      logger.debug("cleanEntity: removing bad attribute: \"" + key + "\"");
      entity.remove(key);
    }
  }

  /**
   * Inserts missing attributes into the collection based on the provided DirectoryCollectionPut object.
   *
   * @param directoryCollectionPut The object containing the missing attributes.
   * @param entity The collection to insert missing attributes into.
   */
  private void insertMissingAttributesIntoEntity(DirectoryCollectionPut directoryCollectionPut, Map<String, Object> entity) {
    if (!entity.containsKey("country"))
      entity.put("country", directoryCollectionPut.getCountryCode());
    if (!entity.containsKey("timestamp"))
      entity.put("timestamp", LocalDateTime.now().toString());
    if (!entity.containsKey("national_node"))
      entity.put("national_node", directoryCollectionPut.getCountryCode());
    if (!entity.containsKey("biobank_label") && entity.containsKey("biobank") && entity.get("biobank") instanceof String)
      entity.put("biobank_label", entity.get("biobank"));
    if (!entity.containsKey("type")) {
      List<String> type = new ArrayList<>();
      type.add("SAMPLE");
      entity.put("type", type);
    }
    if (!entity.containsKey("data_categories")) {
      List<String> dataCategories = new ArrayList<>();
      dataCategories.add("BIOLOGICAL_SAMPLES");
      entity.put("data_categories", dataCategories);
    }
  }

  /**
   * Cleans the timestamp by removing any trailing non-numeric characters.
   * E.g. there might be a 'Z' at the end of the timestamp, which the GrapQL
   * API doesn't need.
   *
   * @param timestamp The timestamp string to be cleaned.
   * @return The cleaned timestamp string without any trailing non-numeric characters.
   */
  private String cleanTimestamp(String timestamp) {
    // Use the String.matches method to check if the timestamp ends with a non-numeric character
    if (timestamp.matches(".*[^\\d]$")) {
      timestamp = timestamp.substring(0, timestamp.length() - 1);
      logger.debug("cleanTimestamp: corrected timestamp: " + timestamp);
    }

    return timestamp;
  }

  /**
   * Transforms the collection for the EMX2 format by transforming specific attributes.
   *
   * @param entity The collection to transform.
   */
  private void transformEntityForEmx2(Map<String, Object> entity) {
    transformAttributeForEmx2(entity, "diagnosis_available", "name");
    transformAttributeForEmx2(entity, "data_categories", "name");
    transformAttributeForEmx2(entity, "storage_temperatures", "name");
    transformAttributeForEmx2(entity, "sex", "name");
    transformAttributeForEmx2(entity, "materials", "name");
    transformAttributeForEmx2(entity, "order_of_magnitude_donors", "name");
    transformAttributeForEmx2(entity, "order_of_magnitude", "name");
    transformAttributeForEmx2(entity, "country", "name");
    transformAttributeForEmx2(entity, "type", "name");
    transformAttributeForEmx2(entity, "data_categories", "name");
    transformAttributeForEmx2(entity, "national_node", "id");
    transformAttributeForEmx2(entity, "contact", "id");
    transformAttributeForEmx2(entity, "biobank", "id");
  }

  /**
   * Transforms a specific attribute of a collection for the EMX2 format by converting it into a specific structure.
   *
   * @param entity The collection containing the attribute to transform.
   * @param attributeName The name of the attribute to transform.
   * @param attributeElementName The name of the element within the attribute to transform.
   */
  private void transformAttributeForEmx2(Map<String, Object> entity, String attributeName, String attributeElementName) {
    // Transform a single attribute
    if (entity.containsKey(attributeName)) {
      Object attribute = entity.get(attributeName);
      if (attribute instanceof List) {
        List<Object> attributeList = (List<Object>) attribute;
        List newAttributeList = new ArrayList();
        if (attributeList.size() > 0) {
          for (Object attributeElementValue : attributeList) {
            if (attributeElementValue instanceof String || attributeElementValue instanceof Integer) {
              Map<String, String> newAttributeValue = new HashMap();
              newAttributeValue.put(attributeElementName, attributeElementValue.toString());
              newAttributeList.add(newAttributeValue);
            } else if (attributeElementValue instanceof Map) {
              // Looks like this attribute has already been transformed, so keep it unchanged
              newAttributeList.add(attributeElementValue);
            } else {
              logger.warn("transformAttributeForEmx2: attribute \"" + attributeName + "\" is not a list or string, value: " + Util.jsonStringFomObject(attributeElementValue));
            }
          }
          entity.remove(attributeName); // Remove old attribute
          entity.put(attributeName, newAttributeList);
        }
      } else if (attribute instanceof String || attribute instanceof Integer) {
        Map<String,String> newAttributeValue = new HashMap();
        newAttributeValue.put(attributeElementName, attribute.toString());
        entity.remove(attributeName); // Remove old attribute
        entity.put(attributeName, newAttributeValue);
      } else {
        logger.warn("transformAttributeForEmx2: attribute \"" + attributeName + "\" is not a list or string");
      }
    }
  }

  /**
   * Updates the fact tables block for a specific country with the provided data.
   *
   * @param countryCode The country code, e.g. DE.
   * @param factTablesBlock A list of maps representing the fact tables block data.
   * @return true if the update was successful, false otherwise.
   */
  @Override
  protected boolean updateFactTablesBlock(String countryCode, List<Map<String, String>> factTablesBlock) {
    if (factTablesBlock.size() == 0)
      // Nothing to insert
      return true;

    List<Map<String, String>> factTables = new ArrayList<>();
    for (Map<String, String> factTable : factTablesBlock)
      try {
        if (!factTable.containsKey("national_node") && countryCode != null && !countryCode.isEmpty())
          factTable.put("national_node", countryCode);
          factTables.add(factTable);
      } catch (Exception e) {
        logger.warn("updateFactTablesBlock: Exception during fact deletion: " + Util.traceFromException(e));
        return false;
      }
    writeFactTablesToFile(factTables);

    return true;
  }

  /**
   * Writes a list of entities to a CSV file in the specified output directory.
   *
   * <p>The method converts the list of maps into a table format using a predefined column order
   * and writes the resulting data to a file named "DirectoryCollections.csv". The file is stored
   * in the directory specified by {@code directoryOutputDirectory}.
   *
   * If directoryOutputDirectory is null, this method only puts data into entityTableString
   * without tring to write to a file.
   *
   * @param entities A list of maps where each map represents an entity with key-value pairs.
   */
  private void writeEntitiesToFile(List<Map<String, Object>> entities) {
    logger.debug("writeEntitiesToFile: entities:\n" + Util.jsonStringFomObject(entities));

    List<String> columnNames = List.of("id", "country", "type", "data_categories", "order_of_magnitude", "size", "timestamp", "number_of_donors", "order_of_magnitude_donors", "sex", "diagnosis_available", "age_low", "age_high", "materials", "storage_temperatures");
    entityTableString = Util.convertListOfMapsToTable(entities, ";", columnNames);

    if (directoryOutputDirectory == null)
        return;
    File outputFile = new File(directoryOutputDirectory, "DirectoryCollections.csv");

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
      writer.write(entityTableString);
      logger.debug("writeFactTablesToFile: Fact tables successfully written to file: " + outputFile.getAbsolutePath());
    } catch (IOException e) {
      logger.error("writeFactTablesToFile: Failed to write fact tables to file", Util.traceFromException(e));
    }
  }

  /**
   * Writes a list of fact tables to a CSV file in the specified output directory.
   *
   * <p>This method formats the given fact tables as a delimited table using a predefined
   * column order and writes the output to a file named "DirectoryFactTables.csv".
   *
   * If directoryOutputDirectory is null, this method only puts data into factTableString
   * without tring to write to a file.
   *
   * @param factTables A list of maps representing fact tables, with string key-value pairs.
   */
  private void writeFactTablesToFile(List<Map<String, String>> factTables) {
    logger.debug("writeFactTableToFile: factTables:\n" + Util.jsonStringFomObject(factTables));

    List<String> columnNames = List.of("id", "sex", "disease", "age_range", "sample_type", "number_of_donors", "number_of_samples", "last_update", "collection");
    factTableString = Util.convertListOfStringMapsToTable(factTables, ";", columnNames);
    if (directoryOutputDirectory == null)
      return;
    File outputFile = new File(directoryOutputDirectory, "DirectoryFactTables.csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
      writer.write(factTableString);
      logger.debug("writeFactTablesToFile: Fact tables successfully written to file: " + outputFile.getAbsolutePath());
    } catch (IOException e) {
      logger.error("writeFactTablesToFile: Failed to write fact tables to file", Util.traceFromException(e));
    }
  }

  /**
   * Dummy operation.
   *
   * @param collectionId The ID of the collection to retrieve fact IDs for.
   * @return
   */
  @Override
  protected List<String> getNextPageOfFactIdsForCollection(String collectionId) {
    return new ArrayList<String>();
  }

  /**
   * Dummy operation.
   *
   * @param countryCode    e.g. DE.
   * @param factIds   The list of fact IDs to be deleted.
   * @return
   */
  @Override
  protected boolean deleteFactsByIds(String countryCode, List<String> factIds) {
    return true;
  }

  /**
   * Dummy operation.
   *
   * @param diagnosis
   * @return
   */
  @Override
  protected boolean isValidIcdValue(String diagnosis) {
    return true;
  }
}
