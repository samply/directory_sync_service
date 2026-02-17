package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.model.Biobank;
import de.samply.directory_sync_service.model.Collections;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.converter.ConvertCollectionsToDirectoryCollectionPut;

import java.util.ArrayList;
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
  private final String directoryOutputDirectory;
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
    super(false);
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
   * <p>
   * This is a dummy implementation that always returns true. No login
   * is needed to write to file.
   */
  public boolean login() {
    return true;
  }

  /**
   * Fetches the Biobank with the given {@code id}.
   * <p>
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
   * @param collections
   */
  public void fetchBasicCollectionData(Collections collections) {
  }

  /**
   * Deposits aggregated collection information into a file.
   *
   * @param collections Summary information about one or more collections
   * @return an outcome, either successful or null
   */
  public boolean sendUpdatedCollections(Collections collections) {
    if (mockDirectory) {
      // Dummy return if we're in mock mode
      return true;
    }

    // Convert Collections object into a DirectoryCollectionPut object. This is
    // really intended for use with the RESTful API, but it can be mutated to
    // work with a tabular interface as well.
    DirectoryCollectionPut directoryCollectionPut = ConvertCollectionsToDirectoryCollectionPut.convert(collections);
    if (directoryCollectionPut == null) {
      logger.warn("sendUpdatedCollections: Problem converting FHIR attributes to Directory attributes");
      return false;
    }
    logger.debug("sendUpdatedCollections: 1 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

    List<Map<String, Object>> entities = new ArrayList<Map<String, Object>>();
    for (String collectionId: directoryCollectionPut.getCollectionIds()) {
      logger.debug("sendUpdatedCollections: about to update collection: " + collectionId);

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
   * Updates the fact tables block for a specific country with the provided data.
   *
   * @param countryCode The country code, e.g. DE.
   * @param factTablesBlock A list of maps representing the fact tables block data.
   * @return true if the update was successful, false otherwise.
   */
  @Override
  protected boolean updateFactTablesBlock(String countryCode, List<Map<String, String>> factTablesBlock) {
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
   * <p>
   * If directoryOutputDirectory is null, this method only puts data into entityTableString
   * without tring to write to a file.
   *
   * @param entities A list of maps where each map represents an entity with key-value pairs.
   */
  private void writeEntitiesToFile(List<Map<String, Object>> entities) {
    List<String> columnNames = List.of("id", "country", "type", "data_categories", "order_of_magnitude", "size", "timestamp", "number_of_donors", "order_of_magnitude_donors", "sex", "diagnosis_available", "age_low", "age_high", "materials", "storage_temperatures");
    entityTableString = Util.convertListOfMapsToTable(entities, ";", columnNames);

    if (directoryOutputDirectory == null)
        return;
    File outputFile = new File(directoryOutputDirectory, "DirectoryCollections.csv");

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
      writer.write(entityTableString);
      logger.debug("writeFactTablesToFile: Fact tables successfully written to file: " + outputFile.getAbsolutePath());
    } catch (IOException e) {
      logger.error("writeFactTablesToFile: Failed to write fact tables to file, exception: " + Util.traceFromException(e));
    }
  }

  /**
   * Writes a list of fact tables to a CSV file in the specified output directory.
   *
   * <p>This method formats the given fact tables as a delimited table using a predefined
   * column order and writes the output to a file named "DirectoryFactTables.csv".
   * <p>
   * If directoryOutputDirectory is null, this method only puts data into factTableString
   * without tring to write to a file.
   *
   * @param factTables A list of maps representing fact tables, with string key-value pairs.
   */
  private void writeFactTablesToFile(List<Map<String, String>> factTables) {
    List<String> columnNames = List.of("id", "sex", "disease", "age_range", "sample_type", "number_of_donors", "number_of_samples", "last_update", "collection");
    factTableString = Util.convertListOfStringMapsToTable(factTables, ";", columnNames);
    if (directoryOutputDirectory == null)
      return;
    File outputFile = new File(directoryOutputDirectory, "DirectoryFactTables.csv");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
      writer.write(factTableString);
      logger.debug("writeFactTablesToFile: Fact tables successfully written to file: " + outputFile.getAbsolutePath());
    } catch (IOException e) {
      logger.error("writeFactTablesToFile: Failed to write fact tables to file, exception: " + Util.traceFromException(e));
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

//  /**
//   * Dummy operation.
//   *
//   * @param diagnosis
//   * @return
//   */
//  @Override
//  public boolean isValidIcdValue(String diagnosis) {
//    return true;
//  }
  /**
   * Dummy ICD-10 validation method used for testing purposes.
   *
   * <p>This method does <strong>not</strong> perform real ICD-10 validation.
   * Instead, it applies a deterministic rule so that some codes are accepted
   * and some are rejected in a predictable way.</p>
   *
   * <p>The expected input format is:</p>
   * <pre>
   *     urn:miriam:icd:&lt;ICD_CODE&gt;
   * </pre>
   *
   * <p>Validation rules:</p>
   * <ul>
   *   <li>The leading prefix {@code "urn:miriam:icd:"} is removed (case-insensitive).</li>
   *   <li>If the ICD code contains a decimal point:
   *       <ul>
   *         <li>The numeric value after the decimal point is parsed.</li>
   *         <li>The method returns {@code true} if that number is even,
   *             {@code false} if it is odd.</li>
   *       </ul>
   *   </li>
   *   <li>If the ICD code does not contain a decimal point:
   *       <ul>
   *         <li>The numeric portion after the leading letter is parsed.</li>
   *         <li>The method returns {@code true} if that number is even,
   *             {@code false} if it is odd.</li>
   *       </ul>
   *   </li>
   *   <li>If parsing fails or the input is malformed, {@code false} is returned.</li>
   * </ul>
   *
   * <p>Examples:</p>
   * <ul>
   *   <li>{@code urn:miriam:icd:C75.4} → {@code true}</li>
   *   <li>{@code urn:miriam:icd:C75.5} → {@code false}</li>
   *   <li>{@code urn:miriam:icd:D62} → {@code true}</li>
   *   <li>{@code urn:miriam:icd:D63} → {@code false}</li>
   * </ul>
   *
   * <p>This method is intended solely for controlled test scenarios and
   * should not be used for real medical validation.</p>
   *
   * @param diagnosis the ICD-10 URN string to validate
   * @return {@code true} if the deterministic parity rule evaluates to even,
   *         {@code false} otherwise
   */
  @Override
  public boolean isValidIcdValue(String diagnosis) {
    if (diagnosis == null) {
      return false;
    }

    String prefix = "urn:miriam:icd:";
    String value = diagnosis;

    // Strip prefix (case-insensitive)
    if (value.regionMatches(true, 0, prefix, 0, prefix.length())) {
      value = value.substring(prefix.length());
    }

    if (value.isEmpty()) {
      return false;
    }

    int dotIndex = value.indexOf('.');

    try {
      if (dotIndex >= 0) {
        // Case: decimal exists → check number after dot
        String decimalPart = value.substring(dotIndex + 1);
        if (decimalPart.isEmpty()) {
          return false;
        }

        int number = Integer.parseInt(decimalPart);
        return number % 2 == 0;  // true if even
      } else {
        // No decimal → check numeric part after first letter
        if (value.length() < 2) {
          return false;
        }

        String numberPart = value.substring(1); // skip leading letter
        int number = Integer.parseInt(numberPart);
        return number % 2 == 0;  // true if even
      }
    } catch (NumberFormatException e) {
      return false; // non-numeric values fail
    }
  }
}
