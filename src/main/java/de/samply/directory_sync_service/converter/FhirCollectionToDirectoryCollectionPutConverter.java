package de.samply.directory_sync_service.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.fhir.model.FhirCollection;

/**
 * Takes a list of FhirCollection objects and converts them into a
 * DirectoryCollectionPut object.
 * 
 * This is a kind of FHIR to Directory conversion, so there are differences
 * in vocabularies, and it is the job of this converter to impedence match them.
 */
public class FhirCollectionToDirectoryCollectionPutConverter {
  private static final Logger logger = LoggerFactory.getLogger(FhirCollectionToDirectoryCollectionPutConverter.class);

  public static DirectoryCollectionPut convert(List<FhirCollection> fhirCollections) {
      DirectoryCollectionPut directoryCollectionPut = new DirectoryCollectionPut();

      for (FhirCollection fhirCollection: fhirCollections)
        if (convert(directoryCollectionPut, fhirCollection) == null) {
            directoryCollectionPut = null;
            break;
        }

      return directoryCollectionPut;
  }

  private static DirectoryCollectionPut convert(DirectoryCollectionPut directoryCollectionPut, FhirCollection fhirCollection) {
    try {
      convertSize(directoryCollectionPut, fhirCollection);
      convertNumberOfDonors(directoryCollectionPut, fhirCollection);
      convertSex(directoryCollectionPut, fhirCollection);
      convertAgeLow(directoryCollectionPut, fhirCollection);
      convertAgeHigh(directoryCollectionPut, fhirCollection);
      convertMaterials(directoryCollectionPut, fhirCollection);
      convertStorageTemperatures(directoryCollectionPut, fhirCollection);
      // convertDiagnosisAvailableEmpty(directoryCollectionPut, fhirCollection);
      convertDiagnosisAvailable(directoryCollectionPut, fhirCollection);
    } catch(Exception e) {
        logger.error("Problem converting FHIR attributes to Directory attributes. " + Util.traceFromException(e));
        return null;
    }

    return directoryCollectionPut;
  }

  private static void convertSize(DirectoryCollectionPut directoryCollectionPut, FhirCollection fhirCollection) {
      String id = fhirCollection.getId();
      Integer size = fhirCollection.getSize();
      directoryCollectionPut.setSize(id, size);
      // Order of magnitude is mandatory in the Directory and can be derived from size
      directoryCollectionPut.setOrderOfMagnitude(id, (int) Math.floor(Math.log10(size)));
  }

  public static void convertNumberOfDonors(DirectoryCollectionPut directoryCollectionPut, FhirCollection fhirCollection) {
      String id = fhirCollection.getId();
      Integer size = fhirCollection.getNumberOfDonors();
      directoryCollectionPut.setNumberOfDonors(id, size);
      // Order of magnitude is mandatory in the Directory and can be derived from size
      directoryCollectionPut.setOrderOfMagnitudeDonors(id, (int) Math.floor(Math.log10(size)));
  }

  public static void convertSex(DirectoryCollectionPut directoryCollectionPut, FhirCollection fhirCollection) {
      String id = fhirCollection.getId();
      List<String> sex = fhirCollection.getSex();

      List<String> ucSex = sex.stream()
              .map(s -> FhirToDirectoryAttributeConverter.convertSex(s))
              .filter(Objects::nonNull) // Use a method reference to check for non-null values
              .distinct()  // Remove duplicate elements
              .collect(Collectors.toList());

      directoryCollectionPut.setSex(id, ucSex);
  }

  public static void convertAgeLow(DirectoryCollectionPut directoryCollectionPut, FhirCollection fhirCollection) {
      String id = fhirCollection.getId();
      Integer ageLow = fhirCollection.getAgeLow();
      // No conversion needed
      directoryCollectionPut.setAgeLow(id, ageLow);
  }

  public static void convertAgeHigh(DirectoryCollectionPut directoryCollectionPut, FhirCollection fhirCollection) {
      String id = fhirCollection.getId();
      Integer ageHigh = fhirCollection.getAgeHigh();
      // No conversion needed
      directoryCollectionPut.setAgeHigh(id, ageHigh);
  }

  public static void convertMaterials(DirectoryCollectionPut directoryCollectionPut, FhirCollection fhirCollection) {
      String id = fhirCollection.getId();
      List<String> materials = fhirCollection.getMaterials();

      if (materials == null)
          materials = new ArrayList<String>();

      List<String> directoryMaterials = materials.stream()
              .map(s -> FhirToDirectoryAttributeConverter.convertMaterial(s))
              .filter(Objects::nonNull) // Use a method reference to check for non-null values
              .distinct()  // Remove duplicate elements
              .collect(Collectors.toList());
 
      directoryCollectionPut.setMaterials(id, directoryMaterials);
  }

  public static void convertStorageTemperatures(DirectoryCollectionPut directoryCollectionPut, FhirCollection fhirCollection) {
    String id = fhirCollection.getId();
    List<String> storageTemperatures = fhirCollection.getStorageTemperatures();

    if (storageTemperatures == null)
        storageTemperatures = new ArrayList<String>();

    List<String> directoryStorageTemperatures = storageTemperatures.stream()
        .map(s -> FhirToDirectoryAttributeConverter.convertStorageTemperature(s))
        .filter(Objects::nonNull) // Use a method reference to check for non-null values
        .distinct()  // Remove duplicate elements
        .collect(Collectors.toList());

    directoryCollectionPut.setStorageTemperatures(id, directoryStorageTemperatures);
  }

  public static void convertDiagnosisAvailableEmpty(DirectoryCollectionPut directoryCollectionPut, FhirCollection fhirCollection) {
    String id = fhirCollection.getId();
    // The Directory is very picky about which ICD10 codes it will accept, and some
    // of the codes that are in our test data are not known to the Directory and
    // give rise to errors, which lead to the entire PUT to the Directory being
    // rejected. So, for the time being, I am turning off the diagnosis conversion.
    directoryCollectionPut.setDiagnosisAvailable(id, new ArrayList<String>());
  }

  public static void convertDiagnosisAvailable(DirectoryCollectionPut directoryCollectionPut, FhirCollection fhirCollection) {
    String id = fhirCollection.getId();
    List<String> diagnoses = fhirCollection.getDiagnosisAvailable();

    if (diagnoses == null)
        diagnoses = new ArrayList<String>();

    List<String> miriamDiagnoses = diagnoses.stream()
            .map(icd -> FhirToDirectoryAttributeConverter.convertDiagnosis(icd))
            .filter(Objects::nonNull) // Use a method reference to check for non-null values
            .distinct()  // Remove duplicate diagnoses
            .collect(Collectors.toList());
    
    directoryCollectionPut.setDiagnosisAvailable(id, miriamDiagnoses);
  }
}
