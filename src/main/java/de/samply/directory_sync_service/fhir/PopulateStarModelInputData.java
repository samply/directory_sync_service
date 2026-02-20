package de.samply.directory_sync_service.fhir;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Specimen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.samply.directory_sync_service.model.StarModelInput;
import de.samply.directory_sync_service.model.StarModelInputRow;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.model.BbmriEricId;

/**
 * Pull data about Patients, Specimens and Dieseases from the FHIR store and
 * use the information they contain to fill a StarModelInputData object.
 */
public class PopulateStarModelInputData {
  private static final Logger logger = LoggerFactory.getLogger(PopulateStarModelInputData.class);
  private final FhirApi fhirApi;

  public PopulateStarModelInputData(FhirApi fhirApi) {
    this.fhirApi = fhirApi;
  }

  /**
   * Populates a Star Model input data object based on specimens fetched from the FHIR server,
   * grouped according to the specified default BBMRI-ERIC collection ID.
   *
   * @param directoryDefaultCollectionId The default BBMRI-ERIC collection ID to group specimens. May be null.
   * @return A StarModelInput object populated with data extracted from the fetched specimens.
   */
  public StarModelInput populate(String directoryDefaultCollectionId) {
    BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
            .valueOf(directoryDefaultCollectionId)
            .orElse(null);
    // Group specimens according to collection.
    Map<String, List<Specimen>> specimensByCollection = fhirApi.fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    if (specimensByCollection == null) {
      logger.error("populate: Problem finding specimens");
      return null;
    }

    StarModelInput starModelInput = new StarModelInput();
    if (specimensByCollection.keySet().size() ==0)
      logger.warn("populate: specimensByCollection.keySet() is empty");
    for (String collectionId: specimensByCollection.keySet()) {
      logger.info("populate: collectionId: " + collectionId);
      populateCollection(starModelInput, collectionId, specimensByCollection.get(collectionId));
    }

    return starModelInput;
  }

  /**
   * Populates the Star Model input data with information extracted from a list of specimens
   * associated with a specific collection.
   *
   * @param starModelInput The Star Model input data to be populated.
   * @param collectionId The identifier for the collection to which the specimens belong.
   * @param specimens The list of specimens from which to extract data and populate the input data.
   *
   * @throws NullPointerException if starModelInputData, collectionId, or specimens is null.
   */
  private void populateCollection(StarModelInput starModelInput, String collectionId, List<Specimen> specimens) {
    if (specimens.size() == 0) {
      logger.warn("populateCollection: specimens list is empty, skipping collection: " + collectionId);
      return;
    }

    int specimenCounter = 0;
    for (Specimen specimen: specimens) {
      populateSpecimen(starModelInput, collectionId, specimen);
      if (specimenCounter % 1000 == 0) {
        logger.debug("populateCollection: specimenCounter: " + specimenCounter + " of " + specimens.size());
      }
      specimenCounter++;
    }
  }

  /**
   * Populates the Star Model input data with information extracted from a single specimen.
   *
   * @param starModelInput The Star Model input data to be populated.
   * @param collectionId The identifier for the collection to which the specimen belongs.
   * @param specimen The specimen from which to extract data and populate the input data.
   *
   * @throws NullPointerException if starModelInputData, collectionId, or specimen is null.
   */
  private void populateSpecimen(StarModelInput starModelInput, String collectionId, Specimen specimen) {
    // Get the Patient who donated the sample
    Patient patient = fhirApi.extractPatientFromSpecimen(specimen);

    if (patient == null) {
      logger.warn("populateSpecimen: patient is null, skipping specimen: " + specimen.getIdElement().getIdPart());
      return;
    }

    String material = extractMaterialFromSpecimen(specimen);
    String patientId = patient.getIdElement().getIdPart();
    String sex = "unknown";
    if (patient.hasGender())
        sex = patient.getGender().getDisplay();
    String age = determinePatientAgeAtCollection(patient);

    // Create a new Row object to hold data extracted from patient and specimen
    StarModelInputRow row = new StarModelInputRow(collectionId, material, patientId, sex, age);

    List<String> diagnoses = extractDiagnosesFromPatientAndSpecimen(patient, specimen);

    logger.info("populateSpecimen: diagnoses.size(): " + diagnoses.size());

    // Add all of the collected information to the input data table.
    for (String diagnosis: diagnoses)
      starModelInput.addInputRow(collectionId, StarModelInputRow.newInputRow(row, diagnosis));
  }

  int nullAgeCounter = 0;
  /**
   * Determines the patient's age at the time of first specimen collection.
   *
   * @param patient The FHIR Patient object from which to retrieve the birth date.
   * @return The patient's age at the time of specimen collection in years, or null if the age calculation fails.
   */
  private String determinePatientAgeAtCollection(Patient patient) {
    String age = null;

    try {
      Date birthDate = patient.getBirthDate();
      if (birthDate == null) {
        birthDate = patient.getBirthDateElement().getValue();
        if (birthDate == null) {
          if (nullAgeCounter++ < 5) { // Don't show this warning too many times
            logger.warn("determinePatientAgeAtCollection: patient.getBirthDate() is null, returning null.");
            logger.warn("determinePatientAgeAtCollection: patient: " + patient.getIdElement().getIdPart());
          }
          return null;
        }
      }

      // Get the patient's birth date as a LocalDate object
      LocalDate localBirthDate = birthDate.toInstant()
              .atZone(java.time.ZoneId.systemDefault())
              .toLocalDate();
      LocalDate collectionDate = getEarliestCollectionDate(patient);
      if (collectionDate == null) {
        logger.warn("determinePatientAgeAtCollection: earliest specimen collection date is null");
        return null;
      }

      // Calculate the patient's age in years using the Period class
      int ageInYears = Period.between(localBirthDate, collectionDate).getYears();

      if (ageInYears < 0) {
        logger.warn("determinePatientAgeAtCollection: age at collection is negative, substituting null");
        age = null;
      } else
        age = Integer.toString(ageInYears);
    } catch (Exception e) {
      logger.warn("determinePatientAgeAtCollection: problem determining patient age, following exception caught: " + Util.traceFromException(e));
    }

    if (age == null)
      logger.warn("determinePatientAgeAtCollection: returning null.");

    return age;
  }

  Map <Patient,LocalDate> patientEarliestCollectionDateMap = new HashMap<>();
  /**
   * Retrieves the earliest specimen collection date for a given patient.
   *
   * <p>This method queries the underlying FHIR API to find all {@link Specimen}
   * resources that reference the specified {@link Patient}. For each specimen,
   * it extracts a collection date using {@code extractCollectionLocalDateFromSpecimen}.
   * Any specimens that return a null collection date are skipped. Among the
   * remaining specimen dates, the method selects and returns the earliest one
   * according to natural ordering (i.e. chronologically).
   *
   * <p>If no specimens with a valid collection date are found, the method returns
   * {@code null}.
   *
   * @param patient the patient whose specimens are to be searched; must not be {@code null}
   * @return the earliest {@link LocalDate} found among this patient's specimens, or {@code null}
   *         if no valid collection date is available
   */
  private LocalDate getEarliestCollectionDate(Patient patient) {
    // This method is slow, so use cached results if available.
    if (patientEarliestCollectionDateMap.containsKey(patient))
      return patientEarliestCollectionDateMap.get(patient);

    // Loop over all specimens associated with the patient and find the earliest collection date.
    LocalDate earliestCollectionDate = fhirApi.findAllSpecimensWithReferencesToPatient(patient).stream()
            .flatMap(specimen ->
                    Optional.ofNullable(extractCollectionLocalDateFromSpecimen(specimen))
                            .stream()
            )
            .min(Comparator.naturalOrder())
            .orElse(null);
    patientEarliestCollectionDateMap.put(patient,earliestCollectionDate); // cache the result
    return earliestCollectionDate;
  }


  /**
   * Extracts the collection date as a LocalDate from the given FHIR Specimen.
   * If the Specimen is null or does not have a collection date, it returns null.
   *
   * @param specimen The FHIR Specimen object from which to extract the collection date.
   * @return The collection date as a LocalDate, or null if the specimen is null or lacks a collection date.
   *
   * @throws NullPointerException if specimen is null.
   */
  private LocalDate extractCollectionLocalDateFromSpecimen(Specimen specimen) {
    // Check if the specimen is null or has no collection date
    if (specimen == null) {
      logger.warn("extractCollectionLocalDateFromSpecimen: specimen is null, returning null");
      return null;
    }
    if (!specimen.hasCollection()) {
      logger.warn("extractCollectionLocalDateFromSpecimen: specimen has no collection date, returning null");
      return null;
    }

    Specimen.SpecimenCollectionComponent collection = specimen.getCollection();
    if (collection.hasCollectedDateTimeType()) {
      DateTimeType collected = collection.getCollectedDateTimeType();
      Date date = collected.getValue(); // Get the java.util.Date object

      return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    } else {
      logger.warn("extractCollectionLocalDateFromSpecimen: no date/time for specimen collection, returning null");
      return null;
    }
  }

  /**
   * Extracts unique diagnoses associated with a given Patient and Specimen.
   * This method combines diagnoses obtained from the Patient's conditions and Specimen's diagnoses.
   *
   * @param patient The FHIR Patient object from which to extract diagnoses.
   * @param specimen The FHIR Specimen object from which to extract diagnoses.
   * @return A List of unique diagnoses associated with the given Patient and Specimen.
   *
   * @throws NullPointerException if either patient or specimen is null.
   */
  private List<String> extractDiagnosesFromPatientAndSpecimen(Patient patient, Specimen specimen) {
    // Find any diagnoses associated with this patient
    List<String> patientConditionCodes = fhirApi.extractConditionCodesFromPatient(patient);

    // Find any diagnoses associated with this specimen
    List<String> diagnosesFromSpecimen = fhirApi.extractDiagnosesFromSpecimen(specimen);

    // Combine diagnosis lists
    return Stream.concat(patientConditionCodes.stream(), diagnosesFromSpecimen.stream())
      .distinct()
      .collect(Collectors.toList());
  }

    /**
     * Extracts the material from a Specimen object.
     * <p>
     * This method returns the text or the code of the type element of the Specimen object,
     * or null if the type element is missing or empty.
     * </p>
     * @param specimen the Specimen object to extract the material from
     * @return the material as a String, or null if not available
     */
    private String extractMaterialFromSpecimen(Specimen specimen) {
        String material = null;

        CodeableConcept type = specimen.getType();
        if (type.hasText())
            material = type.getText();
        else {
            List<Coding> coding = type.getCoding();
            if (coding.size() > 0)
                material = coding.get(0).getCode();
        }

        return material;
    }
}
