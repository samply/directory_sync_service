package de.samply.directory_sync_service.fhir;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Specimen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.samply.directory_sync_service.model.StarModelData;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.model.BbmriEricId;
import io.vavr.control.Either;

/**
 * Pull data about Patients, Specimens and Dieseases from the FHIR store and
 * use the information they contain to fill a StarModelInputData object.
 */
public class PopulateStarModelInputData {
  private static final Logger logger = LoggerFactory.getLogger(PopulateStarModelInputData.class);
  private FhirApi fhirApi;

  public PopulateStarModelInputData(FhirApi fhirApi) {
    this.fhirApi = fhirApi;
  }

  /**
   * Populates a Star Model input data object based on specimens fetched from the FHIR server,
   * grouped according to the specified default BBMRI-ERIC collection ID.
   *
   * @param defaultBbmriEricCollectionId The default BBMRI-ERIC collection ID to group specimens. May be null.
   * @return A StarModelData object populated with data extracted from the fetched specimens.
   */
  public StarModelData populate(BbmriEricId defaultBbmriEricCollectionId) {
    // Group specimens according to collection.
    Either<OperationOutcome, Map<String, List<Specimen>>> specimensByCollectionOutcome = fhirApi.fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    if (specimensByCollectionOutcome.isLeft()) {
      logger.error("Problem finding specimens");
      return null;
    }
    Map<String, List<Specimen>> specimensByCollection = specimensByCollectionOutcome.get();

    StarModelData starModelInputData = new StarModelData();
    for (String collectionId: specimensByCollection.keySet())
      populateCollection(starModelInputData, collectionId, specimensByCollection.get(collectionId));

    return starModelInputData;
  }

  /**
   * Populates the Star Model input data with information extracted from a list of specimens
   * associated with a specific collection.
   *
   * @param starModelInputData The Star Model input data to be populated.
   * @param collectionId The identifier for the collection to which the specimens belong.
   * @param specimens The list of specimens from which to extract data and populate the input data.
   *
   * @throws NullPointerException if starModelInputData, collectionId, or specimens is null.
   */
  private void populateCollection(StarModelData starModelInputData, String collectionId, List<Specimen> specimens) {
    for (Specimen specimen: specimens)
      populateSpecimen(starModelInputData, collectionId, specimen);
  }

  /**
   * Populates the Star Model input data with information extracted from a single specimen.
   *
   * @param starModelInputData The Star Model input data to be populated.
   * @param collectionId The identifier for the collection to which the specimen belongs.
   * @param specimen The specimen from which to extract data and populate the input data.
   *
   * @throws NullPointerException if starModelInputData, collectionId, or specimen is null.
   */
  private void populateSpecimen(StarModelData starModelInputData, String collectionId, Specimen specimen) {
    // Get the Patient who donated the sample
    Patient patient = fhirApi.extractPatientFromSpecimen(specimen);

    String material = extractMaterialFromSpecimen(specimen);
    String patientId = patient.getIdElement().getIdPart();
    String sex = patient.getGender().getDisplay();
    String age = determinePatientAgeAtCollection(patient, specimen);

    // Create a new Row object to hold data extracted from patient and specimen
    StarModelData.InputRow row = starModelInputData.newInputRow(collectionId, material, patientId, sex, age);

    List<String> diagnoses = extractDiagnosesFromPatientAndSpecimen(patient, specimen);

    // Add all of the collected information to the input data table.
    for (String diagnosis: diagnoses)
      starModelInputData.addInputRow(collectionId, starModelInputData.newInputRow(row, diagnosis));
  }

  /**
   * Determines the patient's age at the time of specimen collection.
   *
   * @param patient The FHIR Patient object from which to retrieve the birth date.
   * @param specimen The FHIR Specimen object from which to extract the collection date.
   * @return The patient's age at the time of specimen collection in years, or null if the age calculation fails.
   *
   * @throws NullPointerException if either patient or specimen is null.
   * @throws RuntimeException if an unexpected error occurs during the age calculation.
   */
  private String determinePatientAgeAtCollection(Patient patient, Specimen specimen) {
    String age = null;

    try {
      Date birthDate = patient.getBirthDate();
      if (birthDate == null) {
        logger.warn("determinePatientAgeAtCollection: patient.getBirthDate() is null, returning null.");
        return null;
      }
      // Get the patient's birth date as a LocalDate object
      LocalDate localBirthDate = birthDate.toInstant()
              .atZone(java.time.ZoneId.systemDefault())
              .toLocalDate();

      LocalDate collectionDate = extractCollectionLocalDateFromSpecimen(specimen);
      if (collectionDate == null) {
        logger.warn("determinePatientAgeAtCollection: extractCollectionLocalDateFromSpecimen is null, returning null.");
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
      LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

      return localDate;
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
