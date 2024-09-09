package de.samply.directory_sync_service.fhir;

import ca.uhn.fhir.context.FhirContext;

import de.samply.directory_sync_service.model.StarModelData;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.fhir.model.FhirCollection;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Specimen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides functionality related to FHIR MeasureReports.
 */
public class FhirReporting {

  private static final Logger logger = LoggerFactory.getLogger(FhirReporting.class);

  private static final String LIBRARY_URI = "https://fhir.bbmri.de/Library/collection-size";
  private static final String MEASURE_URI = "https://fhir.bbmri.de/Measure/collection-size";
  private static final String STORAGE_TEMPERATURE_URI = "https://fhir.bbmri.de/StructureDefinition/StorageTemperature";
  private static final String SAMPLE_DIAGNOSIS_URI = "https://fhir.bbmri.de/StructureDefinition/SampleDiagnosis";
  private final FhirContext fhirContext = FhirContext.forR4();
  private final FhirApi fhirApi;

public FhirReporting(FhirApi fhirApi) {
  this.fhirApi = fhirApi;
}

  /**
   * Pulls information relevant to collections from the FHIR store.
   * <p>
   * Returns a list of FhirCollection objects, one per collection.
   * 
   * @param defaultBbmriEricCollectionId
   * @return
   */
  public List<FhirCollection> fetchFhirCollections(BbmriEricId defaultBbmriEricCollectionId) {
    Map<String,FhirCollection> fhirCollectionMap = new HashMap<String,FhirCollection>();

    // Group specimens according to collection, extract aggregated information
    // from each group, and put this information into FhirCollection objects.
    Map<String, List<Specimen>> specimensByCollection = fhirApi.fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    if (specimensByCollection == null) {
        logger.warn("fetchFhirCollections: Problem finding specimens");
        return null;
    }
    updateFhirCollectionsWithSpecimenData(fhirCollectionMap, specimensByCollection);

    // Group patients according to collection, extract aggregated information
    // from each group, and put this information into FhirCollection objects.
    Map<String, List<Patient>> patientsByCollection = fhirApi.fetchPatientsByCollection(specimensByCollection);
    if (patientsByCollection == null) {
        logger.warn("fetchFhirCollections: Problem finding patients");
        return null;
    }
    updateFhirCollectionsWithPatientData(fhirCollectionMap, patientsByCollection);

    return new ArrayList<FhirCollection>(fhirCollectionMap.values());
  }

  private void updateFhirCollectionsWithSpecimenData(Map<String,FhirCollection> entities, Map<String, List<Specimen>> specimensByCollection) {
      for (String key: specimensByCollection.keySet()) {
          List<Specimen> specimenList = specimensByCollection.get(key);
          FhirCollection fhirCollection = entities.getOrDefault(key, new FhirCollection());
          fhirCollection.setId(key);
          fhirCollection.setSize(specimenList.size());
          fhirCollection.setMaterials(extractMaterialsFromSpecimenList(specimenList));
          fhirCollection.setStorageTemperatures(fhirApi.extractExtensionElementValuesFromSpecimens(specimenList, STORAGE_TEMPERATURE_URI));
          fhirCollection.setDiagnosisAvailable(fhirApi.extractExtensionElementValuesFromSpecimens(specimenList, SAMPLE_DIAGNOSIS_URI));
          entities.put(key, fhirCollection);
      }
  }

  private void updateFhirCollectionsWithPatientData(Map<String,FhirCollection> entities, Map<String, List<Patient>> patientsByCollection) {
      for (String key: patientsByCollection.keySet()) {
          List<Patient> patientList = patientsByCollection.get(key);
          FhirCollection fhirCollection = entities.getOrDefault(key, new FhirCollection());
          fhirCollection.setNumberOfDonors(patientList.size());
          fhirCollection.setSex(extractSexFromPatientList(patientList));
          fhirCollection.setAgeLow(extractAgeLowFromPatientList(patientList));
          fhirCollection.setAgeHigh(extractAgeHighFromPatientList(patientList));
          entities.put(key, fhirCollection);
      }
  }

  public StarModelData fetchStarModelInputData(BbmriEricId defaultBbmriEricCollectionId) {
      PopulateStarModelInputData populateStarModelInputData = new PopulateStarModelInputData(fhirApi);
      StarModelData starModelInputData = populateStarModelInputData.populate(defaultBbmriEricCollectionId);

      return starModelInputData;
  }
  
  /**
   * Fetches diagnoses from Specimens and Patients to which collections can be assigned.
   * <p>
   * This method retrieves specimens grouped by collection.
   * <p>
   * It then extracts diagnoses from Specimen extensions and Patient condition codes, eliminating duplicates,
   * and combines the results into a list of unique diagnoses.
   *
   * @param defaultBbmriEricCollectionId The BBMRI ERIC collection ID to fetch specimens and diagnoses.
   * @return a List of unique diagnoses.
   */
  public List<String> fetchDiagnoses(BbmriEricId defaultBbmriEricCollectionId) {
    logger.info("fetchDiagnoses: defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);
    // Group specimens according to collection.
    Map<String, List<Specimen>> specimensByCollection = fhirApi.fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    if (specimensByCollection == null) {
        logger.warn("fetchDiagnoses: Problem finding specimens");
        return null;
    }

    // Get diagnoses from Specimen extensions
    List<String> diagnoses = specimensByCollection.values().stream()
      .flatMap(List::stream)
      .map(s -> fhirApi.extractDiagnosesFromSpecimen(s))
      .flatMap(List::stream)
      .distinct()
      .collect(Collectors.toList());

    // Get diagnoses from Patients
    Map<String, List<Patient>> patientsByCollection = fhirApi.fetchPatientsByCollection(specimensByCollection);
    List<String> patientDiagnoses = patientsByCollection.values().stream()
      .flatMap(List::stream)
      .map(s -> fhirApi.extractConditionCodesFromPatient(s))
      .flatMap(List::stream)
      .distinct()
      .collect(Collectors.toList());

    // Combine diagnoses from specimens and patients, ensuring that there
    // are no duplicates.
    diagnoses = Stream.concat(diagnoses.stream(), patientDiagnoses.stream())
      .distinct()
      .collect(Collectors.toList());

    return diagnoses;
  }

  /**
   * Extracts unique material codes from a list of specimens.
   *
   * @param specimenList A list of {@code Specimen} objects from which to extract material codes.
   * @return A list of unique material codes (as strings) extracted from the specimens.
   */
    private List<String> extractMaterialsFromSpecimenList(List<Specimen> specimenList) {
        if (specimenList == null)
            logger.info("extractMaterialsFromSpecimenList: specimenList is null");
        else
            logger.info("extractMaterialsFromSpecimenList: specimenList.size: " + specimenList.size());
        Set<String> materialSet = new HashSet<>();
        for (Specimen specimen : specimenList) {
            CodeableConcept codeableConcept = specimen.getType();
            if (codeableConcept != null && codeableConcept.getCoding().size() > 0) {
                materialSet.add(codeableConcept.getCoding().get(0).getCode());
            }
        }

        return new ArrayList<>(materialSet);
    }

    private List<String> extractSexFromPatientList(List<Patient> patients) {
    return patients.stream()
            .filter(patient -> Objects.nonNull(patient.getGenderElement())) // Filter out patients with null gender
            .map(patient -> patient.getGenderElement().getValueAsString()) // Map each patient to their gender
            .collect(Collectors.toSet()).stream().collect(Collectors.toList()); // Collect the results into a new list
  }

  private Integer extractAgeLowFromPatientList(List<Patient> patients) {
    return patients.stream()
            // Filter out patients with null age
            .filter(p -> Objects.nonNull(determinePatientAge(p)))
            // Map each patient to their age
            .mapToInt(p -> determinePatientAge(p))
            // Find the minimum age
            .min()
            // Get the result as an int or a default value
            .orElse(-1);
  }

  private Integer extractAgeHighFromPatientList(List<Patient> patients) {
    return patients.stream()
            // Filter out patients with null age
            .filter(p -> Objects.nonNull(determinePatientAge(p)))
            // Map each patient to their age
            .mapToInt(p -> determinePatientAge(p))
            // Find the maximum age
            .max()
            // Get the result as an int or a default value
            .orElse(-1);
  }

  private Integer determinePatientAge(Patient patient) {
    if (!patient.hasBirthDate())
      return null;

    // Get the patient's date of birth as a Date object
    Date birthDate = patient.getBirthDate();

    // Convert the Date object to a LocalDate object
    LocalDate birthDateLocal = birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

    // Get the current date as a LocalDate object
    LocalDate currentDate = LocalDate.now();

    // Calculate the difference between the two dates in years
    int age = currentDate.getYear() - birthDateLocal.getYear();

    // Adjust the age if the current date is before the patient's birthday
    if (currentDate.getDayOfYear() < birthDateLocal.getDayOfYear())
      age--;

    return age;
  }
}
