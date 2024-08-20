package de.samply.directory_sync_service.fhir;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.empty;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.io.ByteStreams;

import de.samply.directory_sync_service.model.StarModelData;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.model.BbmriEricId;
import de.samply.directory_sync_service.fhir.model.FhirCollection;
import io.vavr.Tuple;
import io.vavr.control.Either;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.Measure;
import org.hl7.fhir.r4.model.MeasureReport;
import org.hl7.fhir.r4.model.MeasureReport.StratifierGroupComponent;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
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

  private final FhirContext fhirContext;
  private final FhirApi fhirApi;

  public FhirReporting(FhirContext fhirContext, FhirApi fhirApi) {
    this.fhirContext = Objects.requireNonNull(fhirContext);
    this.fhirApi = Objects.requireNonNull(fhirApi);
  }

  /**
   * The returned map key is an optional FHIR logical ID. The empty case encompasses all Specimen
   * which are not assigned to a Collection.
   */
  private static Map<Optional<String>, Integer> extractStratifierCounts(MeasureReport report) {
    return report.getGroupFirstRep().getStratifierFirstRep().getStratum().stream()
        .collect(Collectors.toMap(FhirReporting::extractFhirId,
            stratum -> stratum.getPopulationFirstRep().getCount(),
            Integer::sum));
  }

  private static Optional<String> extractFhirId(StratifierGroupComponent stratum) {
    String[] parts = stratum.getValue().getText().split("/");
    return parts.length == 2 ? Optional.of(parts[1]) : empty();
  }

  /**
   * Maps the logical FHIR ID keys of {@code counts} to BBMRI-ERIC ID keys using
   * {@code collections}.
   *
   * @param counts      map from FHIR logical ID to counts
   * @param collections list of Organization resources to use for resolving the BBMRI-ERIC ID's
   * @return a map of BBMRI_ERIC ID to counts
   */
  private static Map<BbmriEricId, Integer> resolveBbmriEricIds(Map<String, Integer> counts,
      List<Organization> collections) {
    return collections.stream()
        .map(c -> Tuple.of(FhirApi.bbmriEricId(c), counts.get(c.getIdElement().getIdPart())))
        .filter(t -> t._1.isPresent())
        .filter(t -> t._2 != null)
        .collect(Collectors.toMap(t -> t._1.get(), t -> t._2, Integer::sum));
  }

  /**
   * Tries to create Library and Measure resources if not present on the FHIR server.
   *
   * @return either an error or nothing
   */
  public Either<String, Void> initLibrary() {
    logger.info("initLibrary: entered");
    return fhirApi.resourceExists(Library.class, LIBRARY_URI)
        .flatMap(exists -> exists
            ? Either.right(null)
            : slurp("CollectionSize.Library.json")
                .flatMap(s -> parseResource(Library.class, s))
                .flatMap(this::appendCql)
                .flatMap(fhirApi::createResource));
  }

  public Either<String, Void> initMeasure() {
    logger.info("initMeasure: entered");
    return fhirApi.resourceExists(Measure.class, MEASURE_URI)
        .flatMap(exists -> exists
            ? Either.right(null)
            : slurp("CollectionSize.Measure.json")
                .flatMap(s -> parseResource(Measure.class, s))
                .flatMap(fhirApi::createResource));
  }

  private static Either<String, String> slurp(String name) {
    logger.info("slurp: file name: " + name);
    try (InputStream in = FhirApi.class.getResourceAsStream(name)) {
      if (in == null) {
        logger.error("file `{}` not found in classpath", name);
        return Either.left(format("file `%s` not found in classpath", name));
      } else {
        logger.info("read file `{}` from classpath", name);
        return Either.right(new String(ByteStreams.toByteArray(in), UTF_8));
      }
    } catch (IOException e) {
      logger.error("error while reading the file `{}` from classpath", name, e);
      return Either.left(format("error while reading the file `%s` from classpath", name));
    }
  }

  private <T extends IBaseResource> Either<String, T> parseResource(Class<T> type, String s) {
    logger.info("parseResource: s: " + s);
    IParser parser = fhirContext.newJsonParser();
    logger.info("parseResource: try parsing it");
    try {
      return Either.right(type.cast(parser.parseResource(s)));
    } catch (Exception e) {
      return Either.left(e.getMessage());
    }
  }

  private Either<String, Library> appendCql(Library library) {
    return slurp("CollectionSize.cql").map(cql -> {
      library.getContentFirstRep().setContentType("text/cql");
      library.getContentFirstRep().setData(cql.getBytes(UTF_8));
      return library;
    });
  }

  /**
   * Returns collection sample counts indexed by BBMRI-ERIC identifier.
   * <p>
   * Executes the <a href="https://fhir.bbmri.de/Measure/collection-size">collection-size</a>
   * measure.
   * <p>
   * In case all samples are unassigned, meaning the stratum code has text {@literal null} and only
   * one collection exists, all that samples are assigned to this single collection.
   *
   * @return collection sample counts indexed by BBMRI-ERIC identifier or OperationOutcome
   * indicating an error
   */
  public Either<OperationOutcome, Map<BbmriEricId, Integer>> fetchCollectionSizes() {
    return fhirApi.evaluateMeasure(MEASURE_URI)
        .map(FhirReporting::extractStratifierCounts)
        .flatMap(counts -> {
          if (counts.size() == 1 && counts.containsKey(Optional.<String>empty())) {
            return fhirApi.listAllCollections()
                .map(collections -> {
                  if (collections.size() == 1) {
                    return FhirApi.bbmriEricId(collections.get(0))
                        .map(ericId -> Util.mapOf(ericId, counts.get(Optional.<String>empty())))
                        .orElseGet(Util::mapOf);
                  } else {
                    return Util.mapOf();
                  }
                });
          } else {
            return fhirApi.fetchCollections(filterPresents(counts.keySet()))
                .map(collections -> resolveBbmriEricIds(filterPresents(counts), collections));
          }
        });
  }

  /**
   * Pulls information relevant to collections from the FHIR store.
   * 
   * Returns a list of FhirCollection objects, one per collection.
   * 
   * @param defaultBbmriEricCollectionId
   * @return
   */
  public Either<OperationOutcome, List<FhirCollection>> fetchFhirCollections(BbmriEricId defaultBbmriEricCollectionId) {
    Map<String,FhirCollection> fhirCollectionMap = new HashMap<String,FhirCollection>();

    // Group specimens according to collection, extract aggregated information
    // from each group, and put this information into FhirCollection objects.
    Either<OperationOutcome, Map<String, List<Specimen>>> specimensByCollectionOutcome = fhirApi.fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    if (specimensByCollectionOutcome.isLeft())
      return Either.left(createOutcomeWithError("fetchFhirCollections: Problem finding specimens"));
    updateFhirCollectionsWithSpecimenData(fhirCollectionMap, specimensByCollectionOutcome.get());

    // Group patients according to collection, extract aggregated information
    // from each group, and put this information into FhirCollection objects.
    Either<OperationOutcome, Map<String, List<Patient>>> patientsByCollectionOutcome = fhirApi.fetchPatientsByCollection(specimensByCollectionOutcome.get());
    if (patientsByCollectionOutcome.isLeft()) 
      return Either.left(createOutcomeWithError("Problem finding patients"));
    updateFhirCollectionsWithPatientData(fhirCollectionMap, patientsByCollectionOutcome.get());

    return Either.right(new ArrayList<FhirCollection>(fhirCollectionMap.values()));
  }

  private void updateFhirCollectionsWithSpecimenData(Map<String,FhirCollection> entities, Map<String, List<Specimen>> specimensByCollection) {
      for (String key: specimensByCollection.keySet()) {
          List<Specimen> specimenList = specimensByCollection.get(key);
          FhirCollection fhirCollection = entities.getOrDefault(key, new FhirCollection());
          fhirCollection.setId(key);
          fhirCollection.setSize(specimenList.size());
          fhirCollection.setMaterials(extractMaterialsFromSpecimenList(specimenList));
          fhirCollection.setStorageTemperatures(extractStorageTemperaturesFromSpecimenList(specimenList));
          fhirCollection.setDiagnosisAvailable(extractDiagnosesFromSpecimenList(specimenList));
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

  public Either<OperationOutcome, StarModelData> fetchStarModelInputData(BbmriEricId defaultBbmriEricCollectionId) {
      PopulateStarModelInputData populateStarModelInputData = new PopulateStarModelInputData(fhirApi);
      StarModelData starModelInputData = populateStarModelInputData.populate(defaultBbmriEricCollectionId);

      return Either.right(starModelInputData);
  }
  
  /**
   * Fetches diagnoses from Specimens and Patients to which collections can be assigned.
   *
   * This method retrieves specimens grouped by collection.
   * 
   * It then extracts diagnoses from Specimen extensions and Patient condition codes, eliminating duplicates,
   * and combines the results into a list of unique diagnoses.
   *
   * @param defaultBbmriEricCollectionId The BBMRI ERIC collection ID to fetch specimens and diagnoses.
   * @return Either an OperationOutcome indicating an error or a List of unique diagnoses.
   *         If an error occurs during the fetching process, an OperationOutcome with an error message is returned.
   *         Otherwise, a List of unique diagnoses is returned.
   */
  public Either<OperationOutcome, List<String>> fetchDiagnoses(BbmriEricId defaultBbmriEricCollectionId) {
    logger.info("fetchDiagnoses: defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);
    // Group specimens according to collection.
    Either<OperationOutcome, Map<String, List<Specimen>>> specimensByCollectionOutcome = fhirApi.fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    if (specimensByCollectionOutcome.isLeft())
      return Either.left(createOutcomeWithError("fetchDiagnoses: Problem finding specimens"));
    Map<String, List<Specimen>> specimensByCollection = specimensByCollectionOutcome.get();

    // Get diagnoses from Specimen extensions
    List<String> diagnoses = specimensByCollection.values().stream()
      .flatMap(List::stream)
      .map(s -> fhirApi.extractDiagnosesFromSpecimen(s))
      .flatMap(List::stream)
      .distinct()
      .collect(Collectors.toList());

    // Get diagnoses from Patients
    Either<OperationOutcome, Map<String, List<Patient>>> patientsByCollectionOutcome = fhirApi.fetchPatientsByCollection(specimensByCollection);
    Map<String, List<Patient>> patientsByCollection = patientsByCollectionOutcome.get();
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

    return Either.right(diagnoses);
  }

  private OperationOutcome createOutcomeWithError(String message) {
      OperationOutcome outcome = new OperationOutcome();
      outcome.addIssue().setSeverity(ERROR).setDiagnostics(message);
      return outcome;
  }

/*
  private List<String> extractMaterialsFromSpecimenList(List<Specimen> specimens) {
    return specimens.stream()
            // Map each specimen to its type
            .map(Specimen::getType)
            // Map each CodeableConcept to its display name
            .map(c -> c.getCoding().get(0).getCode())
            // Collect the results into a non-duplicating list
            .collect(Collectors.toSet()).stream().collect(Collectors.toList());
  }
*/

  /**
   * Extracts unique material codes from a list of specimens.
   *
   * @param specimens A list of {@code Specimen} objects from which to extract material codes.
   * @return A list of unique material codes (as strings) extracted from the specimens.
   */
  private List<String> extractMaterialsFromSpecimenList(List<Specimen> specimens) {
    // Print a log info
    logger.info("extractMaterialsFromSpecimenList: entered");
    logger.info("extractMaterialsFromSpecimenList: specimens: " + specimens);
    logger.info("extractMaterialsFromSpecimenList: Number of specimens: " + specimens.size());

    // Step 1: Stream the list of specimens
    // Convert the list of specimens to a stream to process each element individually
    Stream<Specimen> specimenStream = specimens.stream();

    logger.info("extractMaterialsFromSpecimenList: step 2");

    // Step 2: Map each specimen to its type (returns a CodeableConcept object)
    Stream<CodeableConcept> typeStream = specimenStream.map(Specimen::getType);

    logger.info("extractMaterialsFromSpecimenList: typeStream: " + typeStream);
    logger.info("extractMaterialsFromSpecimenList: step 3");

    // Step 3: Map each CodeableConcept to its first coding's code
    Stream<String> codeStream = typeStream
            // Filter out any CodeableConcept objects where getCoding returns an empty list
            .filter(c -> c.getCoding() != null && !c.getCoding().isEmpty())
            // Map each remaining CodeableConcept to its first coding's code
            .map(c -> c.getCoding().get(0).getCode());

    logger.info("extractMaterialsFromSpecimenList: codeStream: " + codeStream);
    logger.info("extractMaterialsFromSpecimenList: step 4");

    // Step 4: Collect the results into a Set to remove duplicates
    Set<String> uniqueCodes = codeStream.collect(Collectors.toSet());

    logger.info("extractMaterialsFromSpecimenList: step 5");

    // Step 5: Convert the Set back into a List and return
    List<String> uniqueCodeList = uniqueCodes.stream().collect(Collectors.toList());

    logger.info("extractMaterialsFromSpecimenList: returning");

    return uniqueCodeList;
  }

  private List<String> extractStorageTemperaturesFromSpecimenList(List<Specimen> specimens) {
    return fhirApi.extractExtensionElementValuesFromSpecimens(specimens, STORAGE_TEMPERATURE_URI);
  }

  private List<String> extractDiagnosesFromSpecimenList(List<Specimen> specimens) {
    return fhirApi.extractExtensionElementValuesFromSpecimens(specimens, SAMPLE_DIAGNOSIS_URI);
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

  private static <T> Set<T> filterPresents(Set<Optional<T>> optionals) {
    return optionals.stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet());
  }

  private static <K, V> Map<K, V> filterPresents(Map<Optional<K>, V> optionals) {
    return filterPresents(optionals.keySet()).stream()
        .collect(Collectors.toMap(Function.identity(), k -> optionals.get(Optional.of(k))));
  }
}
