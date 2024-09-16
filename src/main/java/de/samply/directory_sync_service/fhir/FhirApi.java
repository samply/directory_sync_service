package de.samply.directory_sync_service.fhir;

import static ca.uhn.fhir.rest.api.PreferReturnEnum.OPERATION_OUTCOME;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.fhir.model.FhirCollection;
import de.samply.directory_sync_service.model.BbmriEricId;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.function.Predicate;
import java.util.function.Function;
import java.util.HashSet;

import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.Specimen;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides convenience methods for selected FHIR operations.
 */
public class FhirApi {
  private static final Logger logger = LoggerFactory.getLogger(FhirApi.class);
  private static final String STORAGE_TEMPERATURE_URI = "https://fhir.bbmri.de/StructureDefinition/StorageTemperature";
  private static final String SAMPLE_DIAGNOSIS_URI = "https://fhir.bbmri.de/StructureDefinition/SampleDiagnosis";
  private static final String BIOBANK_PROFILE_URI = "https://fhir.bbmri.de/StructureDefinition/Biobank";
  private static final String COLLECTION_PROFILE_URI = "https://fhir.bbmri.de/StructureDefinition/Collection";
  private static final String DEFAULT_COLLECTION_ID = "DEFAULT";
  private Map<String, List<Specimen>> specimensByCollection = null;
  private Map<String, List<Patient>> patientsByCollection = null;
  private final IGenericClient fhirClient;
  private FhirContext ctx;

  public FhirApi(String fhirStoreUrl) {
    ctx = FhirContext.forR4();
    IGenericClient client = ctx.newRestfulGenericClient(fhirStoreUrl);
    client.registerInterceptor(new LoggingInterceptor(true));

    this.fhirClient =  client;
  }

  /**
   * Returns the BBMRI-ERIC identifier of {@code collection} if some valid one could be found.
   *
   * @param collection the Organization resource, possibly containing a BBMRI-ERIC identifier
   * @return the found BBMRI-ERIC identifier or {@link Optional#empty empty}
   */
  public static Optional<BbmriEricId> bbmriEricId(Organization collection) {
    return collection.getIdentifier().stream()
        .filter(i -> "http://www.bbmri-eric.eu/".equals(i.getSystem()))
        .findFirst().map(Identifier::getValue).flatMap(BbmriEricId::valueOf);
  }

  /**
   * Updates the provided resource on the FHIR server.
   *
   * This method updates the specified resource on the FHIR server and returns the operation outcome.
   *
   * @param resource The resource to be updated on the FHIR server.
   * @return The operation outcome of the update operation.
   *         If the update is successful, the operation outcome will contain information about the update.
   *         If an exception occurs during the update process, an error operation outcome will be returned.
   */
  public OperationOutcome updateResource(IBaseResource resource) {
    logger.info("updateResource: @@@@@@@@@@ entered");

    // Remove the version ID, so that no If-Match header gets added to the request
    // If you don't do this, Blaze will throw an exception like this:
    // "Precondition `\"1\"` failed on `Organization/biobank-0`.", :cognitect.anomalies/category :cognitect.anomalies/conflict, :http/status 412}
    // According to Alex: . Diese Fehlermeldung kommt, wenn Du entweder beim Laden einen If-Match header mitschickst oder im Request Teil vom Transaction Bundle eine ifMatch Property angibst
    resource.setId(resource.getIdElement().toUnqualifiedVersionless());

    try {
      IBaseOperationOutcome outcome = fhirClient
              .update()
              .resource(resource)
              .prefer(OPERATION_OUTCOME)
              .execute()
              .getOperationOutcome();
      logger.info("updateResource: @@@@@@@@@@ return outcome: " +outcome);
      return (OperationOutcome) outcome;
    } catch (Exception e) {
      logger.info("updateResource: @@@@@@@@@@ exception: " + Util.traceFromException(e));
      OperationOutcome outcome = new OperationOutcome();
      outcome.addIssue().setSeverity(ERROR).setDiagnostics(e.getMessage());
      return outcome;
    }
  }

  /**
   * Lists all Organization resources with the biobank profile.
   *
   * @return a list of {@link Organization} resources or null on *
   * errors
   */
  public List<Organization> listAllBiobanks() {
    // List all organizations with the specified biobank profile URI
    Bundle organizationBundle = listAllOrganizations(BIOBANK_PROFILE_URI);

    // Check if the operation was successful
    if (organizationBundle == null) {
      logger.warn("listAllBiobanks: there was a problem during listAllOrganizations");
      return null;
    }

    // Extract the organizations from the Bundle
    List<Organization> organizations = extractOrganizations(organizationBundle, BIOBANK_PROFILE_URI);

    // Return the list of organizations
    return organizations;
  }

  private List<Organization> listAllCollections() {
    // List all organizations with the specified biobank profile URI
    Bundle organizationBundle = listAllOrganizations(COLLECTION_PROFILE_URI);

    // Check if the operation was successful
    if (organizationBundle == null) {
      logger.warn("listAllCollections: there was a problem during listAllOrganizations");
      return null;
    }

    // Extract the organizations from the Bundle
    List<Organization> organizations = extractOrganizations(organizationBundle, COLLECTION_PROFILE_URI);

    // Return the list of organizations
    return organizations;
  }

  private Bundle listAllOrganizations(String profileUri) {
    try {
      return (Bundle) fhirClient.search().forResource(Organization.class)
              .withProfile(profileUri).execute();
    } catch (Exception e) {
      Util.traceFromException(e);
      return null;
    }
  }

  private static List<Organization> extractOrganizations(Bundle bundle, String profileUrl) {
    return bundle.getEntry().stream()
        .map(BundleEntryComponent::getResource)
        .filter(r -> r.getResourceType() == ResourceType.Organization)
        .filter(r -> r.getMeta().hasProfile(profileUrl))
        .map(r -> (Organization) r)
        .collect(Collectors.toList());
  }

  /**
   * Fetches specimens from the FHIR server and groups them by their collection id.
   * If no default collection id is provided, tries to find one from the available collections.
   * If the default collection id is invalid or not found, removes the specimens without a collection id from the result.
   *
   * @param defaultBbmriEricCollectionId the default collection id supplied by the site, to be used for specimens without a collection id. May be null
   * @return a map of collection id to list of specimens, or null in case of an error
   */
  public Map<String,List<Specimen>> fetchSpecimensByCollection(BbmriEricId defaultBbmriEricCollectionId) {
    logger.info("__________ fetchSpecimensByCollection: entered");

    // This method is slow, so use cached value if available.
    if (specimensByCollection != null)
      return specimensByCollection;

    logger.info("__________ fetchSpecimensByCollection: get specimens from FHIR store");

    try {
      specimensByCollection = getAllSpecimensAsMap();

      logger.info("__________ fetchSpecimensByCollection: specimensByCollection size: " + specimensByCollection.size());

      defaultBbmriEricCollectionId = determineDefaultCollectionId(defaultBbmriEricCollectionId, specimensByCollection);

      logger.info("__________ fetchSpecimensByCollection: defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);

      // Remove specimens without a collection from specimensByCollection, but keep
      // the relevant specimen list, just in case we have a valid default ID to
      // associate with them.
      List<Specimen> defaultCollection = specimensByCollection.remove(DEFAULT_COLLECTION_ID);

      if (defaultCollection == null)
        logger.info("__________ fetchSpecimensByCollection: defaultCollection is null");
      else
        logger.info("__________ fetchSpecimensByCollection: defaultCollection size: " + defaultCollection.size());

      // Replace the DEFAULT_COLLECTION_ID key in specimensByCollection by a sensible collection ID,
      // assuming, of course, that there were any specemins caregorized by DEFAULT_COLLECTION_ID.
      if (defaultCollection != null && defaultCollection.size() != 0 && defaultBbmriEricCollectionId != null) {
        logger.info("__________ fetchSpecimensByCollection: Replace the DEFAULT_COLLECTION_ID key");

        specimensByCollection.put(defaultBbmriEricCollectionId.toString(), defaultCollection);
      }

      logger.info("__________ fetchSpecimensByCollection: specimensByCollection size: " + specimensByCollection.size());

      return specimensByCollection;
    } catch (Exception e) {
      OperationOutcome outcome = new OperationOutcome();
      outcome.addIssue().setSeverity(OperationOutcome.IssueSeverity.ERROR).setDiagnostics(Util.traceFromException(e));
      return null;
    }
  }

  /**
   * Retrieves all Specimens from the FHIR server and organizes them into a Map based on their Collection ID.
   *
   * @return A Map where keys are Collection IDs and values are Lists of Specimens associated with each Collection ID.
   * @throws FhirClientConnectionException If there is an issue connecting to the FHIR server.
   */
  private Map<String, List<Specimen>> getAllSpecimensAsMap() {
    logger.info("__________ getAllSpecimensAsMap: entered");

    Map<String, List<Specimen>> result = new HashMap<String, List<Specimen>>();

    // Use ITransactionTyped instead of returnBundle(Bundle.class)
    IQuery<IBaseBundle> bundleTransaction = fhirClient.search().forResource(Specimen.class);
    Bundle bundle = (Bundle) bundleTransaction.execute();

    logger.info("__________ getAllSpecimensAsMap: gather specimens");

    // Keep looping until the store has no more specimens.
    // This gets around the page size limit of 50 that is imposed by the current implementation of Blaze.
    do {
        // Add entries to the result map
        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            Specimen specimen = (Specimen) entry.getResource();
            String collectionId = extractCollectionIdFromSpecimen(specimen);
            if (!result.containsKey(collectionId))
                result.put(collectionId, new ArrayList<>());
            result.get(collectionId).add(specimen);
        }

        logger.info("__________ getAllSpecimensAsMap: Added " + bundle.getEntry().size() + " entries to result, result size: " + result.size());

        // Check if there are more pages
        if (bundle.getLink(Bundle.LINK_NEXT) != null)
            // Use ITransactionTyped to load the next page
            bundle = fhirClient.loadPage().next(bundle).execute();
        else
            bundle = null;
    } while (bundle != null);

    logger.info("__________ getAllSpecimensAsMap: done");

    return result;
  }

  /**
   * Fetches Patient resources from the FHIR server and groups them by their collection ID.
   * Starts with the available specimens and uses Patient references to find the patients.
   * Note that this approach means that Patients with no specimens will not be included.
   *
   * @param specimensByCollection
   * @return
   */
  private Map<String,List<Patient>> fetchPatientsByCollection(Map<String,List<Specimen>> specimensByCollection) {
    // This method is slow, so use cached value if available.
    if (patientsByCollection != null)
      return patientsByCollection;

    patientsByCollection = specimensByCollection.entrySet().stream()
              .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), extractPatientListFromSpecimenList(entry.getValue())))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)) ;

    return patientsByCollection;
  }

  /**
   * Distingushing function used to ensure that Patient objects do not get duplicated.
   * Takes a function as argument and uses the return value of this function when
   * making comparsons.
   *
   * @param keyExtractor
   * @return
   * @param <T>
   */
  public static <T> Predicate<T> distinctBy(Function<? super T, ?> keyExtractor) {
    Set<Object> seen = new HashSet<>();
    return t -> seen.add(keyExtractor.apply(t));
  }

  /**
   * Given a list of Specimen resources, returns a list of Patient resources derived from
   * the subject references in the specimens.
   *
   * @param specimens
   * @return
   */
  private List<Patient> extractPatientListFromSpecimenList(List<Specimen> specimens) {
    List<Patient> patients = specimens.stream()
            // filter out specimens without a patient reference
            .filter(specimen -> specimen.hasSubject())
            // Find a Patient object corresponding to the specimen's subject
            .map(specimen -> extractPatientFromSpecimen(specimen))
            // Avoid duplicating the same patient
            .filter(distinctBy(Patient::getId))
            // collect the patients into a new list
            .collect(Collectors.toList());

    return patients;
  }

  /**
   * Extracts a Patient resource from a Specimen resource.
   * 
   * @param specimen a Specimen resource that contains a reference to a Patient resource
   * @return a Patient resource that matches the reference in the Specimen resource, or null if not found
   * @throws ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException if the FHIR server cannot find the Patient resource
   */
  public Patient extractPatientFromSpecimen(Specimen specimen) {
    return fhirClient
              .read()
              .resource(Patient.class)
              .withId(specimen.getSubject()
                      .getReference()
                      .replaceFirst("Patient/", ""))
              .execute();
  }

  Boolean conditionsPresentInFhirStore = null;

  /**
   * Extracts a list of condition codes from a Patient resource using a FHIR client.
   * The condition codes are based on the system "http://hl7.org/fhir/sid/icd-10".
   * @param patient a Patient resource that has an ID element
   * @return a list of strings that represent the condition codes of the patient, or an empty list if none are found
   */
  public List<String> extractConditionCodesFromPatient(Patient patient) {
    List<String> conditionCodes = new ArrayList<String>();
    try {
      // If there are no conditions in the FHIR store, then we don't
      // need to bother checking the patient for conditions.
      if (conditionsPresentInFhirStore == null) {
        int conditionCount = fhirClient
          .search()
          .forResource(Condition.class)
          .returnBundle(Bundle.class)
          .summaryMode(SummaryEnum.COUNT)
          .execute()
          .getTotal();
        conditionsPresentInFhirStore = conditionCount > 0;
      }
      if (!conditionsPresentInFhirStore)
        return conditionCodes;

      // Search for Condition resources by patient ID
      Bundle bundle = fhirClient
        .search()
        .forResource(Condition.class)
        .where(Condition.SUBJECT.hasId(patient.getIdElement()))
        .returnBundle(Bundle.class)
        .execute();
      if (!bundle.hasEntry())
        return conditionCodes;

      // Create a stream of Condition resources from the Bundle
      Stream<Condition> conditionStream = bundle.getEntry().stream()
        // Map the bundle entries to Condition resources
        .map(entry -> (Condition) entry.getResource());

      // Loop over the Condition resources
      conditionStream.forEach(condition -> {
        // Get the code element of the Condition resource
        CodeableConcept code = condition.getCode();
        // Get the list of coding elements from the code element
        List<Coding> codings = code.getCoding();
        // Loop over the coding elements
        for (Coding coding : codings) {
          // Check if the coding element has the system "http://hl7.org/fhir/sid/icd-10"
          if (coding.getSystem().equals("http://hl7.org/fhir/sid/icd-10")) {
            // Get the code value and the display value from the coding element
            String codeValue = coding.getCode();
            //String displayValue = coding.getDisplay();
            conditionCodes.add(codeValue);
          }
        }
      });
    } catch (ResourceNotFoundException e) {
      logger.error("extractConditionCodesFromPatient: could not find Condition, stack trace:\n" + Util.traceFromException(e));
    }

    return conditionCodes;
  }

  /**
     * Determines a plausible collection id for specimens that do not have a collection id.
     * If no default collection id is provided, tries to find one from the available collections.
     * If no valid collection id can be found, returns null.
     *
     * @param defaultBbmriEricCollectionId the default collection id supplied by the site
     * @param specimensByCollection a map of collection id to list of specimens
     * @return the default collection id, or null if none is found
     */
  private BbmriEricId determineDefaultCollectionId(BbmriEricId defaultBbmriEricCollectionId, Map<String,List<Specimen>> specimensByCollection) {
    logger.info("determineDefaultCollectionId: entered");
    logger.info("determineDefaultCollectionId: initial defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);

    // If no default collection ID has been provided by the site, see if we can find a plausible value.
    // If there are no specimens with a collection ID, but there is a single collection,
    // then we can reasonably assume that the collection can be used as a default.
    if (defaultBbmriEricCollectionId == null && specimensByCollection.size() == 1 && specimensByCollection.containsKey(DEFAULT_COLLECTION_ID)) {
      logger.info("determineDefaultCollectionId: first conditional succeeded");

      List<Organization> collections = listAllCollections();
      if (collections != null) {
        logger.info("determineDefaultCollectionId: second conditional succeeded");

        if (collections.size() == 1) {
          logger.info("determineDefaultCollectionId: third conditional succeeded");

          String defaultCollectionId = extractValidDirectoryIdentifierFromCollection(collections.get(0));

          logger.info("determineDefaultCollectionId: defaultCollectionId: " + defaultCollectionId);

          defaultBbmriEricCollectionId = BbmriEricId
          .valueOf(defaultCollectionId)
          .orElse(null);
        }
      }
    }

    logger.info("determineDefaultCollectionId: final defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);

    return defaultBbmriEricCollectionId;
  }

  /**
   * Extracts the collection id from a Specimen object that has a Custodian extension.
   * The collection id is either a valid Directory collection id or the default value DEFAULT_COLLECTION_ID.
   * If the Specimen object does not have a Custodian extension, the default value is returned.
   * If the Specimen object has a Custodian extension, the collection id is obtained from the Organization reference in the extension.
   * The collection id is then validated against the list of all collections returned by the listAllCollections() method.
   * If the collection id is not found or invalid, the default value is returned.
   *
   * @param specimen the Specimen object to extract the collection id from
   * @return the collection id as a String
   */
  private String extractCollectionIdFromSpecimen(Specimen specimen) {
    // We expect the specimen to have an extension for a collection, where we would find a collection
    // ID. If we can't find that, then return the default collection ID.
    if (!specimen.hasExtension())
      return DEFAULT_COLLECTION_ID;
    Extension extension = specimen.getExtensionByUrl("https://fhir.bbmri.de/StructureDefinition/Custodian");
    if (extension == null)
      return DEFAULT_COLLECTION_ID;

    // Pull the locally-used collection ID from the specimen extension.
    String reference = ((Reference) extension.getValue()).getReference();
    String localCollectionId = reference.replaceFirst("Organization/", "");

    String collectionId = extractValidDirectoryIdentifierFromCollection(
            fhirClient
                    .read()
                    .resource(Organization.class)
                    .withId(localCollectionId)
                    .execute());

    return collectionId;
  }

  /**
   * Gets the Directory collection ID from the identifier of the supplied collection.
   * Returns DEFAULT_COLLECTION_ID if there is no identifier or if the identifier's value is not a valid
   * Directory ID.
   *
   * @param collection
   * @return
   */
  private String extractValidDirectoryIdentifierFromCollection(Organization collection) {
    String collectionId = DEFAULT_COLLECTION_ID;
    List<Identifier> collectionIdentifiers = collection.getIdentifier();
    for (Identifier collectionIdentifier : collectionIdentifiers) {
      String collectionIdentifierString = collectionIdentifier.getValue();
      if (BbmriEricId.isValidDirectoryCollectionIdentifier(collectionIdentifierString)) {
        collectionId = collectionIdentifierString;
        break;
      }
    }

    return collectionId;
  }

  public List<String> extractDiagnosesFromSpecimen(Specimen specimen) {
    return extractExtensionElementValuesFromSpecimen(specimen, SAMPLE_DIAGNOSIS_URI);
  }

  /**
   * Extracts the code value of each extension element with a given URL from a Specimen resource.
   * The extension element must have a value of type CodeableConcept.
   * @param specimen a Specimen resource that may have extension elements
   * @param url the URL of the extension element to extract
   * @return a list of strings that contains the code value of each extension element with the given URL, or an empty list if none is found
   */
  private List<String> extractExtensionElementValuesFromSpecimen(Specimen specimen, String url) {
    List<Extension> extensions = specimen.getExtensionsByUrl(url);
    List<String> elementValues = new ArrayList<String>();

    // Check if the list is not empty
    for (Extension extension: extensions) {
      // Get the value of the extension as a Quantity object
      CodeableConcept codeableConcept = (CodeableConcept) extension.getValue();

      elementValues.add(codeableConcept.getCoding().get(0).getCode());
    }

    return elementValues;
  }

  /**
   * Extracts the code values of the extension elements with a given URL from a list of Specimen resources.
   * The extension elements must have a value of type CodeableConcept.
   * @param specimens a list of Specimen resources that may have extension elements
   * @param url the URL of the extension elements to extract
   * @return a list of strings that contains the distinct code values of the extension elements with the given URL, or an empty list if none are found
   */
  private List<String> extractExtensionElementValuesFromSpecimens(List<Specimen> specimens, String url) {
    return specimens.stream()
            // Flatten each specimen's extension elements into a single stream
            .flatMap(s -> extractExtensionElementValuesFromSpecimen(s, url).stream())
            // Collect the results into a non-duplicating list
            .distinct()
            .collect(Collectors.toList());
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
    Map<String, List<Specimen>> specimensByCollection = fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    if (specimensByCollection == null) {
      logger.warn("fetchFhirCollections: Problem finding specimens");
      return null;
    }
    updateFhirCollectionsWithSpecimenData(fhirCollectionMap, specimensByCollection);

    // Group patients according to collection, extract aggregated information
    // from each group, and put this information into FhirCollection objects.
    Map<String, List<Patient>> patientsByCollection = fetchPatientsByCollection(specimensByCollection);
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
      fhirCollection.setStorageTemperatures(extractExtensionElementValuesFromSpecimens(specimenList, STORAGE_TEMPERATURE_URI));
      fhirCollection.setDiagnosisAvailable(extractExtensionElementValuesFromSpecimens(specimenList, SAMPLE_DIAGNOSIS_URI));
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
    Map<String, List<Specimen>> specimensByCollection = fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    if (specimensByCollection == null) {
      logger.warn("fetchDiagnoses: Problem finding specimens");
      return null;
    }

    // Get diagnoses from Specimen extensions
    List<String> diagnoses = specimensByCollection.values().stream()
            .flatMap(List::stream)
            .map(s -> extractDiagnosesFromSpecimen(s))
            .flatMap(List::stream)
            .distinct()
            .collect(Collectors.toList());

    // Get diagnoses from Patients
    Map<String, List<Patient>> patientsByCollection = fetchPatientsByCollection(specimensByCollection);
    List<String> patientDiagnoses = patientsByCollection.values().stream()
            .flatMap(List::stream)
            .map(s -> extractConditionCodesFromPatient(s))
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
