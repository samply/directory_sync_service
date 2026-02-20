package de.samply.directory_sync_service.fhir;

import de.samply.directory_sync_service.model.Collections;

import static ca.uhn.fhir.rest.api.PreferReturnEnum.OPERATION_OUTCOME;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.LenientErrorHandler;
import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.model.Collection;
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
import org.hl7.fhir.r4.model.IdType;
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
  private final FhirContext ctx;
  String fhirStoreUrl;

  public FhirApi(String fhirStoreUrl) {
    ctx = FhirContext.forR4();
    // Allow invalid values in resource attributes
    ctx.setParserErrorHandler(
            new LenientErrorHandler()
                    .setErrorOnInvalidValue(false)  // <-- key line
    );

    this.fhirStoreUrl = fhirStoreUrl;
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

  public String resourceToJsonString(IBaseResource resource) {
    return ctx.newJsonParser().setPrettyPrint(true).encodeResourceToString(resource);
  }

  /**
   * Updates the provided resource on the FHIR server.
   * <p>
   * This method updates the specified resource on the FHIR server and returns the operation outcome.
   *
   * @param resource The resource to be updated on the FHIR server.
   * @return The operation outcome of the update operation.
   *         If the update is successful, the operation outcome will contain information about the update.
   *         If an exception occurs during the update process, an error operation outcome will be returned.
   */
  public OperationOutcome updateResource(IBaseResource resource) {
    logger.debug("updateResource: entered");

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
      logger.debug("updateResource: return outcome: " +outcome);
      return (OperationOutcome) outcome;
    } catch (Exception e) {
      logger.warn("updateResource: exception: " + Util.traceFromException(e));
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

    // Extract the organizations from the Bundle and return them
    return extractOrganizations(organizationBundle, BIOBANK_PROFILE_URI);
  }

  /**
   * Retrieves a list of all known collections.
   *
   * @return A list of {@link Organization} objects representing biobank collections,
   *         or {@code null} if an error occurs.
   */
  private List<Organization> listAllCollections() {
    // List all organizations with the specified biobank profile URI
    Bundle organizationBundle = listAllOrganizations(COLLECTION_PROFILE_URI);

    // Check if the operation was successful
    if (organizationBundle == null) {
      logger.warn("listAllCollections: there was a problem during listAllOrganizations");
      return null;
    }

    // Extract the organizations from the Bundle and return them
    return extractOrganizations(organizationBundle, COLLECTION_PROFILE_URI);
  }

  /**
   * Retrieves a list of all collections that match the given list of collection IDs.
   *
   * @param collectionIds A list of collection IDs.
   * @return A list of {@link Organization} objects representing biobank collections,
   *         or {@code null} if an error occurs.
   */
  public List<Organization> listAllCollections(List<String> collectionIds) {
    List<Organization> collections = listAllCollections();
    if (collections == null) {
      logger.warn("listAllCollections: collections is null");
      return null;
    }

    return collections.stream()
            .filter(c -> c.getIdentifier().stream()
                    .anyMatch(identifier -> collectionIds.contains(identifier.getValue())))
            .collect(Collectors.toList());
  }

    /**
     * Retrieves all organizations that conform to a given FHIR profile URI.
     *
     * <p>This method performs a FHIR search query to find organizations matching the provided profile URI.
     * If an exception occurs during the search, it logs the error and returns {@code null}.
     *
     * @param profileUri The URI of the FHIR profile used to filter the organizations.
     * @return A {@link Bundle} containing the matching organizations, or {@code null} if an error occurs.
     */
  private Bundle listAllOrganizations(String profileUri) {
    try {
      return (Bundle) fhirClient.search().forResource(Organization.class)
              .withProfile(profileUri).execute();
    } catch (Exception e) {
      Util.traceFromException(e);
      logger.warn("listAllOrganizations: exception: " + Util.traceFromException(e));
      return null;
    }
  }

  /**
   * Extracts a list of {@link Organization} resources from a given FHIR {@link Bundle}.
   *
   * <p>This method filters the entries in the bundle, ensuring they are of type {@link Organization}
   * and that they conform to the specified profile URL. The extracted organizations are returned
   * as a list.
   *
   * @param bundle The FHIR {@link Bundle} containing organization resources.
   * @param profileUrl The expected profile URL to filter organizations.
   * @return A list of {@link Organization} objects matching the specified profile.
   */
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
    logger.debug("fetchSpecimensByCollection: entered");

    // This method is slow, so use cached value if available.
    if (specimensByCollection != null)
      return specimensByCollection;

    logger.debug("fetchSpecimensByCollection: get specimens from FHIR store");

    try {
      specimensByCollection = getAllSpecimensAsMap();

      logger.debug("fetchSpecimensByCollection: specimensByCollection size: " + specimensByCollection.size());

      defaultBbmriEricCollectionId = determineDefaultCollectionId(defaultBbmriEricCollectionId, specimensByCollection);

      logger.debug("fetchSpecimensByCollection: defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);

      // Remove specimens without a collection from specimensByCollection, but keep
      // the relevant specimen list, just in case we have a valid default ID to
      // associate with them.
      List<Specimen> defaultCollection = specimensByCollection.remove(DEFAULT_COLLECTION_ID);

      if (defaultCollection == null)
        logger.debug("fetchSpecimensByCollection: defaultCollection is null");
      else
        logger.debug("fetchSpecimensByCollection: defaultCollection size: " + defaultCollection.size());

      // Replace the DEFAULT_COLLECTION_ID key in specimensByCollection by a sensible collection ID,
      // assuming, of course, that there were any specemins caregorized by DEFAULT_COLLECTION_ID.
      if (defaultCollection != null && defaultCollection.size() != 0 && defaultBbmriEricCollectionId != null) {
        logger.debug("fetchSpecimensByCollection: Replace the DEFAULT_COLLECTION_ID key");

        if (specimensByCollection.containsKey(defaultBbmriEricCollectionId.toString()))
          // Add all specimens with DEFAULT_COLLECTION_ID to defaultBbmriEricCollectionId if it exists
          for (Specimen specimen : defaultCollection)
            specimensByCollection.get(defaultBbmriEricCollectionId.toString()).add(specimen);
        else
          // Move all specimens with DEFAULT_COLLECTION_ID to defaultBbmriEricCollectionId
          specimensByCollection.put(defaultBbmriEricCollectionId.toString(), defaultCollection);
      }

      logger.debug("fetchSpecimensByCollection: specimensByCollection size: " + specimensByCollection.size());
      if (specimensByCollection.size() == 0)
        logger.warn("fetchSpecimensByCollection: no collections found, maybe you need to upload some data to your FHIR store?");

      return specimensByCollection;
    } catch (Exception e) {
      logger.warn("fetchSpecimensByCollection: exception" + Util.traceFromException(e));
      return null;
    }
  }

  public int calculateTotalSpecimenCount(BbmriEricId defaultBbmriEricCollectionId) {
    Map<String, List<Specimen>> specimensByCollection = fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    int totalSpecimenCount = 0;
    for (List<Specimen> specimenList : specimensByCollection.values())
      totalSpecimenCount += specimenList.size();

    return totalSpecimenCount;
  }

  /**
   * Retrieves all sample materials over all collections.
   *
   * @param defaultBbmriEricCollectionId the default BBMRI-ERIC collection ID. May be null.
   * @return a Map representing sample materials as key-value pairs
   */
  public Map<String,String> getSampleMaterials(BbmriEricId defaultBbmriEricCollectionId) {
    Map<String, List<Specimen>> specimensByCollection = fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    Map<String, String> fhirSampleMaterials = new HashMap<String, String>();
    for (List<Specimen> specimenList : specimensByCollection.values()) {
      List<String> materials = extractMaterialsFromSpecimenList(specimenList);
      if (materials == null)
        continue;
      for (String material : materials)
        if (!fhirSampleMaterials.containsKey(material))
          fhirSampleMaterials.put(material, material);
    }

    return fhirSampleMaterials;
  }

  /**
   * Retrieves all Specimens from the FHIR server and organizes them into a Map based on their Collection ID.
   *
   * @return A Map where keys are Collection IDs and values are Lists of Specimens associated with each Collection ID.
   */
  private Map<String, List<Specimen>> getAllSpecimensAsMap() {
    logger.info("getAllSpecimensAsMap: entered");

    Map<String, List<Specimen>> result = new HashMap<String, List<Specimen>>();

    try {
      // Use ITransactionTyped instead of returnBundle(Bundle.class)
      IQuery<IBaseBundle> bundleTransaction = fhirClient.search().forResource(Specimen.class);
      Bundle bundle = (Bundle) bundleTransaction.execute();

      logger.info("getAllSpecimensAsMap: gather specimens");

      // Keep looping until the store has no more specimens.
      // This gets around the page size limit of 50 that is imposed by the current implementation of Blaze.
      int pageNum = 0;
      do {
        if (pageNum % 10 == 0)
          logger.info("getAllSpecimensAsMap: pageNum: " + pageNum);

        // Add entries to the result map
        for (BundleEntryComponent entry : bundle.getEntry()) {
            Specimen specimen = (Specimen) entry.getResource();
            String collectionId = extractCollectionIdFromSpecimen(specimen);
            if (!result.containsKey(collectionId))
                result.put(collectionId, new ArrayList<>());
            result.get(collectionId).add(specimen);
        }

        // Check if there are more pages
        if (bundle.getLink(Bundle.LINK_NEXT) != null)
            // Use ITransactionTyped to load the next page
            bundle = fhirClient.loadPage().next(bundle).execute();
        else
            bundle = null;

        pageNum++;
      } while (bundle != null);
    } catch (Exception e) {
      logger.warn("getAllSpecimensAsMap: could not retrieve data from FHIR store");
      logger.warn("getAllSpecimensAsMap: fhirStoreUrl: " + fhirStoreUrl);
      logger.warn("getAllSpecimensAsMap: httpClient: " + fhirClient.getHttpClient().toString());
      logger.warn("getAllSpecimensAsMap: exception" + Util.traceFromException(e));
    }

    logger.info("getAllSpecimensAsMap: done, result.size(): " + result.size());

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
    return specimens.stream()
            // filter out specimens without a patient reference
            .filter(specimen -> specimen.hasSubject())
            // Find a Patient object corresponding to the specimen's subject
            .map(specimen -> extractPatientFromSpecimen(specimen))
            // Skip null results from the mapping step
            .filter(Objects::nonNull)
            // Avoid duplicating the same patient
            .filter(distinctBy(Patient::getId))
            // collect the patients into a new list
            .collect(Collectors.toList());
  }

  /**
   * Finds all {@link Specimen} resources that reference the given {@link Patient}.
   *
   * <p>This method performs a FHIR search query to retrieve all Specimen resources
   * where the {@code subject} field contains a reference to the specified Patient.
   * It handles paginated results by iterating through multiple response bundles.
   *
   * @param patient The {@link Patient} resource whose related Specimen resources should be retrieved.
   *               Must be a valid Patient object with an assigned FHIR ID.
   * @return A {@link List} of {@link Specimen} objects that reference the given Patient.
   *         If no Specimen resources are found, an empty list is returned.
   */
  public List<Specimen> findAllSpecimensWithReferencesToPatient(Patient patient) {
    List<Specimen> result = new ArrayList<>();
    try {
      // Form the proper reference value. Using getIdElement() is usually safer.
      String patientReference = "Patient/" + patient.getIdElement().getIdPart();

      // Initiate the search
      Bundle bundle = fhirClient
              .search()
              .forResource(Specimen.class)
              // Use a search method that matches the patient reference properly.
              .where(Specimen.SUBJECT.hasId(patientReference))
              .returnBundle(Bundle.class)
              .execute();

      // Process pages
      while (bundle != null) {
        if (bundle.hasEntry()) {
          for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            if (entry.getResource() instanceof Specimen) {
              result.add((Specimen) entry.getResource());
            }
          }
        }
        // Check if there's a next page link and, if so, load it.
        bundle = getNextPage(bundle);
      }
    } catch (Exception e) {
      System.err.println("Error during FHIR search: " + Util.traceFromException(e));
    }
    return result;
  }

  /**
   * Helper method to get the next page of a bundle, if it exists.
   *
   * @param bundle
   * @return
   */
  private Bundle getNextPage(Bundle bundle) {
    // TODO: This is a simplistic implementation; it might be better to use fhirClient.loadPage() or follow the next link yourself.
    Bundle nextBundle = null;
    Bundle.BundleLinkComponent nextLink = bundle.getLink("next");
    if (nextLink != null && nextLink.getUrl() != null) {
      nextBundle = fhirClient.loadPage().next(bundle).execute();
    }
    return nextBundle;
  }

  /**
   * Extracts a Patient resource from a Specimen resource.
   *
   * This works because every Specimen resource contains a reference to a Patient resource.
   * 
   * @param specimen a Specimen resource that contains a reference to a Patient resource
   * @return a Patient resource that matches the reference in the Specimen resource, or null if not found
   */
  public Patient extractPatientFromSpecimen(Specimen specimen) {
    if (specimen == null) {
      logger.warn("extractPatientFromSpecimen: specimen is null");
      return null;
    }
    Patient patient =  null;
    try {
      // Make sure that there is a patient (subject) reference and that it is valid.
      Reference specimenSubject = specimen.getSubject();
      if (specimenSubject == null) {
        logger.warn("extractPatientFromSpecimen: specimen subject is null for specimen ID: " + specimen.getIdElement().getIdPart());
        return null;
      }
      String specimenSubjectReference = specimenSubject.getReference();
      if (specimenSubjectReference == null) {
        logger.warn("extractPatientFromSpecimen: specimen subject reference is null for specimen ID: " + specimen.getIdElement().getIdPart());
        return null;
      }
      if (specimenSubjectReference.isEmpty()) {
        logger.warn("extractPatientFromSpecimen: specimen subject reference is empty for specimen ID: " + specimen.getIdElement().getIdPart());
        return null;
      }
      if (!specimenSubjectReference.startsWith("Patient/"))
        logger.warn("extractPatientFromSpecimen: specimen id does not start with 'Patient/' for specimen ID: " + specimen.getIdElement().getIdPart());
      else {
        specimenSubjectReference = specimenSubjectReference.replaceFirst("Patient/", "");
        if (specimenSubjectReference.isEmpty()) {
          logger.warn("extractPatientFromSpecimen: after removing 'Patient/', specimen subject reference has become empty for specimen ID: " + specimen.getIdElement().getIdPart());
          return null;
        }
      }
      logger.info("extractPatientFromSpecimen: specimen subject reference: " + specimenSubjectReference);

      // Find the Patient with the ID found from the reference in the Specimen.
      patient =  fhirClient
                .read()
                .resource(Patient.class)
                .withId(specimenSubjectReference)
                .execute();
      logger.info("extractPatientFromSpecimen: done");
    } catch (Exception e) {
      logger.warn("extractPatientFromSpecimen: exception message: " + e.getMessage());
      logger.warn("extractPatientFromSpecimen: exception: " + e);
      logger.warn("extractPatientFromSpecimen: exception trace: " + Util.traceFromException(e));
    }

    return patient;
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
      // Cache the result, so that we only need to do the check once.
      if (conditionsPresentInFhirStore == null) {
        int conditionCount = fhirClient
          .search()
          .forResource(Condition.class)
          .returnBundle(Bundle.class)
          .summaryMode(SummaryEnum.COUNT)
          .execute()
          .getTotal();
        conditionsPresentInFhirStore = conditionCount > 0;
        logger.info("extractConditionCodesFromPatient: total of all conditions in FHIR store, conditionCount: " + conditionCount);
      }
      if (!conditionsPresentInFhirStore)
        return conditionCodes;

      // Build the full reference like "Patient/{id}"
      IdType patientIdElement = patient.getIdElement();
      if (patientIdElement == null) {
        logger.warn("extractConditionCodesFromPatient: patient has no id element, returning empty list.");
        return conditionCodes;
      }
      String idPart = patientIdElement.getIdPart();
      if (idPart == null) {
        logger.warn("extractConditionCodesFromPatient: patient has no id part, returning empty list.");
        return conditionCodes;
      }
      if (idPart.isEmpty()) {
        logger.warn("extractConditionCodesFromPatient: patient has an empty id part, returning empty list.");
        return conditionCodes;
      }
      String patientReference = "Patient/" + idPart;

      // Search for Condition resources by patient reference
      Bundle bundle = fhirClient
              .search()
              .forResource(Condition.class)
              .where(Condition.SUBJECT.hasId(patientReference)) // Full reference here
              .returnBundle(Bundle.class)
              .execute();

      if (!bundle.hasEntry()) {
        logger.warn("extractConditionCodesFromPatient: Condition bundle has no entries for patientReference " + patientReference + ", returning empty list.");
        return conditionCodes;
      }

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
      logger.warn("extractConditionCodesFromPatient: could not find Condition, stack trace:\n" + Util.traceFromException(e));
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
    logger.debug("determineDefaultCollectionId: entered");
    logger.debug("determineDefaultCollectionId: initial defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);

    // If no default collection ID has been provided by the site, see if we can find a plausible value.
    // If there are no specimens with a collection ID, but there is a single collection,
    // then we can reasonably assume that the collection can be used as a default.
    if (defaultBbmriEricCollectionId == null && specimensByCollection.size() == 1 && specimensByCollection.containsKey(DEFAULT_COLLECTION_ID)) {
      logger.debug("determineDefaultCollectionId: first conditional succeeded");

      List<Organization> collections = listAllCollections();
      if (collections != null) {
        logger.debug("determineDefaultCollectionId: second conditional succeeded");

        if (collections.size() == 1) {
          logger.debug("determineDefaultCollectionId: third conditional succeeded");

          String defaultCollectionId = extractValidDirectoryIdentifierFromCollection(collections.get(0));

          logger.debug("determineDefaultCollectionId: defaultCollectionId: " + defaultCollectionId);

          defaultBbmriEricCollectionId = BbmriEricId
          .valueOf(defaultCollectionId)
          .orElse(null);
        }
      }
    }

    logger.debug("determineDefaultCollectionId: final defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);

    return defaultBbmriEricCollectionId;
  }

  /**
   * Extracts the collection id from a reference to a collection ID obtained from an extension to Specimen.
   *
   * @param reference
   * @return The collection id.
   */
  private String extractCollectionIdFromReference(String reference) {
    if (reference == null) return null;

    String collectionId = reference;
    if (reference.matches("^http[^/:]*://.*/.[^/]*$")) {
      logger.info("extractCollectionIdFromReference: reference is a URL (" + reference + "), using last part of URL as collection ID");
      // The reference has been written like a URL. Assume that the collection ID
      // is the last part of the URL
      String[] collectionIdParts = reference.split("/");
      collectionId = collectionIdParts[collectionIdParts.length - 1];
      if (collectionId.isEmpty() && collectionIdParts.length > 1)
        // The URL ended with a slash, so the collection id is the second to last part
        collectionId = collectionIdParts[collectionIdParts.length - 2];
    } else if (reference.startsWith("Organization/")) {
      // This is the expected case: the reference starts with "Organization/".
      collectionId = reference.replaceFirst("Organization/", "");
    }

    return collectionId;
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

    String directoryIdentifier = DEFAULT_COLLECTION_ID; // return value
    // Pull the locally-used collection ID from the specimen extension.
    String reference = ((Reference) extension.getValue()).getReference();
    String localCollectionId = extractCollectionIdFromReference(reference);

    logger.debug("extractCollectionIdFromSpecimen: localCollectionId: " + localCollectionId);

    try {
      Organization collection = fhirClient
              .read()
              .resource(Organization.class)
              .withId(localCollectionId)
              .execute();
      directoryIdentifier = extractValidDirectoryIdentifierFromCollection(collection);
    } catch (Exception e) {
      logger.warn("extractCollectionIdFromSpecimen: could not get collection with reference " + reference + " and with localCollectionId " + localCollectionId + " from FHIR store, stack trace:\n" + Util.traceFromException(e));
      List<String> orgPairs = listOrganizations();
      if (orgPairs.size() == 0) {
        logger.warn("extractCollectionIdFromSpecimen: no Organizations found in FHIR store");
      } else {
        logger.warn("extractCollectionIdFromSpecimen: known Organizations in FHIR store:");
        orgPairs.forEach(pair -> logger.warn("       {}", pair));
      }
      logger.warn("extractCollectionIdFromSpecimen: returning default collection ID: " + DEFAULT_COLLECTION_ID);
    }

    return directoryIdentifier;
  }

  /**
   * Fetches and returns a list of Organization ID/Name pairs as strings.
   */
  private List<String> listOrganizations() {
    List<String> results = new ArrayList<>();

    try {
      Bundle bundle = fhirClient
              .search()
              .forResource(Organization.class)
              .returnBundle(Bundle.class)
              .count(50) // page size
              .execute();

      while (bundle != null && !bundle.getEntry().isEmpty()) {
        for (BundleEntryComponent entry : bundle.getEntry()) {
          if (entry.getResource() instanceof Organization org) {
            String id = org.getIdElement().getIdPart();
            String name = org.getName() != null ? org.getName() : "<No Name>";
            results.add(String.format("%s (ID: %s)", name, id));
          }
        }

        // Handle pagination if more results exist
        if (bundle.getLink(Bundle.LINK_NEXT) != null) {
          bundle = fhirClient.loadPage().next(bundle).execute();
        } else {
          break;
        }
      }
    } catch (Exception e) {
      logger.warn("listOrganizations: could not get list of Organizations from FHIR store, stack trace:\n" + Util.traceFromException(e));
    }

    return results;
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

  /**
   * Extracts and returns any diagnoses in the extension to the specimen with the
   * URI SAMPLE_DIAGNOSIS_URI.
   *
   * @param specimen
   * @return
   */
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
   * Returns a list of Collection objects, one per collection.
   *
   * @param directoryDefaultCollectionId
   * @return
   */
  public Collections generateCollections(String directoryDefaultCollectionId) {
    BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
            .valueOf(directoryDefaultCollectionId)
            .orElse(null);

    Collections collections = new Collections();

    // Group specimens according to collection, extract aggregated information
    // from each group, and put this information into Collection objects.
    Map<String, List<Specimen>> specimensByCollection = fetchSpecimensByCollection(defaultBbmriEricCollectionId);
    if (specimensByCollection == null) {
      logger.warn("fetchFhirCollections: Problem finding specimens");
      return null;
    }
    updateCollectionsWithSpecimenData(collections, specimensByCollection);

    // Group patients according to collection, extract aggregated information
    // from each group, and put this information into Collection objects.
    Map<String, List<Patient>> patientsByCollection = fetchPatientsByCollection(specimensByCollection);
    if (patientsByCollection == null) {
      logger.warn("fetchFhirCollections: Problem finding patients");
      return null;
    }
    updateCollectionsWithPatientData(collections, patientsByCollection);

    return collections;
  }

  /**
   * Updates a map of {@link Collection} entities with specimen-related data.
   *
   * <p>This method iterates over the provided map of specimen lists, grouped by collection key.
   * For each key, it retrieves or creates a corresponding {@link Collection}, then updates its attributes
   * based on the specimens in the list, such as size, materials, storage temperatures, and diagnosis availability.
   *
   * @param collections A map of existing {@link Collection} entities, keyed by collection ID.
   *                 If a collection does not exist, a new one is created.
   * @param specimensByCollection A map where each key represents a collection ID,
   *                              and the value is a list of {@link Specimen} objects associated with that collection.
   */
  private void updateCollectionsWithSpecimenData(Collections collections, Map<String, List<Specimen>> specimensByCollection) {
    for (String collectionId: specimensByCollection.keySet()) {
      List<Specimen> specimenList = specimensByCollection.get(collectionId);
      Collection collection = collections.getOrDefault(collectionId, new Collection());
      collection.setId(collectionId);
      collection.setSize(specimenList.size());
      collection.setMaterials(extractMaterialsFromSpecimenList(specimenList));
      collection.setStorageTemperatures(extractExtensionElementValuesFromSpecimens(specimenList, STORAGE_TEMPERATURE_URI));
      collection.setDiagnosisAvailable(extractExtensionElementValuesFromSpecimens(specimenList, SAMPLE_DIAGNOSIS_URI));
      collections.addCollection(collectionId, collection);
    }
  }

  /**
   * Updates a map of {@link Collection} entities with patient-related data.
   *
   * <p>This method iterates over the provided map of patient lists, grouped by collection key.
   * For each key, it retrieves or creates a corresponding {@link Collection}, then updates its attributes
   * based on the patients in the list, such as number of donors, sex distribution, age range, and diagnoses.
   *
   * @param collections A map of existing {@link Collection} entities, keyed by collection ID.
   *                 If a collection does not exist, a new one is created.
   * @param patientsByCollection A map where each key represents a collection ID,
   *                              and the value is a list of {@link Patient} objects associated with that collection.
   */
  private void updateCollectionsWithPatientData(Collections collections, Map<String, List<Patient>> patientsByCollection) {
    for (String collectionId: patientsByCollection.keySet()) {
      List<Patient> patientList = patientsByCollection.get(collectionId);
      Collection collection = collections.getOrDefault(collectionId, new Collection());
      collection.setNumberOfDonors(patientList.size());
      collection.setSex(extractSexFromPatientList(patientList));
      collection.setAgeLow(extractAgeLowFromPatientList(patientList));
      collection.setAgeHigh(extractAgeHighFromPatientList(patientList));
      collection.setDiagnosisAvailable(extractDiagnosesFromPatientList(patientList));
      collections.addCollection(collectionId, collection);
    }
  }

  /**
   * Extracts a list of unique diagnosis codes from a given list of patients.
   *
   * <p>This method searches for {@link Condition} resources that reference each patient in the provided list.
   * If a {@link Condition} is found, it extracts the diagnosis codes from the {@link CodeableConcept}
   * and ensures uniqueness using a {@link Set}. The final list of diagnosis codes is returned.
   *
   * @param patientList The list of {@link Patient} resources from which to extract diagnoses.
   * @return A {@link List} of unique diagnosis codes as {@link String} values.
   *         If no diagnoses are found, returns an empty list.
   */
  private List<String> extractDiagnosesFromPatientList(List<Patient> patientList) {
    Set<String> diagnosisSet = new HashSet<>(); // Use Set to ensure uniqueness

    for (Patient patient : patientList) {
      // Construct the reference string for the patient
      String patientReference = "Patient/" + patient.getIdElement().getIdPart();

      // Fetch conditions associated with the patient
      Bundle conditionBundle = fhirClient
              .search()
              .forResource(Condition.class)
              .where(Condition.SUBJECT.hasId(patientReference))
              .returnBundle(Bundle.class)
              .execute();

      // Process conditions
      for (Bundle.BundleEntryComponent entry : conditionBundle.getEntry()) {
        if (entry.getResource() instanceof Condition condition) {

            // Extract diagnosis code
          if (condition.hasCode() && condition.getCode().hasCoding()) {
            for (Coding coding : condition.getCode().getCoding()) {
              if (coding.hasCode()) {
                diagnosisSet.add(coding.getCode());
              }
            }
          }
        }
      }
    }

    return new ArrayList<>(diagnosisSet);
  }

  /**
   * Fetches diagnoses from Specimens and Patients to which collections can be assigned.
   * <p>
   * This method retrieves specimens grouped by collection.
   * <p>
   * It then extracts diagnoses from Specimen extensions and Patient condition codes, eliminating duplicates,
   * and combines the results into a list of unique diagnoses.
   *
   * @param defaultCollectionId The BBMRI ERIC collection ID to fetch specimens and diagnoses.
   * @return a List of unique diagnoses.
   */
  public List<String> fetchDiagnoses(String defaultCollectionId) {
    Map<String, String> correctedDiagnoses = new HashMap<String, String>();
    // Convert string version of collection ID into a BBMRI ERIC ID.
    BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
            .valueOf(defaultCollectionId)
            .orElse(null);

    logger.info("generateDiagnosisCorrections: defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);

    // Get all diagnoses from the FHIR store for specimens with identifiable
    // collections and their associated patients.
    List<String> fhirDiagnoses = fetchDiagnoses(defaultBbmriEricCollectionId);
    if (fhirDiagnoses == null) {
      logger.warn("Problem getting diagnosis information from FHIR store");
      return null;
    }

    logger.info("generateDiagnosisCorrections: fhirDiagnoses.size(): " + fhirDiagnoses.size());

    return fhirDiagnoses;
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
  private List<String> fetchDiagnoses(BbmriEricId defaultBbmriEricCollectionId) {
    logger.debug("fetchDiagnoses: defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);
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

    logger.debug("fetchDiagnoses: number of diagnoses from specimens: " + diagnoses.size());

    // Get diagnoses from Patients
    Map<String, List<Patient>> patientsByCollection = fetchPatientsByCollection(specimensByCollection);
    List<String> patientDiagnoses = patientsByCollection.values().stream()
            .flatMap(List::stream)
            .map(s -> extractConditionCodesFromPatient(s))
            .flatMap(List::stream)
            .distinct()
            .collect(Collectors.toList());

    logger.debug("fetchDiagnoses: patientsByCollection.size(): " + patientsByCollection.size());

    // Combine diagnoses from specimens and patients, ensuring that there
    // are no duplicates.
    diagnoses = Stream.concat(diagnoses.stream(), patientDiagnoses.stream())
            .distinct()
            .collect(Collectors.toList());

    return diagnoses;
  }

  int materialWarnCount = 0;
  /**
   * Extracts unique material codes from a list of specimens.
   *
   * @param specimenList A list of {@code Specimen} objects from which to extract material codes.
   * @return A list of unique material codes (as strings) extracted from the specimens.
   */
  private List<String> extractMaterialsFromSpecimenList(List<Specimen> specimenList) {
    if (specimenList == null) {
      logger.warn("extractMaterialsFromSpecimenList: specimenList is null");
      return null;
    } else
      logger.debug("extractMaterialsFromSpecimenList: specimenList.size: " + specimenList.size());
    Set<String> materialSet = new HashSet<>();
    for (Specimen specimen : specimenList) {
      CodeableConcept codeableConcept = specimen.getType();
      if (codeableConcept != null) {
        if (codeableConcept.getCoding().size() > 0)
          materialSet.add(codeableConcept.getCoding().get(0).getCode());
        else if (codeableConcept.hasText())
          materialSet.add(codeableConcept.getText());
        else if (materialWarnCount++ < 5)
          logger.warn("extractMaterialsFromSpecimenList: no material in type for specimen: " + specimen.getId());
      } else if (materialWarnCount++ < 5)
        logger.warn("extractMaterialsFromSpecimenList: no type for specimen: " + specimen.getId());
    }

    return new ArrayList<>(materialSet);
  }

  /**
   * Extracts a list of unique gender values from a list of {@link Patient} objects.
   *
   * <p>The method filters out patients with a null gender, retrieves the gender as a string,
   * and returns a list of unique gender values.
   *
   * @param patients The list of {@link Patient} objects from which to extract gender information.
   * @return A list of unique gender values as strings.
   */
  private List<String> extractSexFromPatientList(List<Patient> patients) {
    return patients.stream()
            .filter(patient -> Objects.nonNull(patient.getGenderElement())) // Filter out patients with null gender
            .map(patient -> patient.getGenderElement().getValueAsString()) // Map each patient to their gender
            .collect(Collectors.toSet()).stream().collect(Collectors.toList()); // Collect the results into a new list
  }

  /**
   * Determines the lowest age among a list of {@link Patient} objects.
   *
   * <p>The method filters out patients with a null birthdate, computes their ages using {@link #determinePatientAge(Patient)},
   * and returns the minimum age found. If no valid age is found, it returns -1.
   *
   * @param patients The list of {@link Patient} objects.
   * @return The lowest age found, or -1 if no valid ages are present.
   */
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

  /**
   * Determines the highest age among a list of {@link Patient} objects.
   *
   * <p>The method filters out patients with a null birthdate, computes their ages using {@link #determinePatientAge(Patient)},
   * and returns the maximum age found. If no valid age is found, it returns -1.
   *
   * @param patients The list of {@link Patient} objects.
   * @return The highest age found, or -1 if no valid ages are present.
   */
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

  /**
   * Determines the age of a given {@link Patient} based on their birth date.
   *
   * <p>The method calculates the difference between the current date and the patient's birth date in years.
   * If the current date is before the patient's birthday in the current year, the age is adjusted accordingly.
   *
   * Note: if the age is specified in an extension, this method will not see it.
   *
   * @param patient The {@link Patient} object whose age is to be determined.
   * @return The patient's age in years, or {@code null} if the birth date is not available.
   */
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
