package de.samply.directory_sync_service.fhir;

import static ca.uhn.fhir.rest.api.PreferReturnEnum.OPERATION_OUTCOME;
import static ca.uhn.fhir.rest.api.SummaryEnum.COUNT;
import static java.util.Collections.emptyList;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;

import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.gclient.ICreateTyped;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.gclient.IUpdateExecutable;
import ca.uhn.fhir.rest.gclient.UriClientParam;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.model.BbmriEricId;
import io.vavr.control.Either;

import java.util.AbstractMap;
import java.util.ArrayList;
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
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Measure;
import org.hl7.fhir.r4.model.MeasureReport;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.Specimen;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides convenience methods for selected FHIR operations.
 */
public class FhirApi {

  private static final String BIOBANK_PROFILE_URI = "https://fhir.bbmri.de/StructureDefinition/Biobank";
  private static final String COLLECTION_PROFILE_URI = "https://fhir.bbmri.de/StructureDefinition/Collection";
  private static final String SAMPLE_DIAGNOSIS_URI = "https://fhir.bbmri.de/StructureDefinition/SampleDiagnosis";
  private static final String DEFAULT_COLLECTION_ID = "DEFAULT";

  private static final Logger logger = LoggerFactory.getLogger(FhirApi.class);

  Map<String, List<Specimen>> specimensByCollection = null;
  Map<String, List<Patient>> patientsByCollection = null;
  
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

  private final IGenericClient fhirClient;

  public FhirApi(IGenericClient fhirClient) {
    this.fhirClient = Objects.requireNonNull(fhirClient);
  }

  public OperationOutcome updateResource(IBaseResource theResource) {
    try {
      logger.info("updateResource: run resourceUpdate");
      IUpdateExecutable resourceUpdater = resourceUpdate(theResource);
      logger.info("updateResource: run getOperationOutcome");
      IBaseOperationOutcome outcome = resourceUpdater.execute().getOperationOutcome();
      return (OperationOutcome) outcome;
//      return (OperationOutcome) resourceUpdate(theResource).execute().getOperationOutcome();
    } catch (Exception e) {
      logger.info("updateResource: exception: " + Util.traceFromException(e));
      OperationOutcome outcome = new OperationOutcome();
      outcome.addIssue().setSeverity(ERROR).setDiagnostics(e.getMessage());
      return outcome;
    }
  }

  private IUpdateExecutable resourceUpdate(IBaseResource theResource) {
    return fhirClient.update().resource(theResource).prefer(OPERATION_OUTCOME);
  }

  public Either<String, Void> createResource(IBaseResource resource) {
    logger.info("createResource: entered");
    try {
      MethodOutcome outcome = resourceCreate(resource).execute();
      if (outcome.getCreated()) {
        return Either.right(null);
      } else {
        return Either.left("error while creating a resource");
      }
    } catch (Exception e) {
      return Either.left(e.getMessage());
    }
  }

  private ICreateTyped resourceCreate(IBaseResource resource) {
    logger.info("resourceCreate: entered");
    return fhirClient.create().resource(resource).prefer(OPERATION_OUTCOME);
  }

  /**
   * Lists all Organization resources with the biobank profile.
   *
   * @return either a list of {@link Organization} resources or an {@link OperationOutcome} on *
   * errors
   */
  public Either<OperationOutcome, List<Organization>> listAllBiobanks() {
    return listAllOrganizations(BIOBANK_PROFILE_URI)
        .map(bundle -> extractOrganizations(bundle, BIOBANK_PROFILE_URI));
  }

  private Either<OperationOutcome, Bundle> listAllOrganizations(String profileUri) {
    try {
      return Either.right((Bundle) fhirClient.search().forResource(Organization.class)
          .withProfile(profileUri).execute());
    } catch (Exception e) {
      OperationOutcome outcome = new OperationOutcome();
      outcome.addIssue().setSeverity(ERROR)
          .setDiagnostics(e.getMessage());
      return Either.left(outcome);
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
   * Lists all Organization resources with the collection profile.
   *
   * @return either a list of {@link Organization} resources or an {@link OperationOutcome} on
   * errors
   */
  public Either<OperationOutcome, List<Organization>> listAllCollections() {
    return listAllOrganizations(COLLECTION_PROFILE_URI)
        .map(bundle -> extractOrganizations(bundle, COLLECTION_PROFILE_URI));
  }

  /**
   * Checks whether a resource of {@code type} and canonical {@code uri} exists.
   *
   * @param type the resource type
   * @param uri  the canonical URI
   * @return a Right with {@code true} if the resource exists or a Left in case of an error
   */
  public Either<String, Boolean> resourceExists(Class<? extends IBaseResource> type, String uri) {
    logger.info("Check whether " + type + " with canonical URI " + uri + " exists.");
    try {
      return Either.right(resourceQuery(type, uri).execute().getTotal() == 1);
    } catch (Exception e) {
      logger.info("Problem running check");
      return Either.left(e.getMessage());
    }
  }

  private IQuery<Bundle> resourceQuery(Class<? extends IBaseResource> type, String uri) {
    logger.info("resourceQuery: uri: " + uri);
    return fhirClient.search().forResource(type)
        .where(new UriClientParam("url").matches().value(uri))
        .summaryMode(COUNT)
        .returnBundle(Bundle.class);
  }

  /**
   * Executes the Measure with the given canonical URL.
   *
   * @param url canonical URL of the Measure to be executed
   * @return MeasureReport or OperationOutcome in case of error.
   */
  Either<OperationOutcome, MeasureReport> evaluateMeasure(String url) {
    // Create the input parameters to pass to the server
    Parameters inParams = new Parameters();
    inParams.addParameter().setName("periodStart").setValue(new DateType("1900"));
    inParams.addParameter().setName("periodEnd").setValue(new DateType("2100"));
    inParams.addParameter().setName("measure").setValue(new StringType(url));

    try {
      Parameters outParams = fhirClient
          .operation()
          .onType(Measure.class)
          .named("$evaluate-measure")
          .withParameters(inParams)
          .useHttpGet()
          .execute();

      return Either.right((MeasureReport) outParams.getParameter().get(0).getResource());
    } catch (Exception e) {
      OperationOutcome outcome = new OperationOutcome();
      outcome.addIssue().setSeverity(ERROR)
          .setDiagnostics(e.getMessage());
      return Either.left(outcome);
    }
  }

  /**
   * Loads the Organization resource for each of the FHIR ids given.
   *
   * @param ids logical ids of the Organization resources to load
   * @return List of Organization Resources or OperationOutcome in case of failure.
   */
  Either<OperationOutcome, List<Organization>> fetchCollections(Set<String> ids) {
    if (ids.isEmpty()) {
      return Either.right(emptyList());
    }
    try {
      Bundle response = (Bundle) fhirClient.search().forResource(Organization.class)
          .where(Organization.RES_ID.exactly().codes(ids)).execute();

      return Either.right(response.getEntry().stream()
          .filter(e -> ResourceType.Organization == e.getResource().getResourceType())
          .map(e -> (Organization) e.getResource())
          .collect(Collectors.toList()));
    } catch (Exception e) {
      OperationOutcome outcome = new OperationOutcome();
      outcome.addIssue().setSeverity(ERROR)
          .setDiagnostics(e.getMessage());
      return Either.left(outcome);
    }
  }

  /**
   * Fetches specimens from the FHIR server and groups them by their collection id.
   * If no default collection id is provided, tries to find one from the available collections.
   * If the default collection id is invalid or not found, removes the specimens without a collection id from the result.
   *
   * @param defaultBbmriEricCollectionId the default collection id supplied by the site, to be used for specimens without a collection id. May be null
   * @return an Either object containing either a map of collection id to list of specimens, or an OperationOutcome object in case of an error
   */
  public Either<OperationOutcome, Map<String,List<Specimen>>> fetchSpecimensByCollection(BbmriEricId defaultBbmriEricCollectionId) {
    logger.info("__________ fetchSpecimensByCollection: entered");

    // This method is slow, so use cached value if available.
    if (specimensByCollection != null)
      return Either.right(specimensByCollection);

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

      return Either.right(specimensByCollection);
    } catch (Exception e) {
      OperationOutcome outcome = new OperationOutcome();
      outcome.addIssue().setSeverity(OperationOutcome.IssueSeverity.ERROR).setDiagnostics(Util.traceFromException(e));
      return Either.left(outcome);
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
   * @param defaultCollectionId
   * @return
   */
  Either<OperationOutcome, Map<String,List<Patient>>> fetchPatientsByCollection(Map<String,List<Specimen>> specimensByCollection) {
    // This method is slow, so use cached value if available.
    if (patientsByCollection != null)
      return Either.right(patientsByCollection);

    patientsByCollection = specimensByCollection.entrySet().stream()
              .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), extractPatientListFromSpecimenList(entry.getValue())))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)) ;

    return Either.right(patientsByCollection);
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

      Either<OperationOutcome, List<Organization>> collectionsOutcome = listAllCollections();
      if (collectionsOutcome.isRight()) {
        logger.info("determineDefaultCollectionId: second conditional succeeded");

        List<Organization> collections = collectionsOutcome.get();
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
      if (isValidDirectoryCollectionIdentifier(collectionIdentifierString)) {
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
  public List<String> extractExtensionElementValuesFromSpecimen(Specimen specimen, String url) {
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
  public List<String> extractExtensionElementValuesFromSpecimens(List<Specimen> specimens, String url) {
    return specimens.stream()
            // Flatten each specimen's extension elements into a single stream
            .flatMap(s -> extractExtensionElementValuesFromSpecimen(s, url).stream())
            // Collect the results into a non-duplicating list
            .distinct()
            .collect(Collectors.toList());
  }

  private boolean isValidDirectoryCollectionIdentifier(String collectionIdentifier) {
    if (collectionIdentifier == null)
      return false;
    String[] parts = collectionIdentifier.split(":");
    if (parts.length != 5)
      return false;
    if ( ! parts[1].equals("ID"))
      return false;
    if ( ! parts[3].equals("collection"))
      return false;
    return true;
  }
}
