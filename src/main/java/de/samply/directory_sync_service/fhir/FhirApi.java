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
  protected static final Logger logger = LoggerFactory.getLogger(FhirApi.class);
  protected static final String MIABIS_BIOBANK_PROFILE_URI = "https://fhir.bbmri-eric.eu/StructureDefinition/miabis-biobank"; // Needed by isMiabisOnFhirProfile
  protected static final String DEFAULT_COLLECTION_ID = "DEFAULT_1010101";
  protected Map<String, List<Specimen>> specimensByCollection = null;
  protected Map<String, List<Patient>> patientsByCollection = null;
  protected final IGenericClient fhirClient;
  protected final FhirContext ctx;
  protected String fhirStoreUrl;

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
   * Determines whether the connected FHIR store uses the MIABIS on FHIR profile.
   *
   * <p>The method searches all {@link Organization} resources in the FHIR store
   * and checks whether at least one resource declares the MIABIS biobank profile
   * URI in its {@code meta.profile} element:
   *
   * <pre>
   * https://fhir.bbmri-eric.eu/StructureDefinition/miabis-biobank
   * </pre>
   *
   * <p>If a matching profile is found, the method immediately returns {@code true}.
   * If no matching profile is found after inspecting all Organization resources,
   * the method returns {@code false}.
   *
   * <p>If the FHIR store contains no Organization resources at all, a warning is
   * logged and the method returns {@code false}.
   *
   * @return {@code true} if the FHIR store contains at least one Organization
   *         resource declaring the MIABIS biobank profile; {@code false} otherwise
   */
  public boolean isMiabisOnFhirProfile() {
    Bundle bundle = fhirClient
            .search()
            .forResource(Organization.class)
            .returnBundle(Bundle.class)
            .execute();

    boolean foundAnyOrganization = false;

    while (bundle != null) {
      for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
        if (!(entry.getResource() instanceof Organization organization)) {
          continue;
        }

        foundAnyOrganization = true;

        boolean hasMiabisProfile = organization.hasMeta()
                && organization.getMeta().hasProfile(MIABIS_BIOBANK_PROFILE_URI);

        if (hasMiabisProfile) {
          return true;
        }
      }

      // Just in case there are so many Organization resources that the FHIR store
      // has to revert to pagination.
      if (bundle.getLink(Bundle.LINK_NEXT) != null) {
        bundle = fhirClient
                .loadPage()
                .next(bundle)
                .execute();
      } else {
        bundle = null;
      }
    }

    if (!foundAnyOrganization) {
      logger.warn("No Organization resources found in the FHIR store.");
    }

    return false;
  }

  protected String getBiobankProfileUri() {
    return null;
  }

  protected String getCollectionProfileUri() {
    return null;
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
    Bundle organizationBundle = listAllOrganizations(getBiobankProfileUri());

    // Check if the operation was successful
    if (organizationBundle == null) {
      logger.warn("listAllBiobanks: there was a problem during listAllOrganizations");
      return null;
    }

    // Extract the organizations from the Bundle and return them
    return extractOrganizations(organizationBundle, getBiobankProfileUri());
  }

  /**
   * Retrieves a list of all known collections.
   *
   * @return A list of {@link Organization} objects representing biobank collections,
   *         or {@code null} if an error occurs.
   */
  protected List<Organization> listAllCollections() {
    // List all organizations with the specified biobank profile URI
    Bundle organizationBundle = listAllOrganizations(getCollectionProfileUri());

    // Check if the operation was successful
    if (organizationBundle == null) {
      logger.warn("listAllCollections: there was a problem during listAllOrganizations");
      return null;
    }

    // Extract the organizations from the Bundle and return them
    return extractOrganizations(organizationBundle, getCollectionProfileUri());
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
  protected Bundle listAllOrganizations(String profileUri) {
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
  protected static List<Organization> extractOrganizations(Bundle bundle, String profileUrl) {
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

      logger.info("fetchSpecimensByCollection: TTTTTTTTTTTTTTTTTTTTTTTTTTT specimensByCollection size: " + specimensByCollection.size());

      defaultBbmriEricCollectionId = determineDefaultCollectionId(defaultBbmriEricCollectionId, specimensByCollection);

      logger.info("fetchSpecimensByCollection: TTTTTTTTTTTTTTTTTTTTTTTTTTT defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);

      // Remove specimens without a collection from specimensByCollection, but keep
      // the relevant specimen list, just in case we have a valid default ID to
      // associate with them.
      List<Specimen> defaultCollection = specimensByCollection.remove(DEFAULT_COLLECTION_ID);

      if (defaultCollection == null)
        logger.info("fetchSpecimensByCollection: defaultCollection is null");
      else
        logger.info("fetchSpecimensByCollection: TTTTTTTTTTTTTTTTTTTTTTTTTTT defaultCollection size: " + defaultCollection.size());

      // Replace the DEFAULT_COLLECTION_ID key in specimensByCollection by a sensible collection ID,
      // assuming, of course, that there were any specemins caregorized by DEFAULT_COLLECTION_ID.
      if (defaultCollection != null && defaultCollection.size() != 0 && defaultBbmriEricCollectionId != null) {
        logger.info("fetchSpecimensByCollection: TTTTTTTTTTTTTTTTTTTTTTTTTTT Replace the DEFAULT_COLLECTION_ID key");

        if (specimensByCollection.containsKey(defaultBbmriEricCollectionId.toString()))
          // Add all specimens with DEFAULT_COLLECTION_ID to defaultBbmriEricCollectionId if it exists
          for (Specimen specimen : defaultCollection)
            specimensByCollection.get(defaultBbmriEricCollectionId.toString()).add(specimen);
        else
          // Move all specimens with DEFAULT_COLLECTION_ID to defaultBbmriEricCollectionId
          specimensByCollection.put(defaultBbmriEricCollectionId.toString(), defaultCollection);
      }

      logger.info("fetchSpecimensByCollection: TTTTTTTTTTTTTTTTTTTTTTTTTTT specimensByCollection size: " + specimensByCollection.size());
      if (specimensByCollection.size() == 0)
        logger.warn("fetchSpecimensByCollection: no collections found, maybe you need to upload some data to your FHIR store?");
      else {
        String randomCollectionId = (String) specimensByCollection.keySet().toArray()[0];
        logger.info("fetchSpecimensByCollection: TTTTTTTTTTTTTTTTTTTTTTTTTTT for collection ID " + randomCollectionId + ", specimen count is: " + specimensByCollection.get(randomCollectionId).size());
      }
      return specimensByCollection;
    } catch (Exception e) {
      logger.warn("fetchSpecimensByCollection: exception" + Util.traceFromException(e));
      return null;
    }
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
  protected BbmriEricId determineDefaultCollectionId(BbmriEricId defaultBbmriEricCollectionId, Map<String,List<Specimen>> specimensByCollection) {
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
   * Gets the Directory collection ID from the identifier of the supplied collection.
   * Returns DEFAULT_COLLECTION_ID if there is no identifier or if the identifier's value is not a valid
   * Directory ID.
   *
   * N.B. Assuming that this will also work for MIABIS on FHIR. Currently, the synthetic FHIR does not
   * contain Directory IDs, so it's hard to tell.
   *
   * @param collection
   * @return
   */
  protected String extractValidDirectoryIdentifierFromCollection(Organization collection) {
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
   * Retrieves all Specimens from the FHIR server and organizes them into a Map based on their Collection ID.
   *
   * @return A Map where keys are Collection IDs and values are Lists of Specimens associated with each Collection ID.
   */
  protected Map<String, List<Specimen>> getAllSpecimensAsMap() {
    return null;
  }

  public int calculateTotalSpecimenCount(BbmriEricId defaultBbmriEricCollectionId) {
    return -1;
  }

  /**
   * Retrieves all sample materials over all collections.
   *
   * @param defaultBbmriEricCollectionId the default BBMRI-ERIC collection ID. May be null.
   * @return a Map representing sample materials as key-value pairs
   */
  public Map<String,String> getSampleMaterials(BbmriEricId defaultBbmriEricCollectionId) {
    return null;
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
    return null;
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
    return null;
  }

  /**
   * Extracts a list of condition codes from a Patient resource using a FHIR client.
   * The condition codes are based on the system "http://hl7.org/fhir/sid/icd-10".
   * @param patient a Patient resource that has an ID element
   * @return a list of strings that represent the condition codes of the patient, or an empty list if none are found
   */
  public List<String> extractConditionCodesFromPatient(Patient patient) {
    return null;
  }

  /**
   * Extracts and returns any diagnoses in the extension to the specimen with the
   * URI SAMPLE_DIAGNOSIS_URI.
   *
   * @param specimen
   * @return
   */
  public List<String> extractDiagnosesFromSpecimen(Specimen specimen) {
    return null;
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
    return null;
  }

  /**
   * Fetches diagnoses from Specimens and Patients in all collections.
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
    return null;
  }
}
