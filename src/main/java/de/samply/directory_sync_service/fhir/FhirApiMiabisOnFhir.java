package de.samply.directory_sync_service.fhir;

import ca.uhn.fhir.rest.api.SummaryEnum;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.model.BbmriEricId;
import de.samply.directory_sync_service.model.Collection;
import de.samply.directory_sync_service.model.Collections;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.Specimen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ca.uhn.fhir.rest.api.PreferReturnEnum.OPERATION_OUTCOME;
import static org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity.ERROR;

/**
 * Provides convenience methods for selected FHIR operations.
 */
public class FhirApiMiabisOnFhir extends FhirApi {
  protected static final Logger logger = LoggerFactory.getLogger(FhirApiMiabisOnFhir.class);
  protected static final String BIOBANK_PROFILE_URI = "https://fhir.bbmri-eric.eu/StructureDefinition/miabis-biobank";
  protected static final String COLLECTION_PROFILE_URI = "https://fhir.bbmri-eric.eu/StructureDefinition/miabis-collection"; // there is also a miabis-collection-organization, I am not sure which one to use.

  public FhirApiMiabisOnFhir(String fhirStoreUrl) {
    super(fhirStoreUrl);
    logger.debug("FhirApiMiabisOnFhir: initialized");
  }

  protected String getBiobankProfileUri() {
    return BIOBANK_PROFILE_URI;
  }

  protected String getCollectionProfileUri() {
    return COLLECTION_PROFILE_URI;
  }

  /**
   * Lists all Organization resources with the biobank profile.
   *
   * @return a list of {@link Organization} resources or null on *
   * errors
   */
  public List<Organization> listAllBiobanks() {
    return null;
  }

  /**
   * Retrieves a list of all collections that match the given list of collection IDs.
   *
   * @param collectionIds A list of collection IDs.
   * @return A list of {@link Organization} objects representing biobank collections,
   *         or {@code null} if an error occurs.
   */
  public List<Organization> listAllCollections(List<String> collectionIds) {
    return null;
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
    return null;
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
