package de.samply.directory_sync_service.fhir;

import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Specimen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Provides convenience methods for selected FHIR operations.
 */
public class FhirApiBbmriDe extends FhirApi {
  protected static final Logger logger = LoggerFactory.getLogger(FhirApiBbmriDe.class);
  protected static final String STORAGE_TEMPERATURE_URI = "https://fhir.bbmri.de/StructureDefinition/StorageTemperature";
  protected static final String SAMPLE_DIAGNOSIS_URI = "https://fhir.bbmri.de/StructureDefinition/SampleDiagnosis";
  protected static final String BIOBANK_PROFILE_URI = "https://fhir.bbmri.de/StructureDefinition/Biobank";
  protected static final String COLLECTION_PROFILE_URI = "https://fhir.bbmri.de/StructureDefinition/Collection";

  public FhirApiBbmriDe(String fhirStoreUrl) {
    super(fhirStoreUrl);
    logger.debug("FhirApiBbmriDe: initialized");
  }

  protected String getBiobankProfileUri() {
    return BIOBANK_PROFILE_URI;
  }

  protected String getCollectionProfileUri() {
    return COLLECTION_PROFILE_URI;
  }

  protected String getSampleDiagnosisUri() {
    return SAMPLE_DIAGNOSIS_URI;
  }

  /**
   * Find the local ID of the collection to which the specimen belongs.
   *
   * For bbmri.de, we expect the Specimen to have an extension for a collection, where we would
   * find a collection ID. We expect to find a reference.
   *
   * @param specimen
   * @return
   */
  protected String extractLocalCollectionIdFromSpecimen(Specimen specimen) {
    // Find the relevant extension.
    if (!specimen.hasExtension())
      return null;
    Extension extension = specimen.getExtensionByUrl("https://fhir.bbmri.de/StructureDefinition/Custodian");
    if (extension == null)
      return null;

    // Pull the locally-used collection ID from the specimen extension.
    String reference = ((Reference) extension.getValue()).getReference();
    String localCollectionId = extractCollectionIdFromReference(reference);

    return localCollectionId;
  }

  protected List<String> extractStorageTemperaturesFromSpecimenList(List<Specimen> specimenList) {
    return extractExtensionElementValuesFromSpecimens(specimenList, STORAGE_TEMPERATURE_URI);
  }
}
