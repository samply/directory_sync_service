package de.samply.directory_sync_service.fhir;

import de.samply.directory_sync_service.Util;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Specimen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Provides convenience methods for selected FHIR operations.
 */
public class FhirApiMiabisOnFhir extends FhirApi {
  protected static final Logger logger = LoggerFactory.getLogger(FhirApiMiabisOnFhir.class);
  protected static final String BIOBANK_PROFILE_URI = "https://fhir.bbmri-eric.eu/StructureDefinition/miabis-biobank";
  protected static final String COLLECTION_PROFILE_URI = "https://fhir.bbmri-eric.eu/StructureDefinition/miabis-collection"; // there is also a miabis-collection-organization, I am not sure which one to use.
  protected static final String SAMPLE_DIAGNOSIS_URI = "https://fhir.bbmri-eric.eu/StructureDefinition/miabis-sample-storage-temperature-extension";
  private static final String STORAGE_TEMP_EXTENSION = "https://fhir.bbmri-eric.eu/StructureDefinition/miabis-sample-storage-temperature-extension";
  private static final String STORAGE_TEMP_SYSTEM = "https://fhir.bbmri-eric.eu/CodeSystem/miabis-storage-temperature-cs";
  private static final String COLLECTION_EXTENSION_URL = "https://fhir.bbmri-eric.eu/StructureDefinition/miabis-sample-collection-extension";

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

  protected String getSampleDiagnosisUri() {
    return SAMPLE_DIAGNOSIS_URI;
  }

  /**
   * Find the local ID of the collection to which the specimen belongs.
   *
   * For MIABIS on FHIR, we expect the Specimen to have an extension for a collection, where we would
   * find a collection ID. We expect to find an identifier.
   *
   * @param specimen
   * @return
   */
  protected String extractLocalCollectionIdFromSpecimen(Specimen specimen) {
    if (!specimen.hasExtension())
      return null;
    Extension extension = specimen.getExtensionByUrl(COLLECTION_EXTENSION_URL);
    if (extension == null)
      return null;
    if (!(extension.getValue() instanceof Identifier identifier))
      return null;
    String localCollectionId = extractCollectionIdFromReference(identifier.getValue());

    logger.debug("extractLocalCollectionIdFromSpecimen: localCollectionId: " + localCollectionId);

    return localCollectionId;
  }

  // Map MIABIS on FHIR materials onto bbmri.de materials.
  private static final Map<String, String> MIABIS_TO_BBMRI_MATERIALS = Map.ofEntries(
          Map.entry("Blood","whole-blood"),
          Map.entry("BoneMarrowAspirate","bone-marrow"),
          Map.entry("BuffyCoat","buffy-coat"),
          Map.entry("CerebrospinalFluid","csf-liquor"),
          Map.entry("DNA","dna"),
          Map.entry("Faeces","stool-faeces"),
          Map.entry("LiquidBiopsy","liquid-other"),
          Map.entry("Other",""),
          Map.entry("PBMC","peripheral-blood-cells-vital"),
          Map.entry("Plasma","blood-plasma"),
          Map.entry("RNA","rna"),
          Map.entry("Saliva","saliva"),
          Map.entry("Serum","blood-serum"),
          Map.entry("Sputum","saliva"),
          Map.entry("TissueFFPE","tissue-formalin"),
          Map.entry("TissueFixed","tissue-paxgene-or-else"),
          Map.entry("TissueFreshFrozen","tissue-frozen"),
          Map.entry("TissueFrozen","tissue-frozen"),
          Map.entry("Urine","urine"),
          Map.entry("WholeBlood","whole-blood")
  );

  /**
   * Extracts a material code from a specimen.
   *
   * @param specimen A {@code Specimen} object from which to extract a material code.
   * @return A material code extracted from the specimen.
   */
  protected String extractMaterialFromSpecimen(Specimen specimen) {
    String miabisMaterial = super.extractMaterialFromSpecimen(specimen);
    if (miabisMaterial == null)
      return null;
    String material = miabisMaterial.toLowerCase();
    if (MIABIS_TO_BBMRI_MATERIALS.containsKey(miabisMaterial))
      material = MIABIS_TO_BBMRI_MATERIALS.get(miabisMaterial);
    else
      logger.warn("extractMaterialFromSpecimen: no mapping for MIABIS on FHIR material: " + miabisMaterial + ", replacing with:" + material);
    return material;
  }

  // Map MIABIS on FHIR materials onto bbmri.de materials.
  private static final Map<String, String> MIABIS_TO_BBMRI_TEMPERATURES = Map.ofEntries(
          Map.entry("RT","temperatureRoom"),
          Map.entry("2to10","temperature2to10"),
          Map.entry("-18to-35","temperature-18to-35"),
          Map.entry("-60to-85","temperature-60to-85"),
          Map.entry("LN","temperatureLN"),
          Map.entry("Other","temperatureOther")
  );

  protected List<String> extractStorageTemperaturesFromSpecimenList(List<Specimen> specimens) {
    List<String> storageTemperatures = null;
    try {
      List<String> miabisStorageTemperatures = specimens.stream()
              .flatMap(specimen -> specimen.getProcessing().stream())
              .flatMap(processing -> processing.getExtension().stream())
              .filter(extension -> STORAGE_TEMP_EXTENSION.equals(extension.getUrl()))
              .map(extension -> extension.getValue())
              .filter(CodeableConcept.class::isInstance)
              .map(CodeableConcept.class::cast)
              .flatMap(cc -> cc.getCoding().stream())
              .filter(coding -> STORAGE_TEMP_SYSTEM.equals(coding.getSystem()))
              .map(Coding::getCode)
              .filter(Objects::nonNull)
              .distinct()
              .toList();
      // Convert MIABIS on FHIR storage temperatures to bbmri.de storage temperatures.
      storageTemperatures = new ArrayList<>();
      for (String miabisStorageTemperature : miabisStorageTemperatures) {
        if (miabisStorageTemperature == null || miabisStorageTemperature.isEmpty())
          continue;
        if (MIABIS_TO_BBMRI_TEMPERATURES.containsKey(miabisStorageTemperature))
          storageTemperatures.add(MIABIS_TO_BBMRI_TEMPERATURES.get(miabisStorageTemperature));
        else {
          String temperature = "temperature" + miabisStorageTemperature;
          logger.warn("extractStorageTemperaturesFromSpecimenList: no mapping for MIABIS on FHIR temperature: " + miabisStorageTemperatures + ", replacing with: " + temperature);
          storageTemperatures.add(temperature);
        }
      }
    } catch (Exception e) {
      logger.warn("extractStorageTemperaturesFromSpecimenList: exception: " + Util.traceFromException(e));
    }

    return storageTemperatures;
  }
}
