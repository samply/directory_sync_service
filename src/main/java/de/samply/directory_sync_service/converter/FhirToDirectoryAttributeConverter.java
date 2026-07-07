package de.samply.directory_sync_service.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Utility class for converting FHIR attributes to Directory attributes.
 * This class provides static methods to perform conversions for specific FHIR attributes
 * in order to align them with Directory attribute conventions.
 */
public class FhirToDirectoryAttributeConverter {
    private static final Logger logger = LoggerFactory.getLogger(FhirToDirectoryAttributeConverter.class);


    private static final Map<String, String> FHIR_TO_DIRECTORY_SEX = Map.of(
            "male", "MALE",
            "female", "FEMALE",
            "other", "UNDIFFERENTIAL",
            "unknown", "NAV"
    );
    /**
     * Converts the given sex attribute to Directory conventions.
     *
     * @param sex The sex attribute to be converted.
     * @return The converted sex attribute in uppercase.
     */
    public static String convertSex(String sex) {
        if (sex == null)
            return null;

        // Convert standard FHIR sex values.
        String lcSex = sex.toLowerCase();
        if (FHIR_TO_DIRECTORY_SEX.containsKey(lcSex))
            return FHIR_TO_DIRECTORY_SEX.get(lcSex);

        logger.warn("convertSex: unknown sex: " + sex + ", defaulting to NAV");
        return "NAV";
    }

    private static final Map<String, String> FHIR_TO_DIRECTORY_MATERIAL = Map.ofEntries(
            Map.entry("ascites", "OTHER"),
            Map.entry("blood-plasma", "PLASMA"),
            Map.entry("blood-serum", "SERUM"),
            Map.entry("bone-marrow", "OTHER"),
            Map.entry("buffy-coat", "BUFFY_COAT"),
            Map.entry("cf-dna", "CDNA"),
            Map.entry("csf-liquor", "OTHER"),
            Map.entry("derivative", "OTHER"),
            Map.entry("derivative-other", "OTHER"),
            Map.entry("dna", "DNA"),
            Map.entry("dried-whole-blood", "WHOLE_BLOOD"),
            Map.entry("liquid", "OTHER"),
            Map.entry("liquid-other", "OTHER"),
            Map.entry("peripheral-blood-cells-vital", "PERIPHERAL_BLOOD_CELLS"),
            Map.entry("rna", "RNA"),
            Map.entry("saliva", "SALIVA"),
            Map.entry("stool-faeces", "FECES"),
            Map.entry("swab", "NASAL_SWAB"),
            Map.entry("tissue", "TISSUE_FROZEN"),
            Map.entry("tissue-ffpe", "TISSUE_PARAFFIN_EMBEDDED"),
            Map.entry("tissue-formalin", "TISSUE_PARAFFIN_EMBEDDED"),
            Map.entry("tissue-frozen", "TISSUE_FROZEN"),
            Map.entry("tissue-other", "TISSUE_FROZEN"),
            Map.entry("tissue-paxgene-or-else", "OTHER"),
            Map.entry("urine", "URINE"),
            Map.entry("whole-blood", "WHOLE_BLOOD")
    );
    /**
     * Converts the given material attribute to Directory conventions.
     *
     * @param material The material attribute to be converted.
     * @return The converted material attribute according to Directory conventions.
     */
    public static String convertMaterial(String material) {
        if (material == null)
            return null;

        // Convert standard FHIR material names.
        String lcMaterial = material.toLowerCase().replaceAll("_", "-");
        if (FHIR_TO_DIRECTORY_MATERIAL.containsKey(lcMaterial))
            return FHIR_TO_DIRECTORY_MATERIAL.get(lcMaterial);

        // Some material names are uppercase, so they might actually already be valid Directory terms..
        String ucMaterial = material.toUpperCase().replaceAll("-", "_");
        if (ucMaterial.contains("_VITAL"))
            ucMaterial = ucMaterial.replaceAll("_VITAL", "");
        if (FHIR_TO_DIRECTORY_MATERIAL.containsValue(ucMaterial))
            return ucMaterial;

        logger.info("convertMaterial: material has a non-standard name, trying some heuristics");

        // Heuristics for some frequently-seen non-standard material names.
        if (ucMaterial.contains("TISSUE")) {
            return "TISSUE_FROZEN";
        }
        if (ucMaterial.contains("CF_DNA")) {
            return "CDNA";
        }
        if (ucMaterial.contains("SERUM")) {
            return "SERUM";
        }
        if (ucMaterial.contains("STOOL") || ucMaterial.contains("FAECES") || ucMaterial.contains("FECES")) {
            return "FECES";
        }
        if (ucMaterial.contains("PLASMA")) {
            return "PLASMA";
        }

        logger.warn("convertMaterial: unknown material: " + material + ", defaulting to OTHER");
        return "OTHER";
    }

    private static final Map<String, String> FHIR_TO_DIRECTORY_TEMPERATURE = Map.ofEntries(
            Map.entry("temperatureRoom", "temperatureRoom"),
            Map.entry("temperature2to10", "temperature2to10"),
            Map.entry("four_degrees", "temperatureOther"),
            Map.entry("temperature-18to-35", "temperature-18to-35"),
            Map.entry("temperature-60to-85", "temperature-60to-85"),
            Map.entry("temperatureGN", "temperatureOther"),
            Map.entry("temperatureLN", "temperatureLN"),
            Map.entry("storage_temperature_uncharted", "temperatureOther"),
            Map.entry("temperatureOther", "temperatureOther")
    );
    /**
     * Converts the given storage temperature attribute to align with Directory conventions.
     *
     * @param storageTemperature The storage temperature attribute to be converted.
     * @return The converted storage temperature attribute according to Directory conventions.
     */
    public static String convertStorageTemperature(String storageTemperature) {
        if (storageTemperature == null)
            return null;

        // Convert standard FHIR temperature values.
        if (FHIR_TO_DIRECTORY_TEMPERATURE.containsKey(storageTemperature))
            return FHIR_TO_DIRECTORY_TEMPERATURE.get(storageTemperature);

        logger.warn("convertStorageTemperature: unknown storage temperature: " + storageTemperature + ", defaulting to temperatureOther");
        return "temperatureOther";
    }

    /**
     * Converts the given diagnosis attribute to a MIRIAM ICD code if not already in MIRIAM format.
     *
     * @param diagnosis The diagnosis attribute to be converted.
     * @return The converted diagnosis attribute in MIRIAM ICD format.
     */
    public static String convertDiagnosis(String diagnosis) {
        if (diagnosis == null)
            return null;
        if (diagnosis.isEmpty())
            return "";

        String miriamDiagnosis = diagnosis;
        if (!miriamDiagnosis.startsWith("urn:miriam:icd:"))
            miriamDiagnosis = "urn:miriam:icd:" + miriamDiagnosis;

        return miriamDiagnosis;
    }
}
