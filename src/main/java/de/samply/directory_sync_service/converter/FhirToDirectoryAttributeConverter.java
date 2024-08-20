package de.samply.directory_sync_service.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for converting FHIR attributes to Directory attributes.
 * This class provides static methods to perform conversions for specific FHIR attributes
 * in order to align them with Directory attribute conventions.
 */
public class FhirToDirectoryAttributeConverter {
    private static final Logger logger = LoggerFactory.getLogger(FhirToDirectoryAttributeConverter.class);

    /**
     * Converts the given sex attribute to uppercase as per Directory conventions.
     *
     * @param sex The sex attribute to be converted.
     * @return The converted sex attribute in uppercase.
     */
    public static String convertSex(String sex) {
        // Signifiers for sex largely overlap between FHIR and Directory, but Directory likes
        // upper case
        return sex.toUpperCase();
    }

    /**
     * Converts the given age attribute. No conversion needed.
     *
     * @param age The age attribute to be converted.
     * @return The unchanged age attribute.
     */
    public static Integer convertAge(Integer age) {
        // No conversion needed
        return age;
    }

    /**
     * Converts the given material attribute to Directory conventions.
     *
     * @param material The material attribute to be converted.
     * @return The converted material attribute according to Directory conventions.
     */
    public static String convertMaterial(String material) {
        if (material == null)
            return null;

        String directoryMaterial = material
                // Basic conversion: make everything upper case, replace - with _
                .toUpperCase()
                .replaceAll("-", "_")
                // Some names are different between FHIR and Directory, so convert those.
                .replaceAll("_VITAL", "")
                .replaceAll("^TISSUE_FORMALIN$", "TISSUE_PARAFFIN_EMBEDDED")
                .replaceAll("^TISSUE$", "TISSUE_FROZEN")
                .replaceAll("^CF_DNA$", "CDNA")
                .replaceAll("^BLOOD_SERUM$", "SERUM")
                .replaceAll("^STOOL_FAECES$", "FECES")
                .replaceAll("^BLOOD_PLASMA$", "SERUM")
                // Some names are present in FHIR but not in Directory. Use "OTHER" as a placeholder.
                .replaceAll("^.*_OTHER$", "OTHER")
                .replaceAll("^DERIVATIVE$", "OTHER")
                .replaceAll("^CSF_LIQUOR$", "OTHER")
                .replaceAll("^LIQUID$", "OTHER")
                .replaceAll("^ASCITES$", "OTHER")
                .replaceAll("^BONE_MARROW$", "OTHER")
                .replaceAll("^TISSUE_PAXGENE_OR_ELSE$", "OTHER")
                ;
    
        return directoryMaterial;
    }

    /**
     * Converts the given storage temperature attribute to align with Directory conventions.
     *
     * @param storageTemperature The storage temperature attribute to be converted.
     * @return The converted storage temperature attribute according to Directory conventions.
     */
    public static String convertStorageTemperature(String storageTemperature) {
        if (storageTemperature == null)
            return null;

        // The Directory understands most of the FHIR temperature codes, but it doesn't
        // know about gaseous nitrogen.
        String directoryStorageTemperature = storageTemperature
            .replaceAll("temperatureGN", "temperatureOther");

        return directoryStorageTemperature;
    }

    /**
     * Converts the given diagnosis attribute to a MIRIAM ICD code if not already in MIRIAM format.
     *
     * @param diagnosis The diagnosis attribute to be converted.
     * @return The converted diagnosis attribute in MIRIAM ICD format or null if the input is null.
     */
    public static String convertDiagnosis(String diagnosis) {
        if (diagnosis == null)
            return null;

        String miriamDiagnosis = null;
        if (diagnosis.startsWith("urn:miriam:icd:"))
            miriamDiagnosis = diagnosis;
        else if (diagnosis.length() == 3 || diagnosis.length() == 5)  // E.g. C75 or E23.1
            miriamDiagnosis = "urn:miriam:icd:" + diagnosis;
        else
            logger.warn("Entities.setDiagnosisAvailable: invalid diagnosis code " + diagnosis);
        
        return miriamDiagnosis;
    }
}
