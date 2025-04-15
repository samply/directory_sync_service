package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;
import de.samply.directory_sync_service.model.BbmriEricId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiagnosisCorrections {
    private static final Logger logger = LoggerFactory.getLogger(DiagnosisCorrections.class);

    /**
     * Generates corrections to the diagnoses obtained from the FHIR store, to make them
     * compatible with the Directory. You should supply this method with an empty map
     * via the correctedDiagnoses variable. This map will be filled by the method and
     * you can subsequently use it elsewhere.
     * <p>
     * This method performs the following steps:
     * <p>
     * * Retrieves diagnoses from the FHIR store for specimens with identifiable collections and their associated patients.
     * * Converts raw ICD-10 codes into MIRIAM-compatible codes.
     * * Collects corrected diagnosis codes from the Directory API based on the MIRIAM-compatible codes.
     * <p>
     * @param fhirApi
     * @param directoryApi
     * @param defaultCollectionId Default collection ID. May be null.
     * @return A list containing diagnosis corrections.
     *         If any errors occur during the process, null is returned.
     */
    public static Map<String, String> generateDiagnosisCorrections(FhirApi fhirApi, DirectoryApi directoryApi, String defaultCollectionId) {
        try {
            Map<String, String> correctedDiagnoses = new HashMap<String, String>();
            // Convert string version of collection ID into a BBMRI ERIC ID.
            BbmriEricId defaultBbmriEricCollectionId = BbmriEricId
                    .valueOf(defaultCollectionId)
                    .orElse(null);

            // Get all diagnoses from the FHIR store for specimens with identifiable
            // collections and their associated patients.
            List<String> fhirDiagnoses = fhirApi.fetchDiagnoses(defaultBbmriEricCollectionId);
            if (fhirDiagnoses == null) {
                logger.warn("Problem getting diagnosis information from FHIR store");
                return null;
            }
            logger.debug("generateDiagnosisCorrections: fhirDiagnoses.size(): " + fhirDiagnoses.size());

            if (fhirDiagnoses.size() == 0) {
                logger.warn("generateDiagnosisCorrections: No diagnoses found in FHIR store, no need to continu");
                return correctedDiagnoses;
            }

            // Convert the raw ICD 10 codes into MIRIAM-compatible codes and put the
            // codes into a map with identical keys and values.
            fhirDiagnoses.forEach(diagnosis -> {
                String miriamDiagnosis = FhirToDirectoryAttributeConverter.convertDiagnosis(diagnosis);
                correctedDiagnoses.put(miriamDiagnosis, miriamDiagnosis);
            });

            // Get corrected diagnosis codes from the Directory
            directoryApi.collectDiagnosisCorrections(correctedDiagnoses);
            logger.debug("generateDiagnosisCorrections: correctedDiagnoses.size(): " + correctedDiagnoses.size());

            if (correctedDiagnoses.size() == 0)
                logger.warn("generateDiagnosisCorrections: No diagnosis corrections generated");

            return correctedDiagnoses;
        } catch (Exception e) {
            logger.warn("generateDiagnosisCorrections - unexpected error: " + Util.traceFromException(e));
        }

        return null;
    }
}
