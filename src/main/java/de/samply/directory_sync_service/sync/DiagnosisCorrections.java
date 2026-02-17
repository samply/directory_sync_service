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

            logger.info("generateDiagnosisCorrections: defaultBbmriEricCollectionId: " + defaultBbmriEricCollectionId);

            // Get all diagnoses from the FHIR store for specimens with identifiable
            // collections and their associated patients.
            List<String> fhirDiagnoses = fhirApi.fetchDiagnoses(defaultBbmriEricCollectionId);
            if (fhirDiagnoses == null) {
                logger.warn("Problem getting diagnosis information from FHIR store");
                return correctedDiagnoses;
            }

            logger.info("generateDiagnosisCorrections: fhirDiagnoses.size(): " + fhirDiagnoses.size());

            if (fhirDiagnoses.size() == 0) {
                logger.warn("generateDiagnosisCorrections: No diagnoses found in FHIR store, no need to continue");
                return correctedDiagnoses;
            }

            // Convert the raw ICD 10 codes into MIRIAM-compatible codes and put the
            // codes into a map with identical keys and values.
            fhirDiagnoses.forEach(diagnosis -> {
                String miriamDiagnosis = FhirToDirectoryAttributeConverter.convertDiagnosis(diagnosis);
                correctedDiagnoses.put(miriamDiagnosis, miriamDiagnosis);
            });

            logger.info("generateDiagnosisCorrections: correctedDiagnoses.size(): " + correctedDiagnoses.size());

            // Get corrected diagnosis codes from the Directory
            //directoryApi.collectDiagnosisCorrections(correctedDiagnoses);
            collectDiagnosisCorrections(directoryApi, correctedDiagnoses);

            if (correctedDiagnoses.size() == 0)
                logger.warn("generateDiagnosisCorrections: No diagnosis corrections generated");

            logger.info("generateDiagnosisCorrections: done");

            return correctedDiagnoses;
        } catch (Exception e) {
            logger.warn("generateDiagnosisCorrections - unexpected error: " + Util.traceFromException(e));
            logger.warn("generateDiagnosisCorrections - exception: " + e);
            logger.warn("generateDiagnosisCorrections - trace: " + e.getStackTrace());
            logger.warn("generateDiagnosisCorrections - exception message: " + e.getMessage());
        }

        return null;
    }

    /**
     * Collects diagnosis corrections from the Directory.
     * <p>
     * It checks with the Directory if the diagnosis codes are valid ICD values and corrects them if necessary.
     * <p>
     * Two levels of correction are possible:
     * <p>
     * 1. If the full code is not correct, remove the number after the period and try again. If the new truncated code is OK, use it to replace the existing diagnosis.
     * 2. If that doesn't work, replace the existing diagnosis with null.
     * <p>
     * The supplied Map object, {@code diagnoses}, is modified in-place.
     *
     * @param directoryApi DirectoryApi object for GraphQL, REST or whatever.
     * @param diagnoses A string map containing diagnoses to be corrected.
     */
    private static void collectDiagnosisCorrections(DirectoryApi directoryApi, Map<String, String> diagnoses) {
        if (diagnoses == null) {
            logger.warn("collectDiagnosisCorrections: diagnoses is null");
            return;
        }

        if (diagnoses.size() == 0) {
            logger.warn("collectDiagnosisCorrections: diagnoses is empty");
            return;
        }

        if (diagnoses.keySet().size() < 5) {
            logger.debug("collectDiagnosisCorrections: uncorrected diagnoses: ");
            for (String diagnosis : diagnoses.keySet())
                logger.debug("collectDiagnosisCorrections: diagnosis: " + diagnosis);
        }

        int diagnosisCounter = 0; // for diagnostics only
        int diagnosisNullCounter = 0;
        int invalidIcdValueCounter = 0;
        int correctedIcdValueCounter = 0;
        int discardedIcdValueCounter = 0;
        // Apply corrections to all diagnoses if necessary
        for (String diagnosis: diagnoses.keySet()) {
            if (diagnosisCounter%500 == 0)
                logger.debug("collectDiagnosisCorrections: diagnosisCounter: " + diagnosisCounter + ", total diagnoses: " + diagnoses.size());
            if (diagnosis == null)
                diagnosisNullCounter++;
            else if (!directoryApi.isValidIcdValue(diagnosis)) {
                // The current disgnosis is not valid, try to correct it by removing
                // any numbers after the period.
                invalidIcdValueCounter++;
                String diagnosisCategory = diagnosis.split("\\.")[0];
                if (directoryApi.isValidIcdValue(diagnosisCategory)) {
                    // Diagnosis successfully corrected.
                    logger.debug("collectDiagnosisCorrections: corrected diagnosis from: " + diagnosis + " to: " + diagnosisCategory);
                    correctedIcdValueCounter++;
                    diagnoses.put(diagnosis, diagnosisCategory);
                } else {
                    // Diagnosis still invalid, replace with null.
                    logger.debug("collectDiagnosisCorrections: diagnosisCategory: " + diagnosisCategory + " is still not valid, replacing with null");
                    discardedIcdValueCounter++;
                    diagnoses.put(diagnosis, null);
                }
            } // else do nothing: leave diagnosis untouched
            diagnosisCounter++;
        }

        logger.debug("collectDiagnosisCorrections: total diagnoses: " + diagnoses.size() + ", diagnosisNullCounter: " + diagnosisNullCounter + ", invalidIcdValueCounter: " + invalidIcdValueCounter + ", correctedIcdValueCounter: " + correctedIcdValueCounter + ", discardedIcdValueCounter: " + discardedIcdValueCounter);
        if (diagnoses.keySet().size() > 0 && diagnoses.keySet().size() < 5) {
            logger.debug("collectDiagnosisCorrections: corrected diagnoses: ");
            for (String diagnosis : diagnoses.keySet())
                logger.debug("collectDiagnosisCorrections: diagnosis: " + diagnosis);
        }
    }

}
