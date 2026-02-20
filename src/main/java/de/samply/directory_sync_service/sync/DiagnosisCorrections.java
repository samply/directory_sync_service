package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import de.samply.directory_sync_service.converter.Icd10WhoNormalizer;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.fhir.FhirApi;
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
            List<String> fhirDiagnoses = fhirApi.fetchDiagnoses(defaultCollectionId);

            if (fhirDiagnoses == null) {
                logger.warn("generateDiagnosisCorrections: problem finding diagnoses in FHIR store, aborting");
                return correctedDiagnoses;
            }

            int diagnosisCounter = 0; // for diagnostics only
            int diagnosisNullCounter = 0;
            int invalidIcdValueCounter = 0;
            int correctedIcdValueCounter = 0;
            int discardedIcdValueCounter = 0;
            for (String diagnosis : fhirDiagnoses) {
                // Convert the raw ICD 10 codes into MIRIAM-compatible codes and put the
                // codes into a map with identical keys and values.
                String miriamDiagnosis = FhirToDirectoryAttributeConverter.convertDiagnosis(diagnosis);
                correctedDiagnoses.put(miriamDiagnosis, miriamDiagnosis);

                // Check with the Directory if the diagnosis code is a valid ICD value and correct it if necessary.
                if (diagnosisCounter%500 == 0)
                    logger.debug("collectDiagnosisCorrections: diagnosisCounter: " + diagnosisCounter + ", total diagnoses: " + correctedDiagnoses.size());
                if (miriamDiagnosis == null)
                    diagnosisNullCounter++;
                else if (!directoryApi.isValidIcdValue(miriamDiagnosis)) {
                    invalidIcdValueCounter++;

                    // First try to normalize the ICD-10 code and then see if that works.
                    String normalizedMiriamDiagnosis = FhirToDirectoryAttributeConverter.convertDiagnosis(Icd10WhoNormalizer.normalize(diagnosis));
                    String normalizedDiagnosisCategory = normalizedMiriamDiagnosis.split("\\.")[0];

                    if (normalizedMiriamDiagnosis.equals("urn:miriam:icd:R69"))
                        logger.debug("collectDiagnosisCorrections: =========================================== we got the unknown code: R69");

                    if (directoryApi.isValidIcdValue(normalizedMiriamDiagnosis)) {
                        // Normalized diagnosis successfully corrected.
                        logger.debug("collectDiagnosisCorrections: corrected diagnosis from: " + miriamDiagnosis + " to normalized: " + normalizedMiriamDiagnosis);
                        correctedIcdValueCounter++;
                        correctedDiagnoses.put(miriamDiagnosis, normalizedMiriamDiagnosis);
                    } else if (directoryApi.isValidIcdValue(normalizedDiagnosisCategory)) {
                        // The normalized diagnosis is not valid, try to correct it by removing
                        // any numbers after the period (if there is one).
                        logger.debug("collectDiagnosisCorrections: corrected normalized diagnosis from: " + normalizedMiriamDiagnosis + " to base: " + normalizedDiagnosisCategory);
                        correctedIcdValueCounter++;
                        correctedDiagnoses.put(miriamDiagnosis, normalizedDiagnosisCategory);
                    } else {
                        // The current diagnosis is not valid, try to correct it by removing
                        // any numbers after the period (if there is one).
                        String diagnosisCategory = miriamDiagnosis.split("\\.")[0];
                        if (directoryApi.isValidIcdValue(diagnosisCategory)) {
                            // Diagnosis successfully corrected.
                            logger.debug("collectDiagnosisCorrections: corrected diagnosis from: " + miriamDiagnosis + " to base: " + diagnosisCategory);
                            correctedIcdValueCounter++;
                            correctedDiagnoses.put(miriamDiagnosis, diagnosisCategory);
                        } else {
                            // Diagnosis still invalid, replace with null.
                            logger.debug("collectDiagnosisCorrections: diagnosisCategory: " + diagnosisCategory + " is still not valid, replacing with null");
                            discardedIcdValueCounter++;
                            correctedDiagnoses.put(miriamDiagnosis, null);
                        }
                    }
                } // else do nothing: leave diagnosis untouched
                diagnosisCounter++;
            }

            logger.debug("generateDiagnosisCorrections: total diagnoses: " + correctedDiagnoses.size() + ", diagnosisNullCounter: " + diagnosisNullCounter + ", invalidIcdValueCounter: " + invalidIcdValueCounter + ", correctedIcdValueCounter: " + correctedIcdValueCounter + ", discardedIcdValueCounter: " + discardedIcdValueCounter);
            logger.info("generateDiagnosisCorrections: correctedDiagnoses.size(): " + correctedDiagnoses.size());

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
}
