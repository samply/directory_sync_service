package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.converter.FhirToDirectoryAttributeConverter;
import de.samply.directory_sync_service.directory.DirectoryApi;
import de.samply.directory_sync_service.directory.DirectoryApiWriteToFile;
import de.samply.directory_sync_service.directory.MergeDirectoryCollectionGetToDirectoryCollectionPut;
import de.samply.directory_sync_service.directory.model.Collections;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.directory.model.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Update collections in the Directory with data from the local FHIR store.
 */
public class CollectionUpdater {
    private static final Logger logger = LoggerFactory.getLogger(CollectionUpdater.class);

    /**
     * Take information from the FHIR store and send aggregated updates to the Directory.
     * <p>
     * This is a multi step process:
     *  1. Fetch a list of collections objects from the FHIR store. These contain aggregated
     *     information over all specimens in the collections.
     *  2. Convert the FHIR collection objects into Directory collection PUT DTOs. Copy
     *     over avaialble information from FHIR, converting where necessary.
     *  3. Using the collection IDs found in the FHIR store, send queries to the Directory
     *     and fetch back the relevant GET collections. If any of the collection IDs cannot be
     *     found, this ie a breaking error.
     *  4. Transfer data from the Directory GET collections to the corresponding Directory PUT
     *     collections.
     *  5. Push the new information back to the Directory.
     *
     * @param directoryApi API for communicating with Directory.
     * @param correctedDiagnoses Maps ICD10 codes to corrected ICD10 codes.
     * @param collectionList List of FHIR collection objects.
     * @return A list of OperationOutcome objects indicating the outcome of the update operation.
     */
    public static boolean sendUpdatesToDirectory(DirectoryApi directoryApi, Map<String, String> correctedDiagnoses, List<Collection> collectionList) {
        try {
            DirectoryCollectionPut directoryCollectionPut = convert(collectionList);
            if (directoryCollectionPut == null) {
                logger.warn("sendUpdatesToDirectory: Problem converting FHIR attributes to Directory attributes");
                return false;
            }
            logger.debug("sendUpdatesToDirectory: 1 directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

            List<String> collectionIds = directoryCollectionPut.getCollectionIds();
            String countryCode = directoryCollectionPut.getCountryCode();
            directoryApi.login();
            Collections collections = directoryApi.fetchCollections(countryCode, collectionIds);
            if (collections == null) {
                logger.warn("Problem getting collections from Directory");
                return false;
            }
            logger.debug("sendUpdatesToDirectory: collections.size(): " + collections.size());

            // Merge the information relating to the collection that was pulled from the
            // Directory (directoryCollectionGet) with the information pulled from
            // the FHIR store (directoryCollectionPut). Don't do this however if
            // directoryApi is an instance of DirectoryApiWriteToFile, because this
            // class does not actually operate with a real Directory, so the
            // information will not actually be present.
            if (!(directoryApi instanceof DirectoryApiWriteToFile) &&
                    !MergeDirectoryCollectionGetToDirectoryCollectionPut.merge(collections, directoryCollectionPut)) {
                logger.warn("Problem merging Directory GET attributes to Directory PUT attributes");
                return false;
            }

            // Apply corrections to ICD 10 diagnoses, to make them compatible with
            // the Directory.
            directoryCollectionPut.applyDiagnosisCorrections(correctedDiagnoses);
            logger.debug("sendUpdatesToDirectory: directoryCollectionPut.getCollectionIds().size()): " + directoryCollectionPut.getCollectionIds().size());

            directoryApi.login();

            if (!directoryApi.updateEntities(directoryCollectionPut)) {
                logger.warn("sendUpdatesToDirectory: Problem during collection update");
                return false;
            }

            logger.info("sendUpdatesToDirectory: successfully sent updates to Directory");

            return true;
        } catch (Exception e) {
            logger.warn("sendUpdatesToDirectory - unexpected error: " + Util.traceFromException(e));
            return false;
        }
    }

    public static DirectoryCollectionPut convert(List<Collection> collections) {
        DirectoryCollectionPut directoryCollectionPut = new DirectoryCollectionPut();

        for (Collection collection : collections)
            if (convert(directoryCollectionPut, collection) == null) {
                directoryCollectionPut = null;
                break;
            }

        return directoryCollectionPut;
    }

    private static DirectoryCollectionPut convert(DirectoryCollectionPut directoryCollectionPut, Collection collection) {
        try {
            convertSize(directoryCollectionPut, collection);
            convertNumberOfDonors(directoryCollectionPut, collection);
            convertSex(directoryCollectionPut, collection);
            convertAgeLow(directoryCollectionPut, collection);
            convertAgeHigh(directoryCollectionPut, collection);
            convertMaterials(directoryCollectionPut, collection);
            convertStorageTemperatures(directoryCollectionPut, collection);
            convertDiagnosisAvailable(directoryCollectionPut, collection);
        } catch(Exception e) {
            logger.error("Problem converting FHIR attributes to Directory attributes. " + Util.traceFromException(e));
            return null;
        }

        return directoryCollectionPut;
    }

    private static void convertSize(DirectoryCollectionPut directoryCollectionPut, Collection collection) {
        String id = collection.getId();
        Integer size = collection.getSize();
        directoryCollectionPut.setSize(id, size);
        // Order of magnitude is mandatory in the Directory and can be derived from size
        directoryCollectionPut.setOrderOfMagnitude(id, (int) Math.floor(Math.log10(size)));
    }

    private static void convertNumberOfDonors(DirectoryCollectionPut directoryCollectionPut, Collection collection) {
        String id = collection.getId();
        Integer size = collection.getNumberOfDonors();
        directoryCollectionPut.setNumberOfDonors(id, size);
        // Order of magnitude is mandatory in the Directory and can be derived from size
        directoryCollectionPut.setOrderOfMagnitudeDonors(id, (int) Math.floor(Math.log10(size)));
    }

    private static void convertSex(DirectoryCollectionPut directoryCollectionPut, Collection collection) {
        String id = collection.getId();
        List<String> sex = collection.getSex();

        List<String> ucSex = sex.stream()
                .map(s -> FhirToDirectoryAttributeConverter.convertSex(s))
                .filter(Objects::nonNull) // Use a method reference to check for non-null values
                .distinct()  // Remove duplicate elements
                .collect(Collectors.toList());

        directoryCollectionPut.setSex(id, ucSex);
    }

    private static void convertAgeLow(DirectoryCollectionPut directoryCollectionPut, Collection collection) {
        String id = collection.getId();
        Integer ageLow = collection.getAgeLow();
        // No conversion needed
        directoryCollectionPut.setAgeLow(id, ageLow);
    }

    private static void convertAgeHigh(DirectoryCollectionPut directoryCollectionPut, Collection collection) {
        String id = collection.getId();
        Integer ageHigh = collection.getAgeHigh();
        // No conversion needed
        directoryCollectionPut.setAgeHigh(id, ageHigh);
    }

    private static void convertMaterials(DirectoryCollectionPut directoryCollectionPut, Collection collection) {
        String id = collection.getId();
        List<String> materials = collection.getMaterials();

        if (materials == null)
            materials = new ArrayList<String>();

        List<String> directoryMaterials = materials.stream()
                .map(s -> FhirToDirectoryAttributeConverter.convertMaterial(s))
                .filter(Objects::nonNull) // Use a method reference to check for non-null values
                .distinct()  // Remove duplicate elements
                .collect(Collectors.toList());

        directoryCollectionPut.setMaterials(id, directoryMaterials);
    }

    private static void convertStorageTemperatures(DirectoryCollectionPut directoryCollectionPut, Collection collection) {
        String id = collection.getId();
        List<String> storageTemperatures = collection.getStorageTemperatures();

        if (storageTemperatures == null)
            storageTemperatures = new ArrayList<String>();

        List<String> directoryStorageTemperatures = storageTemperatures.stream()
                .map(s -> FhirToDirectoryAttributeConverter.convertStorageTemperature(s))
                .filter(Objects::nonNull) // Use a method reference to check for non-null values
                .distinct()  // Remove duplicate elements
                .collect(Collectors.toList());

        directoryCollectionPut.setStorageTemperatures(id, directoryStorageTemperatures);
    }

    private static void convertDiagnosisAvailable(DirectoryCollectionPut directoryCollectionPut, Collection collection) {
        String id = collection.getId();
        List<String> diagnoses = collection.getDiagnosisAvailable();

        if (diagnoses == null)
            diagnoses = new ArrayList<String>();

        List<String> miriamDiagnoses = diagnoses.stream()
                .map(icd -> FhirToDirectoryAttributeConverter.convertDiagnosis(icd))
                .filter(Objects::nonNull) // Use a method reference to check for non-null values
                .distinct()  // Remove duplicate diagnoses
                .collect(Collectors.toList());

        directoryCollectionPut.setDiagnosisAvailable(id, miriamDiagnoses);
    }
}
