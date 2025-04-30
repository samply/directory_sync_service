package de.samply.directory_sync_service.converter;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.model.Collections;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Copy information from a Collections object to a DirectoryCollectionPut object.
 */
public class ConvertCollectionsToDirectoryCollectionPut {
    private static final Logger logger = LoggerFactory.getLogger(ConvertCollectionsToDirectoryCollectionPut.class);

    /**
     * Converts a Collections object into a DirectoryCollectionPut object.
     *
     */
    public static DirectoryCollectionPut convert(Collections collections) {
        DirectoryCollectionPut directoryCollectionPut = new DirectoryCollectionPut();

        for (Collection collection : collections.getCollections())
            if (convert(collection, directoryCollectionPut) == null) {
                directoryCollectionPut = null;
                break;
            }

        return directoryCollectionPut;
    }

    /**
     * Takes a single Collection object, converts it, and then adds it to the DirectoryCollectionPut object.
     *
     */
    private static DirectoryCollectionPut convert(Collection collection, DirectoryCollectionPut directoryCollectionPut) {
        try {

            // Attributes pulled from the Directory
            directoryCollectionPut.setBiobank(collection.getId(), collection.getBiobank());
            directoryCollectionPut.setContact(collection.getId(), collection.getContact());
            directoryCollectionPut.setCountry(collection.getId(), collection.getCountry());
            directoryCollectionPut.setDataCategories(collection.getId(), collection.getDataCategories());
            directoryCollectionPut.setDescription(collection.getId(), collection.getDescription());
            directoryCollectionPut.setHead(collection.getId(), collection.getHead());
            directoryCollectionPut.setLocation(collection.getId(), collection.getLocation());
            directoryCollectionPut.setName(collection.getId(), collection.getName());
            directoryCollectionPut.setNetworks(collection.getId(), collection.getNetwork());
            directoryCollectionPut.setType(collection.getId(), collection.getType());
            directoryCollectionPut.setUrl(collection.getId(), collection.getUrl());

            // Attributes calculated based on FHIR store content
            convertAgeLow(directoryCollectionPut, collection);
            convertAgeHigh(directoryCollectionPut, collection);
            convertDiagnosisAvailable(directoryCollectionPut, collection);
            convertMaterials(directoryCollectionPut, collection);
            convertNumberOfDonors(directoryCollectionPut, collection);
            convertSex(directoryCollectionPut, collection);
            convertSize(directoryCollectionPut, collection);
            convertStorageTemperatures(directoryCollectionPut, collection);
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
