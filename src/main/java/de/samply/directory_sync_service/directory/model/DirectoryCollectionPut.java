package de.samply.directory_sync_service.directory.model;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.google.gson.Gson;
import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.model.BbmriEricId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a data transfer object that maps onto the JSON needed for a PUT request
 * to the Directory API when you want to update one or more collections.
 * <p>
 * It simply extends a Map and adds a single key, "entities". This contains a list
 * of collections. Each collection is also a Map, with keys corresponding to the
 * various attributes needed when updating, such as collection name or ID.
 * <p>
 * The setter methods allow you to set attributes in collections identified by
 * collection ID. If you use an ID that is not yet known, a new collection with this
 * ID will first be created.
 */
public class DirectoryCollectionPut extends HashMap {
  private static final Logger logger = LoggerFactory.getLogger(DirectoryCollectionPut.class);

  public DirectoryCollectionPut() {
        // Initializes the list of entities.
        this.put("entities", new ArrayList<Entity>());
    }

    private final Gson gson = new Gson();

    public void setCountry(String collectionId, String country) {
        getEntity(collectionId).setCountry(country);
    }

    public void setName(String collectionId, String name) {
        getEntity(collectionId).setName(name);
    }

    public void setDescription(String collectionId, String description) {
        getEntity(collectionId).setDescription(description);
    }

    public void setContact(String collectionId, String contact) {
        getEntity(collectionId).setContact(contact);
    }

    public void setBiobank(String collectionId, String biobank) {
        getEntity(collectionId).setBiobank(biobank);
    }

    public void setSize(String collectionId, Integer size) {
        getEntity(collectionId).setSize(size);
    }

    public void setOrderOfMagnitude(String collectionId, Integer size) {
        getEntity(collectionId).setOrderOfMagnitude(size);
    }

    public void setNumberOfDonors(String collectionId, Integer size) {
        getEntity(collectionId).setNumberOfDonors(size);
    }

    public void setOrderOfMagnitudeDonors(String collectionId, Integer size) {
        getEntity(collectionId).setOrderOfMagnitudeDonors(size);
    }

    public void setType(String collectionId, List<String> type) {
        getEntity(collectionId).setType(type);
    }

    public void setDataCategories(String collectionId, List<String> dataCategories) {
        getEntity(collectionId).setDataCategories(dataCategories);
    }

    public void setNetworks(String collectionId, List<String> networks) {
        getEntity(collectionId).setNetworks(networks);
    }

    public void setSex(String collectionId, List<String> sex) {
        getEntity(collectionId).setSex(sex);
    }

    public void setAgeLow(String collectionId, Integer value) {
        getEntity(collectionId).setAgeLow(value);
    }

    public void setAgeHigh(String collectionId, Integer value) {
        getEntity(collectionId).setAgeHigh(value);
    }

    public void setMaterials(String collectionId, List<String> value) {
        getEntity(collectionId).setMaterials(value);
    }

    public void setStorageTemperatures(String collectionId, List<String> value) {
        getEntity(collectionId).setStorageTemperatures(value);
    }

    public void setDiagnosisAvailable(String collectionId, List<String> value) {
        getEntity(collectionId).setDiagnosisAvailable(value);
    }

    public List<String> getCollectionIds() {
        return getEntities().stream()
            .map(entity -> (String) entity.get("id"))
            .collect(Collectors.toList());
    }

    /**
     * Gets the country code for the collections, e.g. "DE".
     * <p>
     * Assumes that all collections will have the same code and simply returns
     * the code of the first collection.
     * <p>
     * If there are no collections, returns null.
     * <p>
     * May throw a null pointer exception.
     * 
     * @return Country code
     */
    public String getCountryCode() {
        logger.info("getCountryCode: ++++++++++++++++++++++++++++++++++++++++++++++++++++++++ entered");
        String countryCode = null;
        try {
            List<Entity> entities = getEntities();
            if (entities == null || entities.size() == 0)
                return null;
            logger.info("getCountryCode: entities.size: " + entities.size());
            Entity entity = entities.get(0);
            logger.info("getCountryCode: entity: " + gson.toJson(entity));
            countryCode = entity.getCountry();
            if (countryCode == null || countryCode.isEmpty()) {
                logger.info("getCountryCode: countryCode from first entity is null or empty");
                String entityId = entity.getId();
                logger.info("getCountryCode: entityId: " + entityId);
                Optional<BbmriEricId> bbmriEricId = BbmriEricId.valueOf(entityId);
                logger.info("getCountryCode: bbmriEricId: " + bbmriEricId);
                countryCode = bbmriEricId.orElse(null).getCountryCode();
            }
        } catch (Exception e) {
            logger.info("getCountryCode: exception: " + Util.traceFromException(e));
            return null;
        }

        logger.info("getCountryCode: countryCode: " + countryCode);
        return countryCode;
    }

    private List<Entity> getEntities() {
        return (List<Entity>) get("entities");
    }

    /**
     * Retrieves or creates an Entity with the specified collection ID.
     * <p>
     * This method searches through the existing entities to find one with a matching
     * ID. If found, the existing entity is returned; otherwise, a new Entity is created
     * with the given collection ID and added to the list of entities.
     *
     * @param collectionId The unique identifier for the Entity.
     * @return The Entity with the specified collection ID. If not found, a new Entity
     *         is created and returned.
     */
    private Entity getEntity(String collectionId) {
        Entity entity = null;

        for (Entity e: getEntities())
            if (e.get("id").equals(collectionId)) {
                entity = e;
                break;
            }

        if (entity == null) {
            entity = new Entity(collectionId);
            this.getEntities().add(entity);
        }

        return entity;
    }

    /**
     * Represents an entity with attributes related to a collection.
     * This class extends HashMap to store key-value pairs.
     */
    public class Entity extends HashMap<String, Object> {
        /**
         * Constructs an Entity with the specified collection ID.
         *
         * @param collectionId The unique identifier for the Entity.
         */
        public Entity(String collectionId) {
            setId(collectionId);
        }

        /**
         * Sets the collection ID for the Entity.
         *
         * @param collectionId The unique identifier for the Entity.
         */
        public void setId(String collectionId) {
            put("id", collectionId);
            setTimestamp();
        }

        public void setTimestamp() {
            LocalDateTime dateTime = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
            String formattedDateTime = dateTime.format(formatter);

            put("timestamp", formattedDateTime);
        }

        /**
         * Retrieves the collection ID of the Entity.
         *
         * @return The collection ID.
         */
        public String getId() {
            return (String) get("id");
        }
        
        public void setCountry(String country) {
            if (country == null || country.isEmpty())
                return;

            put("country", country);
        }

        public String getCountry() {
            return (String) get("country");
        }

        public void setName(String name) {
            if (name == null || name.isEmpty())
                return;

            put("name", name);
        }

        public void setDescription(String description) {
            if (description == null || description.isEmpty())
                return;

            put("description", description);
        }

        public void setContact(String contact) {
            if (contact == null || contact.isEmpty())
                return;

            put("contact", contact);
        }

        public void setBiobank(String biobank) {
            if (biobank == null || biobank.isEmpty())
                return;

            put("biobank", biobank);
        }

        public void setSize(Integer size) {
            if (size == null)
                return;

            put("size", size);
        }

        public void setOrderOfMagnitude(Integer orderOfMagnitude) {
            if (orderOfMagnitude == null)
                return;

            put("order_of_magnitude", orderOfMagnitude);
        }

        public void setNumberOfDonors(Integer size) {
            if (size == null)
                return;

            put("number_of_donors", size);
        }

        public void setOrderOfMagnitudeDonors(Integer orderOfMagnitude) {
            if (orderOfMagnitude == null)
                return;

            put("order_of_magnitude_donors", orderOfMagnitude);
        }

        public void setType(List<String> type) {
            if (type == null)
                type = new ArrayList<String>();
 
            put("type", type);
        }

        public void setDataCategories(List<String> dataCategories) {
            if (dataCategories == null)
                dataCategories = new ArrayList<String>();
 
            put("data_categories", dataCategories);
        }

        public void setNetworks(List<String> networks) {
            if (networks == null)
                networks = new ArrayList<String>();
 
            put("network", networks);
        }

        public void setSex(List<String> sex) {
            if (sex == null)
                sex = new ArrayList<String>();

            put("sex", sex);
        }

        public void setAgeLow(Integer value) {
            put("age_low", value);
        }

        public void setAgeHigh(Integer value) {
            put("age_high", value);
        }

        public void setMaterials(List<String> materials) {
            if (materials == null)
                materials = new ArrayList<String>();
 
            put("materials", materials);
        }

        public void setStorageTemperatures(List<String> storageTemperatures) {
            if (storageTemperatures == null)
                storageTemperatures = new ArrayList<String>();

            put("storage_temperatures", storageTemperatures);
        }

        public void setDiagnosisAvailable(List<String> diagnoses) {
            if (diagnoses == null)
                diagnoses = new ArrayList<String>();

            put("diagnosis_available", diagnoses);
        }

        public List<String> getDiagnosisAvailable() {
            return (List<String>) get("diagnosis_available");
        }
    }
    
    /**
     * Applies corrections to the available diagnoses of each Entity based on a provided map.
     * The method iterates through the list of entities and updates the available diagnoses
     * using the provided map of corrections.
     *
     * @param correctedDiagnoses A map containing diagnosis corrections, where the keys
     *                           represent the original diagnoses and the values represent
     *                           the corrected diagnoses.
     */
    public void applyDiagnosisCorrections(Map<String, String> correctedDiagnoses) {
        for (Entity entity: getEntities()) {
            List<String> directoryDiagnoses = entity.getDiagnosisAvailable().stream()
                .filter(diagnosis -> diagnosis != null && correctedDiagnoses.containsKey(diagnosis) && correctedDiagnoses.get(diagnosis) != null)
                .map(correctedDiagnoses::get)
                .distinct()
                .collect(Collectors.toList());
            entity.setDiagnosisAvailable(directoryDiagnoses);
        }
    }
}
