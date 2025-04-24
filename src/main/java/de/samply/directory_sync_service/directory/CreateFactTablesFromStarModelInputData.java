package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.model.StarModelInput;
import de.samply.directory_sync_service.model.FactTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for creating fact tables from Star Model input data.
 * <p>
 * The implementation includes methods for creating fact tables from input data, grouping specimens
 * by collection, and applying various data transformations.
 * </p>
 */
public class CreateFactTablesFromStarModelInputData {
    protected static final Logger logger = LoggerFactory.getLogger(CreateFactTablesFromStarModelInputData.class);

    /**
     * Creates fact tables for each collection in the provided Star Model input data.
     * Fact tables are generated based on input rows and specified criteria such as minimum donors.
     *
     * @param starModelInput The Star Model input data containing information for fact table creation.
     * @param maxFacts
     *
     * @throws NullPointerException if starModelInputData is null.
     */
    public static FactTable createFactTables(StarModelInput starModelInput, int maxFacts) {
        FactTable factTable = new FactTable();
        for (String collectionId: starModelInput.getInputCollectionIds()) {
            List<Map<String, String>> factTableFinal = createFactTableFinal(collectionId,
                    starModelInput.getMinDonors(),
                    maxFacts,
                    starModelInput.getInputRowsAsStringMaps(collectionId));
            logger.debug("createFactTables: collectionId: " + collectionId + ", factTableFinal.size() " + factTableFinal.size());
            if (factTableFinal.size() == 0)
                logger.warn("createFactTables: factTableFinal.size() is zero");
            factTable.addFactTable(factTableFinal);
        }

        return factTable;
    }

    /**
     * Creates a final fact table for a specific collection based on input rows, minimum donors, and data transformations.
     * <p>
     * This code was translated from Petr Holub's R script "CRC-fact-sheet.R".
     *
     * @param collectionId The identifier for the collection for which to create the fact table.
     * @param minDonors The minimum number of donors required for a fact to be included in the table.
     * @param maxFacts
     * @param patientSamples The list of input rows representing patient samples for the collection.
     * @return The final fact table as a list of maps containing key-value pairs.
     */
    private static List<Map<String, String>> createFactTableFinal(String collectionId, int minDonors, int maxFacts, List<Map<String, String>> patientSamples) {
        logger.debug("createFactTableFinal: patientSamples.size() " + patientSamples.size());

        // Transform patient sample data by standardizing fields (e.g., age range, sex, sample material)
        // and adding collection-specific metadata. This returns a list where each map represents a transformed row.
        List<Map<String, String>> patientSamplesFacts = transformData(patientSamples, collectionId);

        logger.debug("createFactTableFinal: patientSamplesFacts.size() " + patientSamplesFacts.size());
        // Print out first and last elements of patientSamplesFacts as JSON
        if (patientSamplesFacts.size() > 0) {
            logger.debug("createFactTableFinal: patientSamplesFacts #0: " + Util.jsonStringFomObject(patientSamplesFacts.get(0)));
            int lastFactNum = patientSamplesFacts.size()-1;
            if (lastFactNum > 0)
                logger.debug("createFactTableFinal: patientSamplesFacts #" + lastFactNum + ": " +  Util.jsonStringFomObject(patientSamplesFacts.get(patientSamplesFacts.size() - 1)));
        }

        // Generate an intermediate fact table by grouping the transformed data and calculating
        // unique counts for patients and samples based on attributes like sex, disease, and age range.
        // The result is a map where keys are attribute combinations and values are maps of counts.
        Map<String, Map<String, Long>> factTable = generateFactTable(patientSamplesFacts);

        logger.debug("createFactTableFinal: factTable.size() " + factTable.size());

        // Finalize the fact table by filtering out entries that do not meet the minimum donor requirement
        // and truncating the list if it exceeds the specified maximum number of facts. Additional metadata
        // and unique identifiers are added to each fact entry.
        List<Map<String, String>> factTableFinal = generateFactTableFinal(factTable, collectionId, minDonors, maxFacts);

        logger.debug("createFactTableFinal: factTableFinal.size() " + factTableFinal.size());

        return factTableFinal;
    }

    /**
     * Transforms data by adding calculated or transformed fields to each record.
     * Specifically, it adds an age range based on age, standardizes the sample material and sex,
     * adds a collection identifier, and sets the last update date to today's date.
     *
     * @param data List of maps where each map represents a row of data about a patient or sample.
     * @param collectionId BBMRI ID for the relevant sample collection.
     * @return List of transformed maps with additional or modified fields.
     */
    private static List<Map<String, String>> transformData(List<Map<String, String>> data, String collectionId) {
        return data.stream()
                .map(row -> {
                    String ageStr = row.get("age_at_primary_diagnosis");
                    row.put("age_range", transformAgeRange(ageStr));
                    row.put("sample_material", transformSampleMaterial(row.get("sample_material")));
                    row.put("sex", transformSex(row.get("sex")));
                    row.put("hist_loc", row.get("hist_loc"));
                    row.put("last_update", LocalDate.now().toString());
                    row.put("collection", collectionId);
                    return row;
                })
                .collect(Collectors.toList());
    }

    /**
     * Generates a fact table by grouping patient data and calculating unique patient and sample counts
     * based on specified criteria (sex, disease, age range, sample material).
     *
     * @param data List of maps representing individual patient/sample data rows.
     * @return A fact table where the keys are a concatenated string of attributes (e.g., "sex|disease|age_range|sample_material"),
     *         and the values are maps containing aggregated counts of patients and samples.
     */
    private static Map<String, Map<String, Long>> generateFactTable(List<Map<String, String>> data) {

        return data.stream()
                .collect(Collectors.groupingBy(
                        row -> row.get("sex") + "|" + row.get("hist_loc") + "|" + row.get("age_range") + "|" + row.get("sample_material"),
                        Collectors.collectingAndThen(Collectors.toList(), records -> {
                            long patientCount = records.stream().map(r -> r.get("id")).distinct().count();
                            long sampleCount = records.size();
                            Map<String, Long> counts = new HashMap<>();
                            counts.put("number_of_donors", patientCount);
                            counts.put("number_of_samples", sampleCount);
                            return counts;
                        })
                ));
    }

    /**
     * Generates a finalized fact table from an aggregated fact table, adding metadata and truncating if necessary.
     *
     * @param factTable A map containing aggregated fact entries (grouped and counted).
     * @param collectionId The collection identifier relevant to the data.
     * @param minDonors The minimum number of donors required to include a fact in the final table.
     * @param maxFacts The maximum number of facts to include in the final table (if >= 0).
     * @return A list of maps representing the finalized fact table, including additional metadata fields.
     */
    private static List<Map<String, String>> generateFactTableFinal(Map<String, Map<String, Long>> factTable, String collectionId, int minDonors, int maxFacts) {
        List<Map<String, String>> factTableFinal = new ArrayList<>();
        int factCount = 0;
        String factIdStub = createFactIdStub(collectionId);
        boolean isDiseaseNull = false;
        boolean isDiseaseEmpty = false;
        for (Map.Entry<String, Map<String, Long>> entry : factTable.entrySet()) {
            // Skip entries that don't meet the minimum donor requirement
            if (entry.getValue().get("number_of_donors") < minDonors) {
                continue;
            }

            // Split the key to get the various fact attributes
            List<String> keyParts = Arrays.asList(entry.getKey().split("\\|"));
            String sampleMaterial = keyParts.get(3);
            for (int i=4; i<keyParts.size(); i++)
                sampleMaterial = sampleMaterial + "_" + keyParts.get(i);

            // Create the fact row
            Map<String, String> row = new HashMap<>();
            row.put("sex", keyParts.get(0));
            row.put("disease", keyParts.get(1));
            row.put("age_range", keyParts.get(2));
            row.put("sample_type", sampleMaterial);
            row.put("number_of_donors", entry.getValue().get("number_of_donors").toString());
            row.put("number_of_samples", entry.getValue().get("number_of_samples").toString());
            row.put("id", factIdStub+ Math.abs(entry.getKey().hashCode())); // Add hash code to make ID unique
            row.put("collection", collectionId);
            row.put("last_update", LocalDate.now().toString());

            if (!isDiseaseNull && row.get("disease") == null) {
                isDiseaseNull = true;
                logger.info("generateFactTableFinal: Disease is null for row: " + Util.jsonStringFomObject(row));
            }
            if (!isDiseaseEmpty && row.get("disease").isEmpty()) {
                isDiseaseEmpty = true;
                logger.info("generateFactTableFinal: Disease is empty for row: " + Util.jsonStringFomObject(row));
            }

            // Add the row to the final list
            factTableFinal.add(row);

            // Stop if we've reached the maximum allowed number of facts
            factCount++;
            if (maxFacts >= 0 && factCount >= maxFacts)
                break;
        }

        return factTableFinal;
    }

    /**
     * Generates a standardized ID prefix for each fact entry in the fact table.
     *
     * @param collectionId The unique identifier for the data collection.
     * @return A standardized prefix for fact IDs based on the collection identifier.
     */
    private static String createFactIdStub(String collectionId) {
        return "bbmri-eric:factID:" // All fact IDs must start with this (mandatory).
                // Snip "bbmri-eric:ID:" from collection ID
                + collectionId.substring(14)
                + ":";
    }

    /**
     * Cuts the age into bins and returns the corresponding age range.
     *
     * @param ageStr The age to be categorized into bins.
     * @return The age range as a string.
     */
    private static String transformAgeRange(String ageStr) {
        int age = (ageStr != null && !ageStr.isEmpty()) ? Integer.parseInt(ageStr) : -1;
        if (age < 0) {
            return "Unknown";
        } else if (age == 0) {
            return "Newborn";
        } else if (age < 2) {
            return "Infant";
        } else if (age < 13) {
            return "Child";
        } else if (age < 18) {
            return "Adolescent";
        } else if (age < 25) {
            return "Young Adult";
        } else if (age < 45) {
            return "Adult";
        } else if (age < 65) {
            return "Middle-aged";
        } else if (age < 80) {
            return "Aged (65-79 years)";
        } else {
            return "Aged (>80 years)";
        }
    }

    /**
     * Converts the sample material names to the Directory standard.
     *
     * @param material The raw sample material type.
     * @return The standardized name for the sample material.
     */
    private static String transformSampleMaterial(String material) {
        if (material == null) return "";
        switch (material) {
            case "FFPE":
                return "TISSUE_PARAFFIN_EMBEDDED";
            case "Cryopreservation":
                return "TISSUE_FROZEN";
            case "Other":
                return "OTHER";
            default:
                return material;
        }
    }

    /**
     * Converts sex designations to the Directory standard.
     *
     * @param sex The raw sex designation.
     * @return The standardized sex designation.
     */
    private static String transformSex(String sex) {
        if (sex == null) return "";
        return sex.equalsIgnoreCase("female") ? "FEMALE" : sex.equalsIgnoreCase("male") ? "MALE" : sex;
    }
}
