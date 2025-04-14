package de.samply.directory_sync_service.directory;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;

/**
 * Takes information from a DirectoryCollectionGet object and inserts it into
 * a preexisting DirectoryCollectionPut object.
 * <p>
 * Both objects should contain lists of collections with identical IDs.
 */
public class MergeDirectoryCollectionGetToDirectoryCollectionPut {
  private static final Logger logger = LoggerFactory.getLogger(MergeDirectoryCollectionGetToDirectoryCollectionPut.class);

  /**
   * Merges collection information from a DirectoryCollectionGet object into a
   * DirectoryCollectionPut object.
   * <p>
   * Returns false if there is a problem, e.g. if there are discrepancies between
   * the collection IDs in the two objects.
   * 
   * @param directoryCollectionGet
   * @param directoryCollectionPut
   * @return
   */
  public static boolean merge(DirectoryCollectionGet directoryCollectionGet, DirectoryCollectionPut directoryCollectionPut) {
    logger.debug("merge: entered");

    // Only do a merge if we are not mocking
    if (directoryCollectionGet.isMockDirectory())
      return true;

    if (directoryCollectionGet.size() == 0) {
      logger.warn("merge: directoryCollectionGet.size() is zero");
      return true;
    }

    List<String> collectionIds = directoryCollectionPut.getCollectionIds();
    if (collectionIds.size() == 0)
      logger.warn("merge: collectionIds.size() is zero");

    boolean mergeSuccess = false;
    for (String collectionId: collectionIds)
        if (merge(collectionId, directoryCollectionGet, directoryCollectionPut) == null)
          logger.warn("merge: Problem merging DirectoryCollectionGet into DirectoryCollectionPut for collectionId: " + collectionId);
        else
          mergeSuccess = true;

    if (!mergeSuccess)
      logger.warn("merge: mergeSuccess is false");

    return mergeSuccess;
  }

  /**
   * Merges data from a DirectoryCollectionGet object into a DirectoryCollectionPut object.
   *
   * @param collectionId The ID of the collection to merge.
   * @param directoryCollectionGet The DirectoryCollectionGet object containing data to merge.
   * @param directoryCollectionPut The DirectoryCollectionPut object to merge data into.
   * @return The DirectoryCollectionPut object with merged data, or null if an exception occurs.
   */
  private static DirectoryCollectionPut merge(String collectionId, DirectoryCollectionGet directoryCollectionGet, DirectoryCollectionPut directoryCollectionPut) {
    logger.debug("Merging DirectoryCollectionGet into DirectoryCollectionPut for collectionId: " + collectionId);
    try {
      String name = directoryCollectionGet.getName(collectionId);
      String description = directoryCollectionGet.getDescription(collectionId);
      String contact = directoryCollectionGet.getContactId(collectionId);
      String country = directoryCollectionGet.getCountryId(collectionId);
      String biobank = directoryCollectionGet.getBiobankId(collectionId);
      List<String> type = directoryCollectionGet.getTypeIds(collectionId);
      List<String> dataCategories = directoryCollectionGet.getDataCategoryIds(collectionId);
      List<String> networks = directoryCollectionGet.getNetworkIds(collectionId);
      if (name == null && description == null && contact == null && country == null && biobank == null && type == null && dataCategories == null && networks == null)
          logger.warn("merge: name, description, contact, country, biobank, type, dataCategories, and networks is null for collectionId: " + collectionId);
      directoryCollectionPut.setName(collectionId, name);
      directoryCollectionPut.setDescription(collectionId, description);
      directoryCollectionPut.setContact(collectionId, contact);
      directoryCollectionPut.setCountry(collectionId, country);
      directoryCollectionPut.setBiobank(collectionId, biobank);
      directoryCollectionPut.setType(collectionId, type);
      directoryCollectionPut.setDataCategories(collectionId, dataCategories);
      directoryCollectionPut.setNetworks(collectionId, networks);
    } catch(Exception e) {
      logger.error("Problem merging DirectoryCollectionGet into DirectoryCollectionPut. " + Util.traceFromException(e));
      return null;
    }

    return directoryCollectionPut;
  }
}
