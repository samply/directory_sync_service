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
    for (String collectionId: collectionIds) {
      logger.debug("merge: processing collectionId: " + collectionId);
      if (merge(collectionId, directoryCollectionGet, directoryCollectionPut) == null)
        logger.warn("merge: Problem merging DirectoryCollectionGet into DirectoryCollectionPut for collectionId: " + collectionId);
      else
        mergeSuccess = true;
    }

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
      directoryCollectionPut.setName(collectionId, directoryCollectionGet.getName(collectionId));
      directoryCollectionPut.setDescription(collectionId, directoryCollectionGet.getDescription(collectionId));
      directoryCollectionPut.setContact(collectionId, directoryCollectionGet.getContactId(collectionId));
      directoryCollectionPut.setCountry(collectionId, directoryCollectionGet.getCountryId(collectionId));
      directoryCollectionPut.setBiobank(collectionId, directoryCollectionGet.getBiobankId(collectionId));
      directoryCollectionPut.setType(collectionId, directoryCollectionGet.getTypeIds(collectionId));
      directoryCollectionPut.setDataCategories(collectionId, directoryCollectionGet.getDataCategoryIds(collectionId));
      directoryCollectionPut.setNetworks(collectionId, directoryCollectionGet.getNetworkIds(collectionId));
    } catch(Exception e) {
      logger.error("Problem merging DirectoryCollectionGet into DirectoryCollectionPut. " + Util.traceFromException(e));
      return null;
    }

    return directoryCollectionPut;
  }
}
