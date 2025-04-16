package de.samply.directory_sync_service.directory;

import java.util.List;

import de.samply.directory_sync_service.directory.model.Collection;
import de.samply.directory_sync_service.directory.model.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.samply.directory_sync_service.Util;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;

/**
 * Takes information from a Collections object and inserts it into
 * a preexisting DirectoryCollectionPut object.
 * <p>
 * Both objects should contain lists of collections with identical IDs.
 */
public class MergeDirectoryCollectionGetToDirectoryCollectionPut {
  private static final Logger logger = LoggerFactory.getLogger(MergeDirectoryCollectionGetToDirectoryCollectionPut.class);

  /**
   * Merges collection information from a Collections object into a
   * DirectoryCollectionPut object.
   * <p>
   * Returns false if there is a problem, e.g. if there are discrepancies between
   * the collection IDs in the two objects.
   * 
   * @param collections
   * @param directoryCollectionPut
   * @return
   */
  public static boolean merge(Collections collections, DirectoryCollectionPut directoryCollectionPut) {
    logger.debug("merge: entered");

    // Only do a merge if we are not mocking
    if (collections.isMockDirectory())
      return true;

    if (collections.size() == 0) {
      logger.warn("merge: collections.size() is zero");
      return true;
    }

    List<String> collectionIds = directoryCollectionPut.getCollectionIds();
    if (collectionIds.size() == 0)
      logger.warn("merge: collectionIds.size() is zero");

    boolean mergeSuccess = false;
    for (String collectionId: collectionIds) {
      logger.debug("merge: processing collectionId: " + collectionId);
      if (merge(collectionId, collections, directoryCollectionPut) == null)
        logger.warn("merge: Problem merging Collections into DirectoryCollectionPut for collectionId: " + collectionId);
      else
        mergeSuccess = true;
    }

    if (!mergeSuccess)
      logger.warn("merge: mergeSuccess is false");

    return mergeSuccess;
  }

  /**
   * Merges data from a Collections object into a DirectoryCollectionPut object.
   *
   * @param collectionId The ID of the collection to merge.
   * @param collections The Collections object containing data to merge.
   * @param directoryCollectionPut The DirectoryCollectionPut object to merge data into.
   * @return The DirectoryCollectionPut object with merged data, or null if an exception occurs.
   */
  private static DirectoryCollectionPut merge(String collectionId, Collections collections, DirectoryCollectionPut directoryCollectionPut) {
    logger.debug("Merging Collections into DirectoryCollectionPut for collectionId: " + collectionId);
    Collection collection = collections.getCollection(collectionId);
    if (collection == null) {
      logger.warn("merge: collection is null for collectionId: " + collectionId);
      return null;
    }
    try {
      directoryCollectionPut.setName(collectionId, collection.getName());
      directoryCollectionPut.setDescription(collectionId, collection.getDescription());
      directoryCollectionPut.setContact(collectionId, collection.getContact());
      directoryCollectionPut.setCountry(collectionId, collection.getCountry());
      directoryCollectionPut.setBiobank(collectionId, collection.getBiobank());
      directoryCollectionPut.setType(collectionId, collection.getType());
      directoryCollectionPut.setDataCategories(collectionId, collection.getDataCategories());
      directoryCollectionPut.setNetworks(collectionId, collection.getNetwork());
    } catch(Exception e) {
      logger.error("Problem merging Collections into DirectoryCollectionPut. " + Util.traceFromException(e));
      return null;
    }

    return directoryCollectionPut;
  }
}
