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
    List<String> collectionIds = directoryCollectionPut.getCollectionIds();
    // Only do a merge if we are not mocking
    if (!directoryCollectionGet.isMockDirectory())
      for (String collectionId: collectionIds)
          if (merge(collectionId, directoryCollectionGet, directoryCollectionPut) == null)
            return false;
    
    return true;
  }

  private static DirectoryCollectionPut merge(String collectionId, DirectoryCollectionGet directoryCollectionGet, DirectoryCollectionPut directoryCollectionPut) {
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
