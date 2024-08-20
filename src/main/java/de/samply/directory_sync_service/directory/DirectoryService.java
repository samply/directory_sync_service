package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.directory.DirectoryApi.CollectionSizeDto;
import de.samply.directory_sync_service.directory.model.BbmriEricId;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.StarModelData;

import io.vavr.control.Either;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.OperationOutcome;

public class DirectoryService {

  private DirectoryApi api;

  public DirectoryService(DirectoryApi api) {
    this.api = Objects.requireNonNull(api);
  }

  public void setApi(DirectoryApi api) {
    this.api = api;
  }

  public List<OperationOutcome> updateCollectionSizes(Map<BbmriEricId, Integer> collectionSizes) {
    return groupCollectionSizesByCountryCode(collectionSizes)
        .entrySet().stream()
        .map(entry -> updateCollectionSizes(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  private Map<String, List<Map.Entry<BbmriEricId, Integer>>> groupCollectionSizesByCountryCode(
      Map<BbmriEricId, Integer> collectionSizes) {
    return collectionSizes.entrySet().stream()
        .collect(Collectors.groupingBy(e -> e.getKey().getCountryCode()));
  }

  public OperationOutcome updateCollectionSizes(String countryCode,
      List<Map.Entry<BbmriEricId, Integer>> collectionSizes) {

    Either<OperationOutcome, Set<BbmriEricId>> result = api.listAllCollectionIds(countryCode);
    if (result.isLeft()) {
      return result.getLeft();
    }

    Set<BbmriEricId> existingCollectionIds = result.get();

    List<CollectionSizeDto> collectionSizeDtos = collectionSizes.stream()
        .filter(e -> existingCollectionIds.contains(e.getKey()))
        .map(e -> new CollectionSizeDto(e.getKey(), e.getValue()))
        .collect(Collectors.toList());

    return api.updateCollectionSizes(countryCode, collectionSizeDtos);
  }

  public List<OperationOutcome> updateEntities(DirectoryCollectionPut directoryCollectionPut) {
    OperationOutcome operationOutcome = api.updateEntities(directoryCollectionPut);
    return Collections.singletonList(operationOutcome);
  }
  
  public Either<OperationOutcome, DirectoryCollectionGet> fetchDirectoryCollectionGetOutcomes(String countryCode, List<String> collectionIds) {
    return(api.fetchCollectionGetOutcomes(countryCode, collectionIds));
  }

  public List<OperationOutcome> updateStarModel(StarModelData starModelInputData) {
    OperationOutcome operationOutcome = api.updateStarModel(starModelInputData);
    return Collections.singletonList(operationOutcome);
  }
}
