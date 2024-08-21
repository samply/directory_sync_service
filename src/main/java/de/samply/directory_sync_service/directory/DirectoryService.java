package de.samply.directory_sync_service.directory;

import de.samply.directory_sync_service.directory.model.DirectoryCollectionGet;
import de.samply.directory_sync_service.directory.model.DirectoryCollectionPut;
import de.samply.directory_sync_service.model.StarModelData;
import io.vavr.control.Either;
import org.hl7.fhir.r4.model.OperationOutcome;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DirectoryService {
  private DirectoryApi api;

  public DirectoryService(DirectoryApi api) {
    this.api = Objects.requireNonNull(api);
  }

  public void setApi(DirectoryApi api) {
    this.api = api;
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
