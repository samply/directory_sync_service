package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.model.Collection;
import org.hl7.fhir.r4.model.Organization;

import java.util.Objects;

public class CollectionTuple {
    public final Organization fhirCollection;
    private final Organization fhirCollectionCopy;
    public final Collection dirCollection;

    public CollectionTuple(Organization fhirCollection, Collection dirCollection) {
        this.fhirCollection = Objects.requireNonNull(fhirCollection);
        this.fhirCollectionCopy = fhirCollection.copy();
        this.dirCollection = Objects.requireNonNull(dirCollection);
    }

    public boolean hasChanged() {
        return !fhirCollection.equalsDeep(fhirCollectionCopy);
    }
}
