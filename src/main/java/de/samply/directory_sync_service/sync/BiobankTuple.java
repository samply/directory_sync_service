package de.samply.directory_sync_service.sync;

import de.samply.directory_sync_service.directory.model.Biobank;
import org.hl7.fhir.r4.model.Organization;

import java.util.Objects;
public class BiobankTuple {
    public final Organization fhirBiobank;
    private final Organization fhirBiobankCopy;
    public final Biobank dirBiobank;

    public BiobankTuple(Organization fhirBiobank, Biobank dirBiobank) {
        this.fhirBiobank = Objects.requireNonNull(fhirBiobank);
        this.fhirBiobankCopy = fhirBiobank.copy();
        this.dirBiobank = Objects.requireNonNull(dirBiobank);
    }

    public boolean hasChanged() {
        return !fhirBiobank.equalsDeep(fhirBiobankCopy);
    }
}
