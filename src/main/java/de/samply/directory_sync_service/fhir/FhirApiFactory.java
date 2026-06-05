package de.samply.directory_sync_service.fhir;

public class FhirApiFactory {
    public static FhirApi create(String fhirStoreUrl) {
        FhirApi fhirApi = new FhirApi(fhirStoreUrl);
        if (fhirApi.isMiabisOnFhirProfile())
            return new FhirApiMiabisOnFhir(fhirStoreUrl);
        else
            return new FhirApiBbmriDe(fhirStoreUrl);
    }
}
