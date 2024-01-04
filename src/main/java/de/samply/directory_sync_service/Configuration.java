package de.samply.directory_sync_service;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Environment configuration parameters. */
@Data
@Component
public class Configuration {
    @Value("${ds.retry.max}")
    private String retryMax;

    @Value("${ds.retry.interval}")
    private String retryInterval;

    @Value("${ds.timer.cron}")
    private String timerCron;

    @Value("${ds.fhir.store.url}")
    private String fhirStoreUrl;

    @Value("${ds.directory.url}")
    private String directoryUrl;

    @Value("${ds.directory.user.name}")
    private String directoryUserName;

    @Value("${ds.directory.user.pass}")
    private String directoryUserPass;

    @Value("${ds.directory.default_collection_id}")
    private String directoryDefaultCollectionId;

    @Value("${ds.directory.allow_star_model}")
    private String directoryAllowStarModel;

    @Value("${ds.directory.min_donors}")
    private String directoryMinDonors;
}
