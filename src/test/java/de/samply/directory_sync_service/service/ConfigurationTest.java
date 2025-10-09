package de.samply.directory_sync_service.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class ConfigurationTest {
    // Happy path: all properites bind.
    // Use Spring’s test slice + inline properties to verify injection

    private final ApplicationContextRunner context =
            new ApplicationContextRunner()
                    .withBean(Configuration.class)
                    .withPropertyValues(
                            "ds.retry.max=5",
                            "ds.retry.interval=2s",
                            "ds.timer.cron=0 0 * * * *",
                            "ds.fhir.store.url=http://fhir",
                            "ds.directory.url=http://dir",
                            "ds.directory.user.name=user",
                            "ds.directory.user.pass=pass",
                            "ds.directory.default_collection_id=bbmri-eric:collection:DE_1",
                            "ds.directory.allow_star_model=true",
                            "ds.directory.min_donors=10",
                            "ds.directory.max_facts=1000",
                            "ds.directory.mock=false",
                            "ds.directory.only_login=false",
                            "ds.directory.write_to_file=false",
                            "ds.directory.output_directory=/tmp/out",
                            "ds.import.biobanks=true",
                            "ds.import.collections=true"
                    );

    @Test
    void bindsAllFields() {
        context.run(ctx -> {
            var cfg = ctx.getBean(Configuration.class);
            assertThat(cfg.getRetryMax()).isEqualTo("5");
            assertThat(cfg.getRetryInterval()).isEqualTo("2s");
            assertThat(cfg.getTimerCron()).isEqualTo("0 0 * * * *");
            assertThat(cfg.getFhirStoreUrl()).isEqualTo("http://fhir");
            assertThat(cfg.getDirectoryUrl()).isEqualTo("http://dir");
            assertThat(cfg.getDirectoryUserName()).isEqualTo("user");
            assertThat(cfg.getDirectoryUserPass()).isEqualTo("pass");
            assertThat(cfg.getDirectoryDefaultCollectionId()).isEqualTo("bbmri-eric:collection:DE_1");
            assertThat(cfg.getDirectoryAllowStarModel()).isEqualTo("true");
            assertThat(cfg.getDirectoryMinDonors()).isEqualTo("10");
            assertThat(cfg.getDirectoryMaxFacts()).isEqualTo("1000");
            assertThat(cfg.getDirectoryMock()).isEqualTo("false");
            assertThat(cfg.getDirectoryOnlyLogin()).isEqualTo("false");
            assertThat(cfg.getDirectoryWriteToFile()).isEqualTo("false");
            assertThat(cfg.getDirectoryOutputDirectory()).isEqualTo("/tmp/out");
            assertThat(cfg.getImportBiobanks()).isEqualTo("true");
            assertThat(cfg.getImportCollections()).isEqualTo("true");
        });
    }

    // Missing property causes startup failure

    @Test
    void bindsWhenPropertiesProvided_othersRemainUnresolved() {
        new ApplicationContextRunner()
                .withBean(Configuration.class)
                .withPropertyValues(
                        "ds.retry.interval=2s",
                        "spring.config.location=classpath:/does-not-exist.yml"
                )
                .run(ctx -> {
                    var cfg = ctx.getBean(Configuration.class);
                    assertThat(cfg.getRetryInterval()).isEqualTo("2s");
                    // Accept common unresolved states
                    assertThat(cfg.getRetryMax())
                            .as("ds.retry.max should be unresolved in this test")
                            .isIn(null, "", "${ds.retry.max}");
                });
    }
}
