package com.gaurav.adDeliveryTesting.bootstrap;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class IndexCreator {

    private final JdbcTemplate jdbc;

    public IndexCreator(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void createIndexes() {
        log.info("Ensuring indexes exist on campaign filter tables...");

        jdbc.execute("CREATE INDEX IF NOT EXISTS ix_cc_filter_country  ON campaign_countries (filter_id, country)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS ix_cl_filter_language ON campaign_languages (filter_id, language)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS ix_cd_filter_device   ON campaign_devices   (filter_id, device)");
        jdbc.execute("CREATE INDEX IF NOT EXISTS ix_co_filter_os       ON campaign_oses      (filter_id, os)");

        log.info("Indexes ensured successfully ✅");
    }
}