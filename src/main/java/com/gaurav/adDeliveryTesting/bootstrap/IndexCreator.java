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

        jdbc.execute("-- campaign\n" +
                "CREATE INDEX IF NOT EXISTS ix_campaign_bid_active\n" +
                "  ON campaign (bidding_rate DESC)\n" +
                "  WHERE remaining_budget > 0;\n" +
                "CREATE INDEX IF NOT EXISTS ix_campaign_rem_bid\n" +
                "  ON campaign (remaining_budget, bidding_rate DESC);\n" +
                "\n" +
                "-- campaign_filters\n" +
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_cf_id\n" +
                "  ON campaign_filters (id);\n" +
                "CREATE INDEX IF NOT EXISTS ix_cf_campaign\n" +
                "  ON campaign_filters (campaign_id);\n" +
                "\n" +
                "-- countries / languages / device / os (value → filter_id + existence)\n" +
                "CREATE INDEX IF NOT EXISTS ix_cc_country_filter ON campaign_countries(country, filter_id);\n" +
                "CREATE INDEX IF NOT EXISTS ix_cc_filter        ON campaign_countries(filter_id);\n" +
                "\n" +
                "CREATE INDEX IF NOT EXISTS ix_cl_lang_filter   ON campaign_languages(language, filter_id);\n" +
                "CREATE INDEX IF NOT EXISTS ix_cl_filter        ON campaign_languages(filter_id);\n" +
                "\n" +
                "CREATE INDEX IF NOT EXISTS ix_cd_device_filter ON campaign_devices(device, filter_id);\n" +
                "CREATE INDEX IF NOT EXISTS ix_cd_filter        ON campaign_devices(filter_id);\n" +
                "\n" +
                "CREATE INDEX IF NOT EXISTS ix_co_os_filter     ON campaign_oses(os, filter_id);\n" +
                "CREATE INDEX IF NOT EXISTS ix_co_filter        ON campaign_oses(filter_id);\n" +
                "\n" +
                "-- browser / iab (need both existence + value)\n" +
                "CREATE INDEX IF NOT EXISTS ix_cb_filter_browser ON campaign_browsers(filter_id, browser);\n" +
                "CREATE INDEX IF NOT EXISTS ix_cb_filter         ON campaign_browsers(filter_id);\n" +
                "\n" +
                "CREATE INDEX IF NOT EXISTS ix_ci_filter_iab     ON campaign_iab_category_filter(filter_id, iab_category);\n" +
                "CREATE INDEX IF NOT EXISTS ix_ci_filter         ON campaign_iab_category_filter(filter_id);\n" +
                "\n" +
                "-- domain/ip allow + block (existence + value)\n" +
                "CREATE INDEX IF NOT EXISTS ix_adom_filter       ON campaign_allowed_domain(filter_id);\n" +
                "CREATE INDEX IF NOT EXISTS ix_adom_val_filter   ON campaign_allowed_domain(allowed_domain, filter_id);\n" +
                "\n" +
                "CREATE INDEX IF NOT EXISTS ix_xdom_filter       ON campaign_excluded_domain(filter_id);\n" +
                "CREATE INDEX IF NOT EXISTS ix_xdom_val_filter   ON campaign_excluded_domain(excluded_domain, filter_id);\n" +
                "\n" +
                "CREATE INDEX IF NOT EXISTS ix_aip_filter        ON campaign_allowed_ip(filter_id);\n" +
                "CREATE INDEX IF NOT EXISTS ix_aip_val_filter    ON campaign_allowed_ip(allowed_ip, filter_id);\n" +
                "\n" +
                "CREATE INDEX IF NOT EXISTS ix_xip_filter        ON campaign_excluded_ip(filter_id);\n" +
                "CREATE INDEX IF NOT EXISTS ix_xip_val_filter    ON campaign_excluded_ip(excluded_ip, filter_id);" );

        log.info("Indexes ensured successfully ✅");
    }
}