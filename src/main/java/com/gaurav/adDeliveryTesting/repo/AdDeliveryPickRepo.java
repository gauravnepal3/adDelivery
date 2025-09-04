// src/main/java/.../repo/AdDeliveryPickRepo.java
package com.gaurav.adDeliveryTesting.repo;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

public interface AdDeliveryPickRepo extends Repository<com.gaurav.adDeliveryTesting.model.Campaign, Integer> {
    @Query(value = """
        SELECT c.campaign_id
        FROM campaign c
        WHERE c.remaining_budget > 0
          AND EXISTS (
              SELECT 1
              FROM campaign_filters f
              WHERE f.campaign_id = c.campaign_id
                AND EXISTS (SELECT 1 FROM campaign_countries  cc WHERE cc.filter_id = f.id AND cc.country  = :country)
                AND EXISTS (SELECT 1 FROM campaign_languages  cl WHERE cl.filter_id = f.id AND cl.language = :language)
                AND EXISTS (SELECT 1 FROM campaign_devices    cd WHERE cd.filter_id = f.id AND cd.device   = :device)
                AND EXISTS (SELECT 1 FROM campaign_oses       co WHERE co.filter_id = f.id AND co.os       = :os)

                -- browser: enforced if list exists (match your "working" SQL)
                AND (
                  NOT EXISTS (SELECT 1 FROM campaign_browsers b WHERE b.filter_id = f.id)
                  OR EXISTS (SELECT 1 FROM campaign_browsers b WHERE b.filter_id = f.id AND b.browser = :browser)
                )

                -- iab: enforced if list exists
                AND (
                  NOT EXISTS (SELECT 1 FROM campaign_iab_category_filter ib WHERE ib.filter_id = f.id)
                  OR EXISTS (SELECT 1 FROM campaign_iab_category_filter ib WHERE ib.filter_id = f.id AND ib.iab_category = :iab)
                )

                -- IP block then allow
                AND NOT EXISTS (SELECT 1 FROM campaign_excluded_ip x WHERE x.filter_id = f.id AND x.excluded_ip = :ip)
                AND (
                  NOT EXISTS (SELECT 1 FROM campaign_allowed_ip a WHERE a.filter_id = f.id)
                  OR EXISTS (SELECT 1 FROM campaign_allowed_ip a WHERE a.filter_id = f.id AND a.allowed_ip = :ip)
                )

                -- domain block then allow (store & pass lowercased)
                AND NOT EXISTS (SELECT 1 FROM campaign_excluded_domain xd WHERE xd.filter_id = f.id AND xd.excluded_domain = :domain)
                AND (
                  NOT EXISTS (SELECT 1 FROM campaign_allowed_domain ad WHERE ad.filter_id = f.id)
                  OR EXISTS (SELECT 1 FROM campaign_allowed_domain ad WHERE ad.filter_id = f.id AND ad.allowed_domain = :domain)
                )
          )
        ORDER BY c.bidding_rate DESC, c.campaign_id
        LIMIT 1
        """, nativeQuery = true)
    Integer pickTopOne(
            @Param("country")  String country,
            @Param("language") String language,
            @Param("device")   String device,
            @Param("os")       String os,
            @Param("browser")  String browser,   // pass a real value; see note below
            @Param("iab")      String iab,       // pass a real value
            @Param("ip")       String ip,
            @Param("domain")   String domain     // lowercased host
    );
}