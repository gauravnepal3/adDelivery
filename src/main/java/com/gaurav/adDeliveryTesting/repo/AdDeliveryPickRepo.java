// src/main/java/.../repo/AdDeliveryPickRepo.java
package com.gaurav.adDeliveryTesting.repo;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

public interface AdDeliveryPickRepo extends Repository<com.gaurav.adDeliveryTesting.model.Campaign, Integer> {
    @Query(value = """
WITH req AS (
  SELECT
    CAST(:country  AS text) AS country,
    CAST(:language AS text) AS language,
    CAST(:device   AS text) AS device,
    CAST(:os       AS text) AS os,
    CAST(:browser  AS text) AS browser,
    CAST(:iab      AS text) AS iab,
    CAST(:ip       AS text) AS ip,
    CAST(:domain   AS text) AS domain
)
SELECT c.campaign_id
FROM campaign c
JOIN campaign_filters f ON f.campaign_id = c.campaign_id
JOIN req r ON true
-- coarse key
WHERE c.remaining_budget > 0
  AND EXISTS (SELECT 1 FROM campaign_countries  cc WHERE cc.filter_id = f.id AND cc.country  = r.country)
  AND EXISTS (SELECT 1 FROM campaign_languages  cl WHERE cl.filter_id = f.id AND cl.language = r.language)
  AND EXISTS (SELECT 1 FROM campaign_devices    cd WHERE cd.filter_id = f.id AND cd.device   = r.device)
  AND EXISTS (SELECT 1 FROM campaign_oses       co WHERE co.filter_id = f.id AND co.os       = r.os)

-- browser allow: only enforced when request has browser AND campaign has non-empty allow list
  AND (
        r.browser = '' OR
        (EXISTS (SELECT 1 FROM campaign_browsers b WHERE b.filter_id = f.id)
         AND EXISTS (SELECT 1 FROM campaign_browsers b WHERE b.filter_id = f.id AND b.browser = r.browser))
      )

-- iab allow: only enforced when request has iab AND campaign has non-empty allow list
  AND (
        r.iab = '' OR
        (EXISTS (SELECT 1 FROM campaign_iab_category_filter ib WHERE ib.filter_id = f.id)
         AND EXISTS (SELECT 1 FROM campaign_iab_category_filter ib WHERE ib.filter_id = f.id AND ib.iab_category = r.iab))
      )

-- IP allow/block (allow enforced only if list non-empty; block always enforced)
  AND (
        r.ip = '' OR
        NOT EXISTS (SELECT 1 FROM campaign_excluded_ip x WHERE x.filter_id = f.id AND x.excluded_ip = r.ip)
      )
  AND (
        r.ip = '' OR
        NOT EXISTS (SELECT 1 FROM campaign_allowed_ip a WHERE a.filter_id = f.id) OR
        EXISTS (SELECT 1 FROM campaign_allowed_ip a WHERE a.filter_id = f.id AND a.allowed_ip = r.ip)
      )

-- domain allow/block (normalize to lowercase host on the Java side first)
  AND (
        r.domain = '' OR
        NOT EXISTS (SELECT 1 FROM campaign_excluded_domain xd WHERE xd.filter_id = f.id AND lower(xd.excluded_domain) = r.domain)
      )
  AND (
        r.domain = '' OR
        NOT EXISTS (SELECT 1 FROM campaign_allowed_domain ad WHERE ad.filter_id = f.id) OR
        EXISTS (SELECT 1 FROM campaign_allowed_domain ad WHERE ad.filter_id = f.id AND lower(ad.allowed_domain) = r.domain)
      )

ORDER BY c.bidding_rate DESC, c.campaign_id
LIMIT 1
""", nativeQuery = true)
    Integer pickTopOne(
            @Param("country")  String country,
            @Param("language") String language,
            @Param("device")   String device,
            @Param("os")       String os,
            @Param("browser")  String browser,   // pass "" if none
            @Param("iab")      String iab,       // pass "" if none
            @Param("ip")       String ip,        // pass "" if none
            @Param("domain")   String domain     // LOWER(host) or "" if none
    );
}