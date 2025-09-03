// src/main/java/com/gaurav/adDeliveryTesting/repo/AdDeliveryNativeRepo.java
package com.gaurav.adDeliveryTesting.repo;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface AdDeliveryNativeRepo extends Repository<com.gaurav.adDeliveryTesting.model.Campaign, Integer> {

    @Query(value = """
        SELECT c.campaign_id
        FROM campaign c
        JOIN campaign_filters f ON f.campaign_id = c.campaign_id
        WHERE c.remaining_budget > 0
          AND EXISTS (SELECT 1 FROM campaign_countries cc
                      WHERE cc.filter_id = f.id AND cc.country = :country)
          AND EXISTS (SELECT 1 FROM campaign_languages cl
                      WHERE cl.filter_id = f.id AND cl.language = :language)
          AND EXISTS (SELECT 1 FROM campaign_devices cd
                      WHERE cd.filter_id = f.id AND cd.device   = :device)
          AND EXISTS (SELECT 1 FROM campaign_oses co
                      WHERE co.filter_id = f.id AND co.os       = :os)
        ORDER BY c.bidding_rate DESC, c.campaign_id
        LIMIT :limit
        """,
            nativeQuery = true)
    List<Integer> findTopIdsForCoarseKey(
            @Param("country") String country,
            @Param("language") String language,
            @Param("device") String device,
            @Param("os") String os,
            @Param("limit") int limit
    );
}