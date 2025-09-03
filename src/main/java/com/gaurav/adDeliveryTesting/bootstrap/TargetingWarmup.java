package com.gaurav.adDeliveryTesting.bootstrap;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.service.CampaignCacheService;
import com.gaurav.adDeliveryTesting.service.CampaignMetadataCache;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.boot.context.event.ApplicationReadyEvent;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class TargetingWarmup {

    private final AdDeliveryRepo repo;
    private final RedissonClient redisson;
    private final CampaignCacheService cache;
    private final CampaignMetadataCache meta;

    /**
     * Auto-warm on startup in small batches (prevents OOM and connection leaks).
     * You can also trigger manually via the controller endpoint below.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void warmOnStartup() {
        try {
            warm(); // safe to call; it is itself @Transactional(readOnly = true)
        } catch (Exception e) {
            log.error("Warmup on startup failed", e);
        }
    }

    /**
     * Warm all Redis structures in batches.
     */
    @Transactional(readOnly = true)
    public void warm() {
        var codec = StringCodec.INSTANCE;

        int page = 0;
        final int batchSize = 1000;

        int total = 0;
        while (true) {
            var pageReq = PageRequest.of(page, batchSize);
            var pageData = repo.findAllWithFilters(pageReq);
            List<Campaign> batch = pageData.getContent();
            if (batch.isEmpty()) break;

            // Warm metadata cache for this batch
            meta.warmAll(batch);

            // Write budgets + allow/block + zset memberships for this batch
            for (Campaign c : batch) {
                String idStr = Integer.toString(c.getCampaignId());

                // budgets
                long remCents = MoneyUtils.toCents(c.getRemainingBudget());
                redisson.getMap("campaign:budget:" + idStr, codec).fastPut("remaining", Long.toString(remCents));
                redisson.getBucket("campaign:delta:" + idStr, codec).set("0");

                // allow/block + zset membership
                cache.indexCampaign(c);
                total++;
            }

            log.info("Warmup processed batch {} ({} campaigns)", page, batch.size());
            if (!pageData.hasNext()) break;
            page++;
        }

        log.info("Warmup finished. Total campaigns indexed: {}", total);
    }
}