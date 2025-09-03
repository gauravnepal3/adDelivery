// src/main/java/com/gaurav/adDeliveryTesting/service/LazyIndexer.java
package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryNativeRepo;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import org.redisson.api.BatchOptions;
import org.redisson.api.RBatch;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class LazyIndexer {

    private final AdDeliveryNativeRepo nativeRepo;
    private final AdDeliveryRepo repo;
    private final RedissonClient redisson;
    private final CampaignMetadataCache meta;

    // tuneables
    private static final int TOP_LIMIT_PER_KEY = 100;           // cap per coarse key
    private static final Duration KEY_TTL = Duration.ofHours(6); // optional TTL

    public LazyIndexer(AdDeliveryNativeRepo nativeRepo,
                       AdDeliveryRepo repo,
                       RedissonClient redisson,
                       CampaignMetadataCache meta) {
        this.nativeRepo = nativeRepo;
        this.repo = repo;
        this.redisson = redisson;
        this.meta = meta;
    }

    /** Ensure zset + per-campaign sets exist for a coarse key; idempotent and memory-safe. */
    public void ensureIndexed(String country, String language, String device, String os) {
        final String zsetKey = CampaignCacheService.zsetKey(country, language, device, os);

        // Cheap existence check
        Long size = redisson.getScoredSortedSet(zsetKey, StringCodec.INSTANCE).sizeInMemory();
        if (size != null && size > 0) return;

        // Lock per coarse key so only one thread builds it
        RLock lock = redisson.getLock("lock:index:" + zsetKey);
        boolean locked = false;
        try {
            locked = lock.tryLock(0, 30, TimeUnit.SECONDS);
            if (!locked) return;

            // Recheck under lock
            size = redisson.getScoredSortedSet(zsetKey, StringCodec.INSTANCE).sizeInMemory();
            if (size != null && size > 0) return;

            // 1) Get top IDs for this key
            List<Integer> ids = nativeRepo.findTopIdsForCoarseKey(country, language, device, os, TOP_LIMIT_PER_KEY);
            if (ids.isEmpty()) return;

            // 2) Load entities + filters with an entity graph
            List<Campaign> campaigns = repo.findBatchWithFilters(ids);

            // 3) Warm metadata cache from entities (no extra DB)
            for (Campaign c : campaigns) {
                meta.put(c);
            }

            // 4) Write everything via Redisson batch without collecting results
            RBatch batch = redisson.createBatch(BatchOptions.defaults()
                    .skipResult()
                    .responseTimeout(2, TimeUnit.SECONDS)
                    .retryAttempts(1)
                    .retryInterval(100, TimeUnit.MILLISECONDS));

            for (Campaign c : campaigns) {
                CampaignCacheService.writeCampaignToBatch(batch, c);
            }

            // Optional: TTL on this coarse key (helps auto-evict cold keys)
            batch.getKeys().expireAsync(zsetKey, KEY_TTL.toSeconds(), java.util.concurrent.TimeUnit.SECONDS);

            batch.execute(); // fire-and-forget, no giant List in heap
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            if (locked) {
                try { lock.unlock(); } catch (Exception ignored) {}
            }
        }
    }
}