// src/main/java/com/gaurav/adDeliveryTesting/service/LazyIndexer.java
package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryNativeRepo;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import java.time.Duration;
import java.util.List;

@Service
public class LazyIndexer {

    private final AdDeliveryNativeRepo nativeRepo;
    private final AdDeliveryRepo repo;
    private final StringRedisTemplate redis;
    private final RedissonClient redisson;
    private final CampaignMetadataCache meta;
    private final CampaignCacheService cache;

    // tuneables
    private static final int TOP_LIMIT_PER_KEY = 500;      // cap loaded campaigns per coarse key
    private static final Duration KEY_TTL = Duration.ofHours(6); // optional TTL to auto-evict cold keys

    public LazyIndexer(AdDeliveryNativeRepo nativeRepo,
                       AdDeliveryRepo repo,
                       StringRedisTemplate redis,
                       RedissonClient redisson,
                       CampaignMetadataCache meta,
                       CampaignCacheService cache) {
        this.nativeRepo = nativeRepo;
        this.repo = repo;
        this.redis = redis;
        this.redisson = redisson;
        this.meta = meta;
        this.cache = cache;
    }

    /** Ensures the coarse key zset + per-campaign allow/block sets exist; idempotent. */
    public void ensureIndexed(String country, String language, String device, String os) {
        final String zsetKey = CampaignCacheService.zsetKey(country, language, device, os);
        // quick existence check
        Long size = redis.opsForZSet().zCard(zsetKey);
        if (size != null && size > 0) return;

        // lock per coarse key to avoid thundering herd
        RLock lock = redisson.getLock("lock:index:" + zsetKey);
        boolean ok = false;
        try {
            ok = lock.tryLock();
            if (!ok) return; // someone else is building it; let caller retry serve
            // recheck under lock
            size = redis.opsForZSet().zCard(zsetKey);
            if (size != null && size > 0) return;

            // fetch top ids for this key
            List<Integer> ids = nativeRepo.findTopIdsForCoarseKey(country, language, device, os, TOP_LIMIT_PER_KEY);
            if (ids.isEmpty()) return;

            // load full entities with filters using the batch entity graph
            List<Campaign> campaigns = repo.findBatchWithFilters(ids);

            // write everything in one pipeline
            redis.executePipelined(new SessionCallback<Object>() {
                @Override
                @SuppressWarnings("unchecked")
                public Object execute(RedisOperations operations) throws DataAccessException {
                    RedisOperations<String,String> ops = (RedisOperations<String,String>) operations;

                    for (Campaign c : campaigns) {
                        String id = Integer.toString(c.getCampaignId());
                        // budget + delta
                        ops.opsForHash().put("campaign:budget:" + id, "remaining",
                                Long.toString(MoneyUtils.toCents(c.getRemainingBudget())));
                        ops.opsForValue().set("campaign:delta:" + id, "0");

                        CampaignFilters f = c.getFilters();
                        if (f == null) continue;

                        CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowBrowserKey(c.getCampaignId()), f.getBrowsers());
                        CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowIabKey(c.getCampaignId()),     f.getIabCategory());
                        CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowIpKey(c.getCampaignId()),      f.getAllowedIP());
                        CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowDomainKey(c.getCampaignId()),
                                CampaignCacheService.toLowerSet(f.getAllowedDomain()));

                        CampaignCacheService.writeBlockSetOps(ops, CampaignCacheService.blockIpKey(c.getCampaignId()),     f.getExcludedIP());
                        CampaignCacheService.writeBlockSetOps(ops, CampaignCacheService.blockDomainKey(c.getCampaignId()),
                                CampaignCacheService.toLowerSet(f.getExcludedDomain()));

                        for (String ctry : f.getCountries())
                            for (String lang : f.getLanguages())
                                for (String dev : f.getDevices())
                                    for (String osItem : f.getOsList())
                                        CampaignCacheService.addCampaignToZsetOps(ops, c, ctry, lang, dev, osItem);
                    }

                    // optional TTL on the coarse zset
                    ops.expire(CampaignCacheService.zsetKey(country, language, device, os), java.time.Duration.ofHours(6));
                    return null;
                }
            });
        } finally {
            if (ok) lock.unlock();
        }
    }
}