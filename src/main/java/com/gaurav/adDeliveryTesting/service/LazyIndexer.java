// src/main/java/com/gaurav/adDeliveryTesting/service/LazyIndexer.java
package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryNativeRepo;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class LazyIndexer {

    private final AdDeliveryNativeRepo nativeRepo;
    private final AdDeliveryRepo repo;
    private final StringRedisTemplate redis;
    private final RedissonClient redisson;
    private final CampaignMetadataCache meta;
    private final CampaignCacheService cache;

    // Tune these first; keeping them small prevents heap blowups
    private static final int TOP_LIMIT_PER_KEY = 50;      // how many campaigns per coarse key
    private static final int SLICE_SIZE        = 50;      // load & pipe this many per slice
    private static final Duration KEY_TTL      = Duration.ofHours(6);

    @PersistenceContext
    private EntityManager em;

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

    /** Idempotent: creates the coarse-key ZSET & per-campaign sets if missing. */
    public void ensureIndexed(String country, String language, String device, String os) {
        final String zsetKey = CampaignCacheService.zsetKey(country, language, device, os);

        // quick check — if present and non-empty, done
        Long size = redis.opsForZSet().zCard(zsetKey);
        if (size != null && size > 0) return;

        // lock per-coarse-key to avoid thundering herd; short wait/lease
        RLock lock = redisson.getLock("lock:index:" + zsetKey);
        boolean locked = false;
        try {
            locked = lock.tryLock(0, 30, TimeUnit.SECONDS);
            if (!locked) return;

            // recheck under lock
            size = redis.opsForZSet().zCard(zsetKey);
            if (size != null && size > 0) return;

            // 1) find top candidates for this coarse key (cheap native query)
            List<Integer> ids = nativeRepo.findTopIdsForCoarseKey(country, language, device, os, TOP_LIMIT_PER_KEY);
            if (ids == null || ids.isEmpty()) {
                // still set a short TTL zset to avoid rework storms on empty segments
                redisson.getKeys().expireAsync(zsetKey, 60, TimeUnit.SECONDS);
                return;
            }

            // 2) load + write in small slices to keep memory flat
            for (int i = 0; i < ids.size(); i += SLICE_SIZE) {
                List<Integer> sliceIds = ids.subList(i, Math.min(i + SLICE_SIZE, ids.size()));
                // loads with EntityGraph, but only 50 at a time
                List<Campaign> slice = repo.findBatchWithFilters(sliceIds);

                // fill the tiny metadata cache from entities (no extra DB calls)
                for (Campaign c : slice) meta.put(c);

                // single pipeline for the slice
                redis.executePipelined(new SessionCallback<Object>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public Object execute(RedisOperations operations) throws DataAccessException {
                        RedisOperations<String,String> ops = (RedisOperations<String,String>) operations;

                        for (Campaign c : slice) {
                            String idStr = Integer.toString(c.getCampaignId());
                            // budgets
                            ops.opsForHash().put("campaign:budget:" + idStr, "remaining",
                                    Long.toString(MoneyUtils.toCents(c.getRemainingBudget())));
                            ops.opsForValue().set("campaign:delta:" + idStr, "0");

                            CampaignFilters f = c.getFilters();
                            if (f == null) continue;

                            // allow/block (one SADD per set)
                            CampaignCacheService.writeAllowSetOps(ops,
                                    CampaignCacheService.allowBrowserKey(c.getCampaignId()),
                                    f.getBrowsers());

                            CampaignCacheService.writeAllowSetOps(ops,
                                    CampaignCacheService.allowIabKey(c.getCampaignId()),
                                    f.getIabCategory());

                            CampaignCacheService.writeAllowSetOps(ops,
                                    CampaignCacheService.allowIpKey(c.getCampaignId()),
                                    f.getAllowedIP());

                            CampaignCacheService.writeAllowSetOps(ops,
                                    CampaignCacheService.allowDomainKey(c.getCampaignId()),
                                    CampaignCacheService.toLowerSet(f.getAllowedDomain()));

                            CampaignCacheService.writeBlockSetOps(ops,
                                    CampaignCacheService.blockIpKey(c.getCampaignId()),
                                    f.getExcludedIP());

                            CampaignCacheService.writeBlockSetOps(ops,
                                    CampaignCacheService.blockDomainKey(c.getCampaignId()),
                                    CampaignCacheService.toLowerSet(f.getExcludedDomain()));

                            // coarse zset memberships
                            for (String ctry : safe(f.getCountries()))
                                for (String lang : safe(f.getLanguages()))
                                    for (String dev  : safe(f.getDevices()))
                                        for (String osItem : safe(f.getOsList()))
                                            CampaignCacheService.addCampaignToZsetOps(ops, c, ctry, lang, dev, osItem);
                        }

                        // put a TTL on this particular coarse zset
                        ops.expire(zsetKey, KEY_TTL);
                        return null;
                    }
                });

                // free the slice from the persistence context to keep heap flat
                em.clear();
            }

            // Redisson expire with (long, TimeUnit) — avoids Duration overload issues
            redisson.getKeys().expireAsync(zsetKey, KEY_TTL.toSeconds(), TimeUnit.SECONDS);

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            if (locked) {
                try { lock.unlock(); } catch (Exception ignore) {}
            }
        }
    }

    private static <T> Iterable<T> safe(Iterable<T> it) {
        return it == null ? new ArrayList<>() : it;
    }
}