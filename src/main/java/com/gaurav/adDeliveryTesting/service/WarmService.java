package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class WarmService {

    private final AdDeliveryRepo repo;
    private final CampaignCacheService cache;
    private final CampaignMetadataCache meta;
    private final StringRedisTemplate redis;

    public WarmService(AdDeliveryRepo repo,
                       CampaignCacheService cache,
                       CampaignMetadataCache meta,
                       StringRedisTemplate redis) {
        this.repo = repo;
        this.cache = cache;
        this.meta = meta;
        this.redis = redis;
    }

    /**
     * Warm a single campaign (full rebuild of Redis + meta cache) – cheap and safe to call.
     */
    public boolean warmOne(int id) {
        var opt = repo.findById(id);
        if (opt.isEmpty()) {
            // remove any stale cache if present
            cache.removeCampaignEverywhere(id);
            meta.invalidate(id);
            return false;
        }
        Campaign c = opt.get();

        // 1) budgets
        var bid = MoneyUtils.toCents(c.getBiddingRate());
        var rem = MoneyUtils.toCents(c.getRemainingBudget());

        // pipeline: write budget/delta + allow/block + zset memberships
        redis.executePipelined(new SessionCallback<Object>() {
            @Override public Object execute(org.springframework.data.redis.core.RedisOperations operations) throws DataAccessException {
                String idStr = Integer.toString(c.getCampaignId());
                // budget
                operations.opsForHash().put("campaign:budget:" + idStr, "remaining", Long.toString(rem));
                operations.opsForValue().set("campaign:delta:" + idStr, "0");

                // clear & write allow/block sets
                CampaignFilters f = c.getFilters();
                if (f != null) {
                    // allow
                    cache.writeAllowSet(CampaignCacheService.allowBrowserKey(c.getCampaignId()), f.getBrowsers());
                    cache.writeAllowSet(CampaignCacheService.allowIabKey(c.getCampaignId()),     f.getIabCategory());
                    cache.writeAllowSet(CampaignCacheService.allowIpKey(c.getCampaignId()),      f.getAllowedIP());
                    cache.writeAllowSet(CampaignCacheService.allowDomainKey(c.getCampaignId()),  cache.lowerAll(f.getAllowedDomain()));
                    // block
                    cache.writeBlockSet(CampaignCacheService.blockIpKey(c.getCampaignId()),      f.getExcludedIP());
                    cache.writeBlockSet(CampaignCacheService.blockDomainKey(c.getCampaignId()),  cache.lowerAll(f.getExcludedDomain()));

                    // zset memberships
                    for (String country : f.getCountries()) {
                        for (String lang : f.getLanguages()) {
                            for (String device : f.getDevices()) {
                                for (String os : f.getOsList()) {
                                    cache.addCampaignToZset(c, country, lang, device, os);
                                }
                            }
                        }
                    }
                }
                return null;
            }
        });

        // 2) meta cache refresh
        meta.reloadFromDb(c.getCampaignId());
        return true;
    }

    /**
     * Warm everything in pages; never loads everything in memory.
     * @param pageSize how many campaign IDs per page (e.g., 5000)
     * @param batchLoadSize how many campaigns to fetch per DB batch (e.g., 1000–2000)
     * @return number of campaigns processed
     */
    public int warmAllPaged(int pageSize, int batchLoadSize) {
        List<Integer> ids = repo.findAllIds(); // only ints in memory
        if (ids.isEmpty()) return 0;

        // optional: clear zset-membership reverse index for all ids (safer rebuild)
        // We only clear per-campaign reverse membership when re-adding members (inside addCampaignToZset we add membership).

        int processed = 0;
        for (int i = 0; i < ids.size(); i += pageSize) {
            List<Integer> page = ids.subList(i, Math.min(i + pageSize, ids.size()));

            // load in smaller DB batches to keep heap steady
            for (int j = 0; j < page.size(); j += batchLoadSize) {
                List<Integer> chunkIds = page.subList(j, Math.min(j + batchLoadSize, page.size()));
                List<Campaign> chunk = repo.findBatchWithFilters(chunkIds);

                // pipeline per chunk
                redis.executePipelined(new SessionCallback<Object>() {
                    @Override public Object execute(org.springframework.data.redis.core.RedisOperations operations) throws DataAccessException {
                        for (Campaign c : chunk) {
                            String idStr = Integer.toString(c.getCampaignId());
                            long rem = MoneyUtils.toCents(c.getRemainingBudget());

                            // budget/delta
                            operations.opsForHash().put("campaign:budget:" + idStr, "remaining", Long.toString(rem));
                            operations.opsForValue().set("campaign:delta:" + idStr, "0");

                            CampaignFilters f = c.getFilters();
                            if (f == null) continue;

                            // allow/block (each writer clears then SADDs)
                            cache.writeAllowSet(CampaignCacheService.allowBrowserKey(c.getCampaignId()), f.getBrowsers());
                            cache.writeAllowSet(CampaignCacheService.allowIabKey(c.getCampaignId()),     f.getIabCategory());
                            cache.writeAllowSet(CampaignCacheService.allowIpKey(c.getCampaignId()),      f.getAllowedIP());
                            cache.writeAllowSet(CampaignCacheService.allowDomainKey(c.getCampaignId()),  cache.lowerAll(f.getAllowedDomain()));
                            cache.writeBlockSet(CampaignCacheService.blockIpKey(c.getCampaignId()),      f.getExcludedIP());
                            cache.writeBlockSet(CampaignCacheService.blockDomainKey(c.getCampaignId()),  cache.lowerAll(f.getExcludedDomain()));

                            for (String country : f.getCountries()) {
                                for (String lang : f.getLanguages()) {
                                    for (String device : f.getDevices()) {
                                        for (String os : f.getOsList()) {
                                            cache.addCampaignToZset(c, country, lang, device, os);
                                        }
                                    }
                                }
                            }
                        }
                        return null;
                    }
                });

                // refresh meta cache for this chunk
                for (Campaign c : chunk) {
                    meta.put(c);
                }

                processed += chunk.size();
            }
        }
        // optional: clear the list cache so /campaigns repopulates
        meta.invalidateCampaignListCache();
        return processed;
    }
}