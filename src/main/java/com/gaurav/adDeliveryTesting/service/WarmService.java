package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

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

    /** Warm a single campaign completely, fully pipelined. */
    public boolean warmOne(int id) {
        var opt = repo.findById(id);
        if (opt.isEmpty()) {
            cache.removeCampaignEverywhere(id);
            meta.invalidate(id);
            return false;
        }
        Campaign c = opt.get();
        meta.put(c);

        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                @SuppressWarnings("unchecked")
                RedisOperations<String,String> ops = (RedisOperations<String,String>) operations;

                String idStr = Integer.toString(c.getCampaignId());
                // budget & delta
                ops.opsForHash().put("campaign:budget:" + idStr, "remaining",
                        Long.toString(MoneyUtils.toCents(c.getRemainingBudget())));
                ops.opsForValue().set("campaign:delta:" + idStr, "0");

                CampaignFilters f = c.getFilters();
                if (f != null) {
                    CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowBrowserKey(c.getCampaignId()), f.getBrowsers());
                    CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowIabKey(c.getCampaignId()),     f.getIabCategory());
                    CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowIpKey(c.getCampaignId()),      f.getAllowedIP());
                    CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowDomainKey(c.getCampaignId()),  CampaignCacheService.toLowerSet(f.getAllowedDomain()));

                    CampaignCacheService.writeBlockSetOps(ops, CampaignCacheService.blockIpKey(c.getCampaignId()),      f.getExcludedIP());
                    CampaignCacheService.writeBlockSetOps(ops, CampaignCacheService.blockDomainKey(c.getCampaignId()),  CampaignCacheService.toLowerSet(f.getExcludedDomain()));

                    for (String country : f.getCountries())
                        for (String lang : f.getLanguages())
                            for (String device : f.getDevices())
                                for (String os : f.getOsList())
                                    CampaignCacheService.addCampaignToZsetOps(ops, c, country, lang, device, os);
                }
                return null;
            }
        });

        return true;
    }

    /** Warm everything in pages & batches; all Redis writes are pipelined. */
    public int warmAllPaged(int pageSize, int batchLoadSize) {
        List<Integer> ids = repo.findAllIds();
        if (ids.isEmpty()) return 0;

        int processed = 0;

        for (int i = 0; i < ids.size(); i += pageSize) {
            List<Integer> page = ids.subList(i, Math.min(i + pageSize, ids.size()));

            for (int j = 0; j < page.size(); j += batchLoadSize) {
                List<Integer> chunkIds = page.subList(j, Math.min(j + batchLoadSize, page.size()));
                List<Campaign> chunk = repo.findBatchWithFilters(chunkIds);

                for (Campaign c : chunk) meta.put(c);

                redis.executePipelined(new SessionCallback<Object>() {
                    @Override
                    public Object execute(RedisOperations operations) throws DataAccessException {
                        @SuppressWarnings("unchecked")
                        RedisOperations<String,String> ops = (RedisOperations<String,String>) operations;

                        for (Campaign c : chunk) {
                            String idStr = Integer.toString(c.getCampaignId());
                            ops.opsForHash().put("campaign:budget:" + idStr, "remaining",
                                    Long.toString(MoneyUtils.toCents(c.getRemainingBudget())));
                            ops.opsForValue().set("campaign:delta:" + idStr, "0");

                            CampaignFilters f = c.getFilters();
                            if (f == null) continue;

                            CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowBrowserKey(c.getCampaignId()), f.getBrowsers());
                            CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowIabKey(c.getCampaignId()),     f.getIabCategory());
                            CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowIpKey(c.getCampaignId()),      f.getAllowedIP());
                            CampaignCacheService.writeAllowSetOps(ops, CampaignCacheService.allowDomainKey(c.getCampaignId()),  CampaignCacheService.toLowerSet(f.getAllowedDomain()));

                            CampaignCacheService.writeBlockSetOps(ops, CampaignCacheService.blockIpKey(c.getCampaignId()),      f.getExcludedIP());
                            CampaignCacheService.writeBlockSetOps(ops, CampaignCacheService.blockDomainKey(c.getCampaignId()),  CampaignCacheService.toLowerSet(f.getExcludedDomain()));

                            for (String country : f.getCountries())
                                for (String lang : f.getLanguages())
                                    for (String device : f.getDevices())
                                        for (String os : f.getOsList())
                                            CampaignCacheService.addCampaignToZsetOps(ops, c, country, lang, device, os);
                        }
                        return null;
                    }
                });

                processed += chunk.size();
            }
        }
        meta.invalidateCampaignListCache();
        return processed;
    }
}