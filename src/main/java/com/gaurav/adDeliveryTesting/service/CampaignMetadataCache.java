package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.responseDto.CampaignResponseDto;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;

@Component
public class CampaignMetadataCache {

    private final AdDeliveryRepo repo;
    // Optional: only used if you want to also clear the @Cacheable("campaign") list cache
    private final CacheManager cacheManager; // may be null if none configured

    public CampaignMetadataCache(AdDeliveryRepo repo, CacheManager cacheManager) {
        this.repo = repo;
        this.cacheManager = cacheManager;
    }

    private final LoadingCache<Integer, CampaignResponseDto> cache = Caffeine.newBuilder()
            .maximumSize(200_000)                   // tune for your cardinality
            .expireAfterWrite(Duration.ofMinutes(10))
            .recordStats()
            .build(this::loadOne);

    // ---------- Loaders ----------

    private CampaignResponseDto loadOne(Integer id) {
        return repo.findById(id)
                .map(CampaignMetadataCache::toDto)
                .orElse(null);
    }

    private static CampaignResponseDto toDto(Campaign c) {
        return new CampaignResponseDto(
                c.getCampaignId(),
                c.getDeliveryLink(),
                MoneyUtils.toCents(c.getBiddingRate()),
                MoneyUtils.toCents(c.getRemainingBudget())
        );
    }

    // ---------- Public API used by serve path & maintenance routes ----------

    /** Get (loads from DB if absent or expired). */
    public CampaignResponseDto get(int id) {
        return cache.get(id);
    }

    /** Put/overwrite with a fresh Campaign snapshot (used after DB update). */
    public void put(Campaign campaign) {
        Objects.requireNonNull(campaign, "campaign");
        cache.put(campaign.getCampaignId(), toDto(campaign));
    }

    /** Warm a bunch at once (used by startup warmup). */
    public void warmAll(Iterable<Campaign> all) {
        for (Campaign c : all) {
            cache.put(c.getCampaignId(), toDto(c));
        }
    }

    /** Evict one id from the per-campaign Caffeine cache. */
    public void invalidate(int id) {
        cache.invalidate(id);
    }

    /**
     * Convenience: reload a single id from DB right now (evict + put),
     * returning the fresh DTO or null if not found.
     */
    public CampaignResponseDto reloadFromDb(int id) {
        var fresh = repo.findById(id).map(CampaignMetadataCache::toDto).orElse(null);
        if (fresh == null) {
            cache.invalidate(id);
        } else {
            cache.put(id, fresh);
        }
        return fresh;
    }

    /** Optional: clear the list cache used by @Cacheable("campaign") getCampaign(). */
    public void invalidateCampaignListCache() {
        if (cacheManager != null) {
            var c = cacheManager.getCache("campaign");
            if (c != null) c.clear();  // clears the whole list cache
        }
    }

    /** Optional helper if you’re reindexing many campaigns. */
    public void reloadBatchFromDb(Collection<Integer> ids) {
        for (Integer id : ids) {
            reloadFromDb(id);
        }
    }
}