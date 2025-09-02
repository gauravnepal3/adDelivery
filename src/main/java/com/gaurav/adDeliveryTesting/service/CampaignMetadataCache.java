package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import com.gaurav.adDeliveryTesting.responseDto.CampaignResponseDto;
import com.github.benmanes.caffeine.cache.*;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.*;

@Component
public class CampaignMetadataCache {

    private final AdDeliveryRepo repo;

    private final LoadingCache<Integer, CampaignResponseDto> cache = Caffeine.newBuilder()
            .maximumSize(1_000_000)                   // adjust to cardinality
            .expireAfterWrite(Duration.ofMinutes(10))
            .recordStats()
            .build(this::loadOne);

    public CampaignMetadataCache(AdDeliveryRepo repo) {
        this.repo = repo;
    }

    private CampaignResponseDto loadOne(Integer id) {
        return repo.findById(id)
                .map(c -> new CampaignResponseDto(
                        c.getCampaignId(),
                        c.getDeliveryLink(),
                        MoneyUtils.toCents(c.getBiddingRate()),
                        MoneyUtils.toCents(c.getRemainingBudget())
                ))
                .orElse(null);
    }

    public CampaignResponseDto get(int id) { return cache.get(id); }

    public void warmAll(Iterable<Campaign> all) {
        all.forEach(c -> cache.put(
                c.getCampaignId(),
                new CampaignResponseDto(
                        c.getCampaignId(),
                        c.getDeliveryLink(),
                        MoneyUtils.toCents(c.getBiddingRate()),
                        MoneyUtils.toCents(c.getRemainingBudget())
                )));
    }

    public void invalidate(int id) { cache.invalidate(id); }
}