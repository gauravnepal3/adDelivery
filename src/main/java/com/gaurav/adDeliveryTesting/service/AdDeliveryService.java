package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.responseDto.ServeResponseDTO;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
@Service
public class AdDeliveryService {

    private final AdDeliveryRepo repo;
    private final CampaignMetadataCache meta;
    private final ServeScriptService serveScript;
    private final AdDeliveryFallbackService fallback;
    private final LazyIndexer indexer;

    @Value("${adserve.dbFallbackEnabled:true}")
    private boolean dbFallbackEnabled;

    public AdDeliveryService(AdDeliveryRepo repo,
                             CampaignMetadataCache meta,
                             ServeScriptService serveScript,
                             AdDeliveryFallbackService fallback,
                             LazyIndexer indexer) {
        this.repo = repo;
        this.meta = meta;
        this.serveScript = serveScript;
        this.fallback = fallback;
        this.indexer = indexer;
    }

    public Optional<ServeResponseDTO> serveFast(String country, String language, String device, String os,
                                                String ip, String domain, String browser, String iab) {
        var r = serveScript.pickAndSpend(country, language, device, os, ip, domain, browser, iab);
        if (r.code == 0 || r.campaignId == null) return Optional.empty();

        var v = meta.get(r.campaignId);
        if (v == null) return Optional.empty();

        return Optional.of(new ServeResponseDTO(
                v.campaignId(),
                v.deliveryLink(),
                com.gaurav.adDeliveryTesting.utils.MoneyUtils.fromCents(v.bidCents()),
                (r.newRemaining != null)
                        ? com.gaurav.adDeliveryTesting.utils.MoneyUtils.fromCents(Math.max(r.newRemaining, 0))
                        : com.gaurav.adDeliveryTesting.utils.MoneyUtils.fromCents(v.remainingCents() - v.bidCents())
        ));
    }

    public Optional<ServeResponseDTO> serve(String country, String language, String device, String os,
                                            String ip, String domain, String browser, String iab) {
        // 1) try fast
        var fast = serveFast(country, language, device, os, ip, domain, browser, iab);
        if (fast.isPresent()) return fast;

        // 2) lazily index this coarse key and retry fast exactly once
        indexer.ensureIndexed(country, language, device, os);
        fast = serveFast(country, language, device, os, ip, domain, browser, iab);
        if (fast.isPresent()) return fast;

        // 3) optional DB fallback (rare)
        if (!dbFallbackEnabled) return Optional.empty();
        return fallback.serveAdFallback(country, language, device, os, ip, domain, browser, iab);
    }

    @org.springframework.transaction.annotation.Transactional(readOnly = true)
    @Cacheable(value = "campaign", key = "'all'", unless = "#result == null || #result.isEmpty()")
    public List<com.gaurav.adDeliveryTesting.model.Campaign> getCampaign() {
        return repo.findAll();
    }
}