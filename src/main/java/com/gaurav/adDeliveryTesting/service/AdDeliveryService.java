package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.repo.AdDeliveryPickRepo;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.repo.BudgetRepo;
import com.gaurav.adDeliveryTesting.responseDto.ServeResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private AdDeliveryPickRepo pickRepo;

    @Autowired
    private BudgetRepo budgetRepo;
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
        // 0) normalize empties & domain host lowercased (you already have DomainUtils)
        final String d  = (domain  == null ? "" : domain);
        final String br = (browser == null ? "" : browser);
        final String ic = (iab     == null ? "" : iab);
        final String ipx= (ip      == null ? "" : ip);

        // 1) try Redis fast path
        var fast = serveFast(country, language, device, os, ipx, d, br, ic);
        if (fast.isPresent()) return fast;

        // 2) DB pick (single-row native) – no entity graphs
        Integer id = pickRepo.pickTopOne(country, language, device, os, br, ic, ipx, d);
        if (id == null) return Optional.empty();

        // 3) DB atomic spend
        var metaDto = meta.get(id); // cheap Caffeine lookup, loads one row if missing
        if (metaDto == null) return Optional.empty();

        var spentRem = budgetRepo.trySpend(id, com.gaurav.adDeliveryTesting.utils.MoneyUtils.fromCents(metaDto.bidCents()));
        if (spentRem == null) return Optional.empty(); // race lost

        // 4) fire-and-forget: index this id into Redis for this coarse key
        indexer.enqueueIndex(country, language, device, os, id);

        // 5) return response immediately
        return Optional.of(new ServeResponseDTO(
                metaDto.campaignId(),
                metaDto.deliveryLink(),
                com.gaurav.adDeliveryTesting.utils.MoneyUtils.fromCents(metaDto.bidCents()),
                spentRem
        ));
    }
    @org.springframework.transaction.annotation.Transactional(readOnly = true)
    @Cacheable(value = "campaign", key = "'all'", unless = "#result == null || #result.isEmpty()")
    public List<com.gaurav.adDeliveryTesting.model.Campaign> getCampaign() {
        return repo.findAll();
    }
}