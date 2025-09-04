package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.repo.AdDeliveryPickRepo;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.repo.BudgetRepo;
import com.gaurav.adDeliveryTesting.responseDto.ServeResponseDTO;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;


@Service
public class AdDeliveryService {

    private final AdDeliveryRepo repo;
    private final CampaignMetadataCache meta;
    private final ServeScriptService serveScript;
    private final AdDeliveryFallbackService fallback;
    private final LazyIndexer indexer;
    private final NegativeResultCache neg;     // NEW
    private final PositivePickCache pos;       // NEW
    private final DbBulkhead bulkhead;         // NEW

    @Autowired private AdDeliveryPickRepo pickRepo;
    @Autowired private BudgetDbService budgetDbService;

    @Value("${adserve.dbFallbackEnabled:true}")
    private boolean dbFallbackEnabled;

    public AdDeliveryService(AdDeliveryRepo repo,
                             CampaignMetadataCache meta,
                             ServeScriptService serveScript,
                             AdDeliveryFallbackService fallback,
                             LazyIndexer indexer,
                             NegativeResultCache neg,
                             PositivePickCache pos,
                             DbBulkhead bulkhead) {
        this.repo = repo;
        this.meta = meta;
        this.serveScript = serveScript;
        this.fallback = fallback;
        this.indexer = indexer;
        this.neg = neg;
        this.pos = pos;
        this.bulkhead = bulkhead;
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
                MoneyUtils.fromCents(v.bidCents()),
                (r.newRemaining != null)
                        ? MoneyUtils.fromCents(Math.max(r.newRemaining, 0))
                        : MoneyUtils.fromCents(v.remainingCents() - v.bidCents())
        ));
    }

    public Optional<ServeResponseDTO> serve(String country, String language, String device, String os,
                                            String ip, String domain, String browser, String iab) {

        final String d  = (domain  == null ? "" : domain);
        final String br = (browser == null ? "" : browser);
        final String ic = (iab     == null ? "" : iab);
        final String ipx= (ip      == null ? "" : ip);

        // 1) try Redis fast path
        var fast = serveFast(country, language, device, os, ipx, d, br, ic);
        if (fast.isPresent()) return fast;

        // build coarse key identical to your SQL parameters order
        final String key = String.join("|", country, language, device, os, br, ic, ipx, d);

        // 2) negative cache: avoid pounding DB on known-miss keys
        if (neg.recentlyMissed(key)) return Optional.empty();

        // 3) positive cache: skip the pick SQL if we very recently picked an id for the same key
        Integer cachedId = pos.get(key);
        if (cachedId != null) {
            var metaDto = meta.get(cachedId);
            if (metaDto != null) {
                var newRem = budgetDbService.trySpendAndGetRemaining(cachedId, MoneyUtils.fromCents(metaDto.bidCents()));
                if (newRem != null) {
                    indexer.enqueueIndex(country, language, device, os, cachedId); // fire-and-forget
                    return Optional.of(new ServeResponseDTO(
                            metaDto.campaignId(),
                            metaDto.deliveryLink(),
                            MoneyUtils.fromCents(metaDto.bidCents()),
                            newRem
                    ));
                } else {
                    // budget failed—drop this positive entry so next time we re-pick
                    pos.invalidate(key);
                }
            } else {
                pos.invalidate(key);
            }
        }

        // 4) bulkhead before doing the expensive pick SQL
        if (!bulkhead.enter()) {
            // fast fail under surge so we don’t exhaust Hikari waiting
            neg.markMiss(key, /*ttl override*/ 1000); // very short miss to dampen a spike
            return Optional.empty();
        }
        try {
            Integer id = pickRepo.pickTopOne(country, language, device, os, br, ic, ipx, d);
            if (id == null) {
                neg.markMiss(key);
                return Optional.empty();
            }

            var metaDto = meta.get(id);
            if (metaDto == null) {
                neg.markMiss(key);
                return Optional.empty();
            }

            var newRemaining = budgetDbService.trySpendAndGetRemaining(id, MoneyUtils.fromCents(metaDto.bidCents()));
            if (newRemaining == null) {
                // lost the race on budget; mark short miss and bail
                neg.markMiss(key, 1000);
                return Optional.empty();
            }

            // warm positive cache briefly so repeated identical requests skip pick SQL
            pos.put(key, id);

            // warm Redis for this coarse key
            indexer.enqueueIndex(country, language, device, os, id);

            return Optional.of(new ServeResponseDTO(
                    metaDto.campaignId(),
                    metaDto.deliveryLink(),
                    MoneyUtils.fromCents(metaDto.bidCents()),
                    newRemaining
            ));
        } finally {
            bulkhead.leave();
        }
    }

    @org.springframework.transaction.annotation.Transactional(readOnly = true)
    @Cacheable(value = "campaign", key = "'all'", unless = "#result == null || #result.isEmpty()")
    public List<com.gaurav.adDeliveryTesting.model.Campaign> getCampaign() {
        return repo.findAll();
    }
}