package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.repo.CampaignFilterRepo;
import com.gaurav.adDeliveryTesting.responseDto.CampaignResponseDto;
import com.gaurav.adDeliveryTesting.responseDto.ServeResponseDTO;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import com.gaurav.adDeliveryTesting.utils.UserAgentParser;
import jakarta.transaction.Transactional;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class AdDeliveryService implements Serializable {

    @Autowired private AdDeliveryRepo repo;
    @Autowired private CampaignFilterRepo filterRepo;
    @Autowired private CampaignCacheService cacheService;
    @Autowired private RedissonClient redisson;
    @Autowired private CampaignMetadataCache meta;
    @Autowired private BudgetCounterService budgetCounterService;
    @Autowired private AdDeliveryFallbackService fallback;
    @Value("${adserve.dbFallbackEnabled:true}")
    private boolean dbFallbackEnabled;

    @Autowired private ServeScriptService serveScript;

    public Optional<ServeResponseDTO> serveAdFast(String country, String language, String os, String browser) {
        // One RTT: pick + spend + touch
        var r = serveScript.pickAndSpend(country, language, os, browser);

        if (r.code() == 0 || r.id() == null) {
            // OPTIONAL: if budgets weren't warmed for this id, init once and retry quickly
            // (kept off by default; uncomment if you see warmup races)
            // if (r.id() != null) {
            //     var v = meta.get(r.id());
            //     if (v != null) {
            //         var codec = org.redisson.client.codec.StringCodec.INSTANCE;
            //         redisson.getMap("campaign:budget:" + r.id(), codec)
            //                 .fastPut("remaining", Long.toString(v.remainingCents()));
            //         r = serveScript.pickAndSpend(country, language, os, browser);
            //         if (r.code() == 0 || r.id() == null) return Optional.empty();
            //     } else {
            //         cacheService.removeCampaignEverywhere(r.id());
            //     }
            // }
            return Optional.empty();
        }

        // If exhausted, remove from all zsets
        if (r.code() == 2) {
            cacheService.removeCampaignEverywhere(r.id());
        }

        var v = meta.get(r.id());
        if (v == null) {
            cacheService.removeCampaignEverywhere(r.id());
            return Optional.empty();
        }

        // Build tiny DTO (no JPA, no Map.of)
        return Optional.of(new ServeResponseDTO(
                v.campaignId(),
                v.deliveryLink(),
                MoneyUtils.fromCents(v.bidCents()),
                r.newRemainingCents() != null
                        ? MoneyUtils.fromCents(Math.max(0, r.newRemainingCents()))
                        : MoneyUtils.fromCents(Math.max(0, v.remainingCents() - v.bidCents()))
        ));
    }
    // Keep your cacheable list endpoint as-is; it's not hot path
    @org.springframework.transaction.annotation.Transactional(readOnly = true)
    @Cacheable(value = "campaign", key = "'all'", unless = "#result == null || #result.isEmpty()")
    public List<Campaign> getCampaign() { return repo.findAll(); }

    // ---------- WRAPPER used by controller ----------
    // Not transactional. Tries FAST first; only uses DB if flag enabled.
    public Optional<ServeResponseDTO> serveAd(String country, String language, String os, String browser) {
        var fast = serveAdFast(country, language, os, browser);
        if (fast.isPresent()) return fast;
        if (!dbFallbackEnabled) return Optional.empty();
        return fallback.serveAdFallback(country, language, os, browser);
    }

    // ---------- OPTIONAL SLOW PATH (ONLY when flag enabled) ----------
    @org.springframework.transaction.annotation.Transactional(readOnly = true)
    protected Optional<ServeResponseDTO> serveAdFallback(String country, String language, String os, String browser) {
        // Your existing fallback logic, but return DTO and avoid extra DB calls
        List<CampaignFilters> filters = filterRepo.findAll();

        var matching = filters.stream()
                .filter(f -> (language == null || f.getLanguages().contains(language)))
                .filter(f -> (os == null || f.getOsList().contains(os)))
                .filter(f -> (browser == null || f.getBrowsers().contains(browser)))
                .map(CampaignFilters::getCampaign)
                .filter(c -> c.getRemainingBudget().compareTo(BigDecimal.ZERO) > 0)
                .toList();

        if (matching.isEmpty()) return Optional.empty();

        BigDecimal topBid = matching.stream().map(Campaign::getBiddingRate).max(BigDecimal::compareTo).orElse(BigDecimal.ZERO);
        var topCampaigns = matching.stream().filter(c -> c.getBiddingRate().compareTo(topBid) == 0).toList();
        if (topCampaigns.isEmpty()) return Optional.empty();

        Campaign chosen = topCampaigns.get(ThreadLocalRandom.current().nextInt(topCampaigns.size()));

        // Warm all combos once so future hits are fast-path only
        CampaignFilters cf = chosen.getFilters();
        if (cf != null) {
            for (String ctry : cf.getCountries()) {
                for (String lang : cf.getLanguages()) {
                    for (String osItem : cf.getOsList()) {
                        for (String br : cf.getBrowsers()) {
                            cacheService.addCampaign(chosen, ctry, lang, osItem, br);
                        }
                    }
                }
            }
        }

        // Spend via Redis and return DTO (no more JPA reads)
        long bidCents = MoneyUtils.toCents(chosen.getBiddingRate());
        int rc = budgetCounterService.trySpendCents(chosen.getCampaignId(), bidCents);
        if (rc == 0) return Optional.empty();
        if (rc == 2) cacheService.removeCampaignEverywhere(chosen.getCampaignId());

        return Optional.of(new ServeResponseDTO(
                chosen.getCampaignId(),
                chosen.getDeliveryLink(),
                chosen.getBiddingRate(),
                chosen.getRemainingBudget().subtract(chosen.getBiddingRate())
        ));
    }
}