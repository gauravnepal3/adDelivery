package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.repo.CampaignFilterRepo;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import com.gaurav.adDeliveryTesting.utils.UserAgentParser;
import jakarta.transaction.Transactional;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class AdDeliveryService implements Serializable {

    @Autowired
    private AdDeliveryRepo repo;

    @Autowired
    private CampaignFilterRepo filterRepo;

    @Autowired
    private UserAgentParser parser;

    @Autowired
    private CampaignCacheService cacheService;

    @Autowired
    private RedissonClient redisson;

    @Autowired private BudgetCounterService budgetCounterService;

    @org.springframework.transaction.annotation.Transactional(readOnly = true)
    @Cacheable(value = "campaign", key = "'all'", unless = "#result == null || #result.isEmpty()")
    public List<Campaign> getCampaign() { return repo.findAll(); }

    private String lockName(Integer campaignId) {
        return "lock:campaign:" + campaignId;
    }

    @Transactional
    public Optional<Campaign> serveAd(String country, String language, String os, String browser) {

        // 1) Fast path: single RT, atomic in Redis
        CampaignCacheService.SelectionResult sel = cacheService.pickAndSpendOne(country, language, os, browser);

        if (sel.code == 1 || sel.code == 2) {
            // served; if at zero (2) clean from filter sets
            if (sel.code == 2 && sel.campaignId != null) {
                cacheService.removeCampaignEverywhere(sel.campaignId);
            }
            if (sel.campaignId != null) {
                return repo.findById(sel.campaignId); // light read for link + payload
            }
            return Optional.empty();
        }
        if (sel.code == 3) {
            // insufficient – evict and fall through to DB fallback
            if (sel.campaignId != null) {
                cacheService.removeCampaignEverywhere(sel.campaignId);
            }
        }

        // 2) DB fallback: compute best and warm Redis
        List<CampaignFilters> filters = filterRepo.findAll();
        List<Campaign> matching = filters.stream()
                .filter(f -> (country == null || f.getCountries().contains(country)))
                .filter(f -> (language == null || f.getLanguages().contains(language)))
                .filter(f -> (os == null || f.getOsList().contains(os)))
                .filter(f -> (browser == null || f.getBrowsers().contains(browser)))
                .map(CampaignFilters::getCampaign)
                .filter(c -> c.getRemainingBudget().compareTo(BigDecimal.ZERO) > 0)
                .toList();
        if (matching.isEmpty()) return Optional.empty();

        BigDecimal topBid = matching.stream().map(Campaign::getBiddingRate).max(BigDecimal::compareTo).orElse(BigDecimal.ZERO);
        List<Campaign> top = matching.stream().filter(c -> c.getBiddingRate().compareTo(topBid) == 0).toList();
        if (top.isEmpty()) return Optional.empty();

        Campaign chosen = top.get(ThreadLocalRandom.current().nextInt(top.size()));

        // warm the exact combo + all combos for chosen
        cacheService.addCampaign(chosen, country, language, os, browser);
        if (chosen.getFilters() != null) {
            var cf = chosen.getFilters();
            for (String ctry : cf.getCountries())
                for (String lang : cf.getLanguages())
                    for (String osItem : cf.getOsList())
                        for (String br : cf.getBrowsers())
                            cacheService.addCampaign(chosen, ctry, lang, osItem, br);
        }

        // Try again via Redis atomic (now should hit)
        CampaignCacheService.SelectionResult sel2 = cacheService.pickAndSpendOne(country, language, os, browser);
        if (sel2.code == 1 || sel2.code == 2) {
            if (sel2.code == 2 && sel2.campaignId != null) cacheService.removeCampaignEverywhere(sel2.campaignId);
            return sel2.campaignId != null ? repo.findById(sel2.campaignId) : Optional.empty();
        }
        return Optional.empty();
    }
    private Optional<Campaign> tryServeCounter(Integer campaignId,
                                               String country, String language, String os, String browser) {
        var codec = org.redisson.client.codec.StringCodec.INSTANCE;

        Optional<Campaign> cOpt = repo.findById(campaignId);
        if (cOpt.isEmpty()) {
            cacheService.removeCampaignEverywhere(campaignId);
            return Optional.empty();
        }
        Campaign c = cOpt.get();
        long bidCents = MoneyUtils.toCents(c.getBiddingRate());

        // First attempt to spend
        int rc = budgetCounterService.trySpendCents(campaignId, bidCents);

        // If not enough, check if budget
        if (rc == 0) {
            var map = redisson.getMap("campaign:budget:" + campaignId, codec);
            String remStr = (String) map.get("remaining");
            if (remStr == null) {
                long remCents = MoneyUtils.toCents(c.getRemainingBudget());
                map.fastPut("remaining", Long.toString(remCents));
                rc = budgetCounterService.trySpendCents(c.getCampaignId(), bidCents);
            }
            if (rc == 0) {

                if (c.getRemainingBudget().compareTo(c.getBiddingRate()) < 0) {
                    cacheService.removeCampaignEverywhere(c.getCampaignId());
                }
                return Optional.empty();
            }
        }


        if (rc == 2) {
            cacheService.removeCampaignEverywhere(c.getCampaignId());
        }

        cacheService.updateCampaignBudget(c, country, language, os, browser);
        return Optional.of(c);
    }
}
