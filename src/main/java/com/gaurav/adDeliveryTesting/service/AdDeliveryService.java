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
        Integer campaignId = cacheService.getBestCampaignId(country, language, os, browser);
        if (campaignId != null) {
            Optional<Campaign> served = tryServeCounter(campaignId, country, language, os, browser);
            if (served.isPresent()) return served;
        }

        // 2) DB fallback: filter & pick best bid, with FAIR tie-breaker across equal bids
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

        // Find the top bid value
        BigDecimal topBid = matching.stream()
                .map(Campaign::getBiddingRate)
                .max(BigDecimal::compareTo)
                .orElse(BigDecimal.ZERO);

        // Collect all campaigns that have the same top bid
        List<Campaign> topCampaigns = matching.stream()
                .filter(c -> c.getBiddingRate().compareTo(topBid) == 0)
                .toList();

        if (topCampaigns.isEmpty()) return Optional.empty();

        // Random tie-breaker among same-bid campaigns
        Campaign chosen = topCampaigns.get(ThreadLocalRandom.current().nextInt(topCampaigns.size()));

        Optional<Campaign> served = tryServeCounter(chosen.getCampaignId(), country, language, os, browser);

        if (served.isPresent()) {
            Campaign warmed = served.get();
            CampaignFilters cf = warmed.getFilters();
            if (cf != null) {
                for (String ctry : cf.getCountries()) {
                    for (String lang : cf.getLanguages()) {
                        for (String osItem : cf.getOsList()) {
                            for (String br : cf.getBrowsers()) {
                                cacheService.addCampaign(warmed, ctry, lang, osItem, br);
                            }
                        }
                    }
                }
            }
        }
        return served;
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