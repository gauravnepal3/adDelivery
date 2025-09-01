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
        // 2) DO NOT fall back to DB on the request path under load.
        //    Return 204 for a miss; let warmup keep Redis hot.
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
