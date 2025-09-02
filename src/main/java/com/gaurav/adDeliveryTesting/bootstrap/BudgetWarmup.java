package com.gaurav.adDeliveryTesting.bootstrap;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.service.CampaignCacheService;
import com.gaurav.adDeliveryTesting.service.CampaignMetadataCache;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

// BudgetWarmup.java
@Component
public class BudgetWarmup {
    private static final Logger log = LoggerFactory.getLogger(BudgetWarmup.class);

    private final AdDeliveryRepo repo;
    private final RedissonClient redisson;
    private final CampaignCacheService cacheService;

    public BudgetWarmup(AdDeliveryRepo repo, RedissonClient redisson, CampaignCacheService cacheService) {
        this.repo = repo; this.redisson = redisson; this.cacheService = cacheService;
    }

    @EventListener(ApplicationReadyEvent.class)
    @org.springframework.transaction.annotation.Transactional(readOnly = true)
    public void warm() {
        var codec = org.redisson.client.codec.StringCodec.INSTANCE;
        int count = 0;

        for (Campaign c : repo.findAll()) {
            // budgets
            long remCents = com.gaurav.adDeliveryTesting.utils.MoneyUtils.toCents(c.getRemainingBudget());
            String id = String.valueOf(c.getCampaignId());
            redisson.getMap("campaign:budget:" + id, codec).fastPut("remaining", Long.toString(remCents));
            redisson.getBucket("campaign:delta:" + id, codec).set("0");

            // zsets – SAFE now because we’re inside a transaction
            CampaignFilters cf = c.getFilters();
            if (cf != null) {
                for (String country : cf.getCountries())
                    for (String lang    : cf.getLanguages())
                        for (String os      : cf.getOsList())
                            for (String br      : cf.getBrowsers()) {
                                cacheService.addCampaign(c, country, lang, os, br);
                            }
            }
            count++;
        }
        log.info("Warmup initialized {} campaigns (budgets + zsets)", count);
    }
}