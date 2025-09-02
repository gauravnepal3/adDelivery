package com.gaurav.adDeliveryTesting.bootstrap;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
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

@Component
public class BudgetWarmup {
    private static final Logger log = LoggerFactory.getLogger(BudgetWarmup.class);

    private final AdDeliveryRepo repo;
    private final RedissonClient redisson;

    @Autowired
    private CampaignMetadataCache meta;
    public BudgetWarmup(AdDeliveryRepo repo, RedissonClient redisson) {
        this.repo = repo; this.redisson = redisson;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void warm() {
        var codec = StringCodec.INSTANCE;
        int count = 0;
        for (Campaign c : repo.findAll()) {
            long remCents = MoneyUtils.toCents(c.getRemainingBudget());
            String id = String.valueOf(c.getCampaignId());
            redisson.getMap("campaign:budget:" + id, codec).fastPut("remaining", Long.toString(remCents));
            redisson.getBucket("campaign:delta:" + id, codec).set("0");
            count++;
        }
        log.info("BudgetWarmup initialized {} campaigns in Redis", count);
        meta.warmAll(repo.findAll());
    }
}
