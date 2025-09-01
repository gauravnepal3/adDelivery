package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Component
public class BudgetDeltaFlusher {

    private final RedissonClient redisson;
    private final AdDeliveryRepo repo;
    private final JdbcTemplate jdbc;

    public BudgetDeltaFlusher(RedissonClient redisson, AdDeliveryRepo repo, DataSource ds) {
        this.redisson = redisson;
        this.repo = repo;
        this.jdbc = new JdbcTemplate(ds);
    }

    @Scheduled(fixedDelayString = "${budget.flush.interval.ms:200}")
    @Transactional
    public void flushDeltas() {
        List<Integer> ids = repo.findAllIds(); // add this projection method to your repo
        if (ids.isEmpty()) return;

        var codec = StringCodec.INSTANCE;
        List<Object[]> batch = new ArrayList<>();

        for (Integer id : ids) {
            RBucket<String> bucket = redisson.getBucket("campaign:delta:" + id, codec);
            String v = bucket.get();
            if (v == null || "0".equals(v)) continue;
            bucket.set("0"); // reset for next interval

            long cents;
            try { cents = Long.parseLong(v); } catch (Exception e) { continue; }
            if (cents <= 0) continue;

            BigDecimal delta = MoneyUtils.fromCents(cents);
            batch.add(new Object[]{ delta, id });
        }

        if (!batch.isEmpty()) {
            jdbc.batchUpdate(
                    "UPDATE campaign SET remaining_budget = remaining_budget - ? WHERE campaign_id = ?",
                    batch
            );
        }
    }
}
