package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import org.redisson.api.RBucket;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;

@Component
public class BudgetDeltaFlusher {

    private final RedissonClient redisson;
    private final JdbcTemplate jdbc;

    public BudgetDeltaFlusher(RedissonClient redisson, AdDeliveryRepo repo, DataSource ds) {
        this.redisson = redisson;
        this.jdbc = new JdbcTemplate(ds);
    }

    @Scheduled(fixedDelayString = "${budget.flush.interval.ms:1000}")
    @Transactional
    public void flushDeltas() {
        // Be explicit: RSet<String>, not var/RSet<Object>
        RSet<String> touched = redisson.getSet("campaign:touched", StringCodec.INSTANCE);

        final int MAX_PER_FLUSH = 10_000; // tune
        java.util.List<Object[]> batch = new java.util.ArrayList<>(1024);

        for (int i = 0; i < MAX_PER_FLUSH; i++) {
            // removeRandom() now returns String
            String idStr = touched.removeRandom();
            if (idStr == null) break;

            int id;
            try { id = Integer.parseInt(idStr); }
            catch (NumberFormatException ignored) { continue; }

            RBucket<String> bucket = redisson.getBucket("campaign:delta:" + id, StringCodec.INSTANCE);
            String v = bucket.getAndSet("0");
            if (v == null || "0".equals(v)) continue;

            long cents;
            try { cents = Long.parseLong(v); } catch (Exception e) { continue; }
            if (cents <= 0) continue;

            batch.add(new Object[]{ com.gaurav.adDeliveryTesting.utils.MoneyUtils.fromCents(cents), id });
        }

        if (!batch.isEmpty()) {
            jdbc.batchUpdate(
                    "UPDATE campaign SET remaining_budget = remaining_budget - ? WHERE campaign_id = ?",
                    batch
            );
        }
    }
}
