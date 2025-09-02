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
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
            // Break into chunks of, say, 2000 rows
            int chunkSize = 2000;
            for (int i = 0; i < batch.size(); i += chunkSize) {
                List<Object[]> sub = batch.subList(i, Math.min(i + chunkSize, batch.size()));

                String placeholders = IntStream.range(0, sub.size())
                        .mapToObj(j -> "(?,?)")
                        .collect(Collectors.joining(","));

                String sql = """
            UPDATE campaign c
            SET remaining_budget = c.remaining_budget - v.delta
            FROM (VALUES %s) AS v(id, delta)
            WHERE c.campaign_id = v.id
        """.formatted(placeholders);

                jdbc.update(con -> {
                    PreparedStatement ps = con.prepareStatement(sql);
                    int idx = 1;
                    for (Object[] row : sub) {
                        ps.setInt(idx++, (Integer) row[1]);           // id
                        ps.setBigDecimal(idx++, (BigDecimal) row[0]); // delta
                    }
                    return ps;
                });
            }
        }
    }
}
