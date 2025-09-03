package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.redisson.api.*;
import org.redisson.client.codec.StringCodec;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
// imports to add at top of CampaignCacheService.java
import org.redisson.api.RBatch;
import org.redisson.api.RBucketAsync;
import org.redisson.api.RMapAsync;
import org.redisson.api.RScoredSortedSetAsync;
import org.redisson.api.RSetAsync;
import org.redisson.client.codec.StringCodec;
@Service
public class CampaignCacheService {

    private static final Logger log = LoggerFactory.getLogger(CampaignCacheService.class);

    private final StringRedisTemplate redis;

    public CampaignCacheService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    // CampaignCacheService.java (inside the class)

    // Write one full campaign using a Redisson batch (no result collection).
    public static void writeCampaignToBatch(RBatch batch, Campaign c) {
        String idStr = Integer.toString(c.getCampaignId());

        // budget & delta
        RMapAsync<String,String> budgetMap =
                batch.getMap("campaign:budget:" + idStr, StringCodec.INSTANCE);
        budgetMap.fastPutAsync("remaining",
                Long.toString(c.getRemainingBudget() == null
                        ? 0L
                        : c.getRemainingBudget().movePointRight(2).longValueExact()));

        RBucketAsync<String> deltaBucket =
                batch.getBucket("campaign:delta:" + idStr, StringCodec.INSTANCE);
        deltaBucket.setAsync("0");

        CampaignFilters f = c.getFilters();
        if (f == null) return;

        // allow/block sets
        writeSetBatch(batch, allowBrowserKey(c.getCampaignId()), f.getBrowsers());
        writeSetBatch(batch, allowIabKey(c.getCampaignId()),     f.getIabCategory());
        writeSetBatch(batch, allowIpKey(c.getCampaignId()),      f.getAllowedIP());
        writeSetBatch(batch, allowDomainKey(c.getCampaignId()),  toLowerSet(f.getAllowedDomain()));

        writeSetBatch(batch, blockIpKey(c.getCampaignId()),      f.getExcludedIP());
        writeSetBatch(batch, blockDomainKey(c.getCampaignId()),  toLowerSet(f.getExcludedDomain()));

        // zset memberships
        for (String country : f.getCountries())
            for (String lang : f.getLanguages())
                for (String device : f.getDevices())
                    for (String os : f.getOsList()) {
                        String zkey = zsetKey(country, lang, device, os);
                        double score = (double) c.getBiddingRate().movePointRight(2).longValueExact();

                        RScoredSortedSetAsync<String> z =
                                batch.getScoredSortedSet(zkey, StringCodec.INSTANCE);
                        z.addAsync(score, idStr);

                        RSetAsync<String> membership =
                                batch.getSet(membershipKey(c.getCampaignId()), StringCodec.INSTANCE);
                        membership.addAsync(zkey);
                    }
    }

    // Batch set writer using async types from RBatch
    private static void writeSetBatch(RBatch batch, String key, java.util.Collection<String> vals) {
        RSetAsync<String> set = batch.getSet(key, StringCodec.INSTANCE);
        set.deleteAsync();
        if (vals == null || vals.isEmpty()) return;

        java.util.List<String> cleaned = vals.stream()
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(String::trim)
                .toList();

        if (!cleaned.isEmpty()) set.addAllAsync(cleaned);
    }

    // ---------- Key builders ----------

    public static String zsetKey(String country, String language, String device, String os) {
        return "campaign:filters:" + part(os) + ":" + part(device) + ":" + part(language) + ":" + part(country);
    }

    public static String rrKey(String country, String language, String device, String os) {
        return "campaign:rr:" + part(os) + ":" + part(device) + ":" + part(language) + ":" + part(country);
    }

    public static String allowBrowserKey(int id) { return "campaign:allow:browser:" + id; }
    public static String allowIabKey(int id)     { return "campaign:allow:iab:" + id; }
    public static String allowIpKey(int id)      { return "campaign:allow:ip:" + id; }
    public static String allowDomainKey(int id)  { return "campaign:allow:domain:" + id; }

    public static String blockIpKey(int id)      { return "campaign:block:ip:" + id; }
    public static String blockDomainKey(int id)  { return "campaign:block:domain:" + id; }

    public static String membershipKey(int id)   { return "campaign:zsetkeys:" + id; }

    private static String part(String s) {
        return (s == null || s.isBlank()) ? "any" : s;
    }

    private static long toCents(BigDecimal bid) {
        return bid.movePointRight(2).longValueExact();
    }

    // ---------- ZSET membership (non-pipelined) ----------

    /** Add a campaign to the coarse ZSET (country/lang/device/os) and track reverse membership for clean removal. */
    public void addCampaignToZset(Campaign c, String country, String language, String device, String os) {
        String key = zsetKey(country, language, device, os);
        String member = Integer.toString(c.getCampaignId());
        double score = (double) toCents(c.getBiddingRate());
        redis.opsForZSet().add(key, member, score);
        redis.opsForSet().add(membershipKey(c.getCampaignId()), key);
        // Optional TTL to prevent unbounded growth if you use lazy indexing:
        // redis.expire(key, Duration.ofHours(6));
    }

    /** Remove campaign from every coarse ZSET it was added to (uses the tracked membership set). */
    public void removeCampaignEverywhere(int campaignId) {
        String mKey = membershipKey(campaignId);
        Set<String> keys = redis.opsForSet().members(mKey);
        if (keys != null && !keys.isEmpty()) {
            String member = Integer.toString(campaignId);
            for (String k : keys) {
                try {
                    redis.opsForZSet().remove(k, member);
                } catch (Exception e) {
                    log.warn("ZREM failed for campaign {} on key {}", campaignId, k, e);
                }
            }
        }
        redis.delete(mKey);
        // Optional: also clear allow/block sets and budget entries if you’re fully de-indexing
        // redis.delete(allowBrowserKey(campaignId));
        // redis.delete(allowIabKey(campaignId));
        // redis.delete(allowIpKey(campaignId));
        // redis.delete(allowDomainKey(campaignId));
        // redis.delete(blockIpKey(campaignId));
        // redis.delete(blockDomainKey(campaignId));
        // redis.delete("campaign:budget:" + campaignId);
        // redis.delete("campaign:delta:" + campaignId);
    }

    // ---------- Allow/Block set writers (non-pipelined) ----------

    /** Writes an allow set. Empty set = no constraint (we store nothing). */
    public void writeAllowSet(String key, Collection<String> values) {
        redis.delete(key);
        if (values == null || values.isEmpty()) return; // empty => no constraint
        for (String v : values) {
            if (v == null) continue;
            String t = v.trim();
            if (!t.isEmpty()) redis.opsForSet().add(key, t);
        }
    }

    /** Writes a block set. Empty set = nothing blocked. */
    public void writeBlockSet(String key, Collection<String> values) {
        redis.delete(key);
        if (values == null || values.isEmpty()) return;
        for (String v : values) {
            if (v == null) continue;
            String t = v.trim();
            if (!t.isEmpty()) redis.opsForSet().add(key, t);
        }
    }

    /** Lowercase helper for domains before writeAllowSet/writeBlockSet. */
    public static java.util.Set<String> toLowerSet(java.util.Set<String> in) {
        java.util.Set<String> out = new java.util.HashSet<>();
        if (in == null) return out;
        for (String s : in) {
            if (s == null) continue;
            String t = s.trim().toLowerCase();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    // ---------- Pipelined helpers (for WarmService/TargetingWarmup lazy indexers) ----------

    /** Pipelined add to ZSET + membership set. */
    public static void addCampaignToZsetOps(RedisOperations<String, String> ops,
                                            Campaign c, String country, String language, String device, String os) {
        String key = zsetKey(country, language, device, os);
        String member = Integer.toString(c.getCampaignId());
        double score = (double) toCents(c.getBiddingRate());
        ops.opsForZSet().add(key, member, score);
        ops.opsForSet().add(membershipKey(c.getCampaignId()), key);
        // ops.expire(key, Duration.ofHours(6)); // optional TTL
    }

    /** Pipelined allow writer. */
    public static void writeAllowSetOps(RedisOperations<String, String> ops, String key, Collection<String> values) {
        ops.delete(key);
        if (values == null || values.isEmpty()) return;
        // collapse to one SADD by passing varargs
        ops.opsForSet().add(key, values.stream()
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(String::trim)
                .toArray(String[]::new));
    }

    /** Pipelined block writer. */
    public static void writeBlockSetOps(RedisOperations<String, String> ops, String key, Collection<String> values) {
        ops.delete(key);
        if (values == null || values.isEmpty()) return;
        ops.opsForSet().add(key, values.stream()
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(String::trim)
                .toArray(String[]::new));
    }

    // ---------- Debug helpers ----------

    public Set<ZSetOperations.TypedTuple<String>> debugRangeWithScores(String key) {
        return redis.opsForZSet().reverseRangeWithScores(key, 0, -1);
    }

    public String debugFilterKey(String country, String language, String device, String os) {
        return zsetKey(country, language, device, os);
    }
}