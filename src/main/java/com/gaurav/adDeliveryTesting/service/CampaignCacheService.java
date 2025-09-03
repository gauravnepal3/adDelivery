package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class CampaignCacheService {

    private final StringRedisTemplate redis;

    public CampaignCacheService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    // ---------- key builders ----------
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

    private static String membershipKey(int id)  { return "campaign:zsetkeys:" + id; }

    private static String part(String s) { return (s == null || s.isBlank()) ? "any" : s; }
    private static long toCents(BigDecimal bid) { return bid.movePointRight(2).longValueExact(); }

    // ---------- helpers ----------
    public static Set<String> toLowerSet(Collection<String> in) {
        if (in == null || in.isEmpty()) return Set.of();
        Set<String> out = new HashSet<>(in.size());
        for (String s : in) {
            if (s == null) continue;
            String t = s.trim().toLowerCase();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    // ---------- non-pipelined small updates ----------
    public void writeAllowSet(String key, Collection<String> values) {
        redis.delete(key);
        if (values == null || values.isEmpty()) return;
        String[] arr = values.stream().filter(Objects::nonNull).map(String::trim).filter(v -> !v.isEmpty()).toArray(String[]::new);
        if (arr.length > 0) redis.opsForSet().add(key, arr);
    }
    public void writeBlockSet(String key, Collection<String> values) {
        redis.delete(key);
        if (values == null || values.isEmpty()) return;
        String[] arr = values.stream().filter(Objects::nonNull).map(String::trim).filter(v -> !v.isEmpty()).toArray(String[]::new);
        if (arr.length > 0) redis.opsForSet().add(key, arr);
    }
    public void addCampaignToZset(Campaign c, String country, String language, String device, String os) {
        String key = zsetKey(country, language, device, os);
        String member = Integer.toString(c.getCampaignId());
        double score = (double) toCents(c.getBiddingRate());
        redis.opsForZSet().add(key, member, score);
        redis.opsForSet().add(membershipKey(c.getCampaignId()), key);
    }

    /** Full cleanup of a campaign from Redis. */
    public void removeCampaignEverywhere(int campaignId) {
        String mKey = membershipKey(campaignId);
        var keys = redis.opsForSet().members(mKey);
        String member = Integer.toString(campaignId);
        if (keys != null) {
            for (String k : keys) {
                try { redis.opsForZSet().remove(k, member); } catch (Exception ignore) {}
            }
        }
        redis.delete(mKey);
        redis.delete(allowBrowserKey(campaignId));
        redis.delete(allowIabKey(campaignId));
        redis.delete(allowIpKey(campaignId));
        redis.delete(allowDomainKey(campaignId));
        redis.delete(blockIpKey(campaignId));
        redis.delete(blockDomainKey(campaignId));
        redis.delete("campaign:budget:" + campaignId);
        redis.delete("campaign:delta:" + campaignId);
    }

    // ---------- *** PIPELINED *** helpers ----------
    public static void writeAllowSetOps(RedisOperations<?,?> ops, String key, Collection<String> values) {
        @SuppressWarnings("unchecked")
        RedisOperations<String,String> rops = (RedisOperations<String,String>) ops;
        rops.delete(key);
        if (values == null || values.isEmpty()) return;
        String[] arr = values.stream().filter(Objects::nonNull).map(String::trim).filter(v -> !v.isEmpty()).toArray(String[]::new);
        if (arr.length > 0) rops.opsForSet().add(key, arr);
    }

    public static void writeBlockSetOps(RedisOperations<?,?> ops, String key, Collection<String> values) {
        @SuppressWarnings("unchecked")
        RedisOperations<String,String> rops = (RedisOperations<String,String>) ops;
        rops.delete(key);
        if (values == null || values.isEmpty()) return;
        String[] arr = values.stream().filter(Objects::nonNull).map(String::trim).filter(v -> !v.isEmpty()).toArray(String[]::new);
        if (arr.length > 0) rops.opsForSet().add(key, arr);
    }

    public static void addCampaignToZsetOps(RedisOperations<?,?> ops, Campaign c, String country, String language, String device, String os) {
        @SuppressWarnings("unchecked")
        RedisOperations<String,String> rops = (RedisOperations<String,String>) ops;
        String key = zsetKey(country, language, device, os);
        String member = Integer.toString(c.getCampaignId());
        double score = (double) toCents(c.getBiddingRate());
        rops.opsForZSet().add(key, member, score);
        rops.opsForSet().add(membershipKey(c.getCampaignId()), key);
    }

    // ---------- debug ----------
    public Set<ZSetOperations.TypedTuple<String>> debugRangeWithScores(String key) {
        return redis.opsForZSet().reverseRangeWithScores(key, 0, -1);
    }
    public String debugFilterKey(String country, String language, String device, String os) {
        return zsetKey(country, language, device, os);
    }
}