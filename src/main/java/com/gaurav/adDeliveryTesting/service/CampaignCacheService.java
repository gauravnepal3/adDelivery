// src/main/java/com/gaurav/adDeliveryTesting/service/CampaignCacheService.java
package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Service
public class CampaignCacheService {

    private final StringRedisTemplate redis;

    public CampaignCacheService(StringRedisTemplate redis) {
        this.redis = redis;
    }

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

    private static String part(String s) { return (s == null || s.isBlank()) ? "any" : s; }
    private static long toCents(BigDecimal bid) { return bid.movePointRight(2).longValueExact(); }

    // ---- Ops-based writers used by LazyIndexer ----
    public static void writeAllowSetOps(RedisOperations<String,String> ops, String key, Collection<String> values) {
        ops.delete(key);
        if (values == null || values.isEmpty()) return;
        ops.opsForSet().add(key, values.stream().filter(v -> v != null && !v.isBlank()).toArray(String[]::new));
    }
    public static void writeBlockSetOps(RedisOperations<String,String> ops, String key, Collection<String> values) {
        ops.delete(key);
        if (values == null || values.isEmpty()) return;
        ops.opsForSet().add(key, values.stream().filter(v -> v != null && !v.isBlank()).toArray(String[]::new));
    }
    public static Set<String> toLowerSet(Set<String> in) {
        Set<String> out = new HashSet<>();
        if (in == null) return out;
        for (String s : in) if (s != null && !s.isBlank()) out.add(s.trim().toLowerCase());
        return out;
    }
    public static void addCampaignToZsetOps(RedisOperations<String,String> ops, Campaign c,
                                            String country, String language, String device, String os) {
        String key = zsetKey(country, language, device, os);
        ops.opsForZSet().add(key, Integer.toString(c.getCampaignId()), (double) toCents(c.getBiddingRate()));
        ops.opsForSet().add("campaign:zsetkeys:" + c.getCampaignId(), key);
    }
}