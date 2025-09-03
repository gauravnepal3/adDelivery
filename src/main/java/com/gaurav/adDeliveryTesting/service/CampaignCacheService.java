package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Set;

@Service
public class CampaignCacheService {

    private final StringRedisTemplate redis;
    @Autowired
    private CampaignMetadataCache meta;
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

    private static String part(String s) {
        return (s == null || s.isBlank()) ? "any" : s;
    }

    private static long toCents(BigDecimal bid) {
        return bid.movePointRight(2).longValueExact();
    }

    // ---------- Non-pipelined helpers (used at request-time occasionally) ----------

    public void addCampaignToZset(Campaign c, String country, String language, String device, String os) {
        String key = zsetKey(country, language, device, os);
        String member = Integer.toString(c.getCampaignId());
        double score = (double) toCents(c.getBiddingRate());
        redis.opsForZSet().add(key, member, score);
        redis.opsForSet().add("campaign:zsetkeys:" + c.getCampaignId(), key);
    }

    public void writeAllowSet(String key, Collection<String> values) {
        redis.delete(key);
        if (values == null || values.isEmpty()) return;
        for (String v : values) {
            if (v == null) continue;
            String t = v.trim();
            if (!t.isEmpty()) redis.opsForSet().add(key, t);
        }
    }

    public void writeBlockSet(String key, Collection<String> values) {
        redis.delete(key);
        if (values == null || values.isEmpty()) return;
        for (String v : values) {
            if (v == null) continue;
            String t = v.trim();
            if (!t.isEmpty()) redis.opsForSet().add(key, t);
        }
    }

    public java.util.Set<String> lowerAll(java.util.Set<String> in) {
        java.util.Set<String> out = new java.util.HashSet<>();
        if (in == null) return out;
        for (String s : in) {
            if (s == null) continue;
            String t = s.trim().toLowerCase();
            if (!t.isEmpty()) out.add(t);
        }
        return out;
    }

    public Set<ZSetOperations.TypedTuple<String>> debugRangeWithScores(String key) {
        return redis.opsForZSet().reverseRangeWithScores(key, 0, -1);
    }

    public String debugFilterKey(String country, String language, String device, String os) {
        return zsetKey(country, language, device, os);
    }

    // ---------- Pipelined writers (used during warmup) ----------

    public void addCampaignToZsetPipelined(StringRedisConnection c, Campaign campaign,
                                           String country, String language, String device, String os) {
        String key    = zsetKey(country, language, device, os);
        String member = Integer.toString(campaign.getCampaignId());
        double score  = (double) toCents(campaign.getBiddingRate());

        // ZADD
        c.zAdd(key, score, member);
        // track membership
        c.sAdd(("campaign:zsetkeys:" + campaign.getCampaignId()), key);
    }

    public void writeAllowSetPipelined(StringRedisConnection c, String key, Collection<String> values) {
        c.del(key);
        if (values == null || values.isEmpty()) return;
        for (String v : values) {
            if (v == null) continue;
            String t = v.trim();
            if (!t.isEmpty()) c.sAdd(key, t);
        }
    }
    public void removeCampaignEverywhere(int campaignId) {
        // Remove all zset memberships for this campaign
        String zsetKeySet = "campaign:zsetkeys:" + campaignId;
        var keys = redis.opsForSet().members(zsetKeySet);
        if (keys != null) {
            for (String key : keys) {
                redis.opsForZSet().remove(key, String.valueOf(campaignId));
            }
            redis.delete(zsetKeySet);
        }

        // Remove allow/block sets
        redis.delete(allowBrowserKey(campaignId));
        redis.delete(allowIabKey(campaignId));
        redis.delete(allowIpKey(campaignId));
        redis.delete(allowDomainKey(campaignId));
        redis.delete(blockIpKey(campaignId));
        redis.delete(blockDomainKey(campaignId));

        // Remove budget keys
        redis.delete("campaign:budget:" + campaignId);
        redis.delete("campaign:delta:" + campaignId);

        // Optionally: also remove from touched set
        redis.opsForSet().remove("campaign:touched", String.valueOf(campaignId));

    }

    public void reindexCampaign(Campaign c) {
        int id = c.getCampaignId();

        // --- Redis ---
        redis.delete(allowBrowserKey(id));
        redis.delete(allowIabKey(id));
        redis.delete(allowIpKey(id));
        redis.delete(allowDomainKey(id));
        redis.delete(blockIpKey(id));
        redis.delete(blockDomainKey(id));

        String zsetTracker = "campaign:zsetkeys:" + id;
        Set<String> zsets = redis.opsForSet().members(zsetTracker);
        if (zsets != null) {
            for (String z : zsets) {
                redis.opsForZSet().remove(z, String.valueOf(id));
            }
        }
        redis.delete(zsetTracker);

        indexCampaign(c); // put fresh filters back

        // --- Caffeine ---
        // after you’ve rebuilt all Redis keys for this campaign:
        meta.invalidate(id);             // drop old per-campaign snapshot
        meta.reloadFromDb(id);           // load fresh snapshot now
        meta.invalidateCampaignListCache(); // (optional) force the @Cacheable("campaign") list to refresh on next call   // load fresh snapshot
    }

    public void indexCampaign(Campaign c) {
        var f = c.getFilters();
        if (f == null) return;

        // Allow/deny sets
        writeAllowSet(allowBrowserKey(c.getCampaignId()), f.getBrowsers());
        writeAllowSet(allowIabKey(c.getCampaignId()),     f.getIabCategory());
        writeAllowSet(allowIpKey(c.getCampaignId()),      f.getAllowedIP());
        writeAllowSet(allowDomainKey(c.getCampaignId()),  lowerAll(f.getAllowedDomain()));

        writeBlockSet(blockIpKey(c.getCampaignId()),      f.getExcludedIP());
        writeBlockSet(blockDomainKey(c.getCampaignId()),  lowerAll(f.getExcludedDomain()));

        // ZSET memberships (coarse)
        for (String country : f.getCountries())
            for (String lang    : f.getLanguages())
                for (String device  : f.getDevices())
                    for (String os   : f.getOsList()) {
                        addCampaignToZset(c, country, lang, device, os);
                    }
    }

    public void writeBlockSetPipelined(StringRedisConnection c, String key, Collection<String> values) {
        c.del(key);
        if (values == null || values.isEmpty()) return;
        for (String v : values) {
            if (v == null) continue;
            String t = v.trim();
            if (!t.isEmpty()) c.sAdd(key, t);
        }
    }
}