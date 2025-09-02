package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.config.RedisCacheConfig;
import com.gaurav.adDeliveryTesting.model.Campaign;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class CampaignCacheService {

    private static final Logger log = LoggerFactory.getLogger(CampaignCacheService.class);

    // Use Spring Boot's auto-configured StringRedisTemplate (plain string keys/values)
    private final StringRedisTemplate zsetRedis;
    private final RedissonClient redisson;
    @Autowired
    private RedisCacheConfig cacheConfig;
    public CampaignCacheService(StringRedisTemplate stringRedisTemplate, RedissonClient redisson) {
        this.zsetRedis = stringRedisTemplate;
        this.redisson = redisson;
    }

    private String filterKey(String country, String language, String os, String browser) {
        return String.format("campaign:filters:%s:%s:%s:%s",
                (os == null ? "any" : os),
                (browser == null ? "any" : browser),
                (language == null ? "any" : language),
                (country == null ? "any" : country));
    }

    private String membershipKey(int campaignId) {
        // A SET containing all ZSET keys this campaign was added to
        return "campaign:zsetkeys:" + campaignId;
    }

    private static long toCents(BigDecimal bid) {
        // rely on DB having scale=2; if not, you can setScale(2) first
        return bid.movePointRight(2).longValueExact();
    }

    /** Convert any legacy member like ""1"" → "1" then parse to Integer. */
    private Integer parseMember(Object member) {
        if (member == null) return null;
        String s = member.toString();
        if (s.length() >= 2 && s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
            s = s.substring(1, s.length() - 1);
        }
        return Integer.valueOf(s);
    }

    // =======================
    // Write operations
    // =======================

    public void addCampaign(Campaign campaign, String country, String language, String os, String browser) {
        String key = filterKey(country, language, os, browser);
        String member = String.valueOf(campaign.getCampaignId());
        double score = (double) toCents(campaign.getBiddingRate()); // integer cents as double
        zsetRedis.opsForZSet().add(key, member, score);
        // Track membership for O(K) removals later (K = number of keys this id is in)
        zsetRedis.opsForSet().add(membershipKey(campaign.getCampaignId()), key);
    }

    public void updateCampaignBudget(Campaign campaign, String country, String language, String os, String browser) {
        addCampaign(campaign, country, language, os, browser);
    }

    /** Remove this campaign from all ZSETs it belongs to (using the tracked membership set). */
    public void removeCampaignEverywhere(Integer campaignId) {
        String mKey = membershipKey(campaignId);
        Set<String> keys = zsetRedis.opsForSet().members(mKey);
        String member = String.valueOf(campaignId);

        if (keys != null) {
            for (String k : keys) {
                try {
                    zsetRedis.opsForZSet().remove(k, member);
                } catch (Exception e) {
                    log.warn("ZREM failed: campaign {} from key {}", campaignId, k, e);
                }
            }
        }
        zsetRedis.delete(mKey);
    }


    public Integer getBestCampaignId(String country, String language, String os, String browser) {
        String key = filterKey(country, language, os, browser);

        final String LUA = """
            -- KEYS[1] = zset key
            local top = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
            if (not top) or (#top == 0) then return {0} end
            local topScore = tonumber(top[2])
            if (not topScore) then return {0} end
            local ties = redis.call('ZRANGEBYSCORE', KEYS[1], topScore, topScore)
            if (not ties) or (#ties == 0) then return {0} end
            -- randomize among ties; seed with TIME so concurrent calls vary
            local t = redis.call('TIME')
            local seed = tonumber(t[1]) * 1000000 + tonumber(t[2])
            math.randomseed(seed)
            local idx = math.random(#ties)
            return {1, ties[idx]}
        """;

        try {
            RScript rscript = redisson.getScript(StringCodec.INSTANCE);
            @SuppressWarnings("unchecked")
            List<Object> res = (List<Object>) rscript.eval(
                    RScript.Mode.READ_ONLY,
                    LUA,
                    RScript.ReturnType.MULTI,
                    Collections.singletonList(key)
            );

            if (res == null || res.isEmpty()) return null;
            Number code = (Number) res.get(0);
            if (code == null || code.intValue() != 1) return null;

            Object member = (res.size() > 1 ? res.get(1) : null);
            return parseMember(member);
        } catch (Exception e) {
            log.warn("Lua tie-break failed on key {}. Falling back to client-side tie-break.", key, e);
            return getBestCampaignIdClient(country, language, os, browser);
        }
    }


    private Integer getBestCampaignIdClient(String country, String language, String os, String browser) {
        String key = filterKey(country, language, os, browser);

        Set<ZSetOperations.TypedTuple<String>> topOne =
                zsetRedis.opsForZSet().reverseRangeWithScores(key, 0, 0);
        if (topOne == null || topOne.isEmpty()) return null;

        ZSetOperations.TypedTuple<String> t = topOne.iterator().next();
        Double topScore = t.getScore();
        if (topScore == null) return null;

        // exact match (integer cents)
        Set<String> ties = zsetRedis.opsForZSet().rangeByScore(key, topScore, topScore);
        if (ties == null || ties.isEmpty()) return null;

        int pick = ThreadLocalRandom.current().nextInt(ties.size());
        String chosen = ties.stream().skip(pick).findFirst().orElse(null);
        return parseMember(chosen);
    }


    public Set<ZSetOperations.TypedTuple<String>> debugRangeWithScores(String key) {
        return zsetRedis.opsForZSet().reverseRangeWithScores(key, 0, -1);
    }

    public String debugFilterKey(String country, String language, String os, String browser) {
        return filterKey(country, language, os, browser);
    }
}