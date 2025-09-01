package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class CampaignCacheService {

    private static final Logger log = LoggerFactory.getLogger(CampaignCacheService.class);

    private final StringRedisTemplate redis;   // plain strings; good for ZSET/HASH ops
    private final RedissonClient redisson;

    public CampaignCacheService(StringRedisTemplate stringRedisTemplate, RedissonClient redisson) {
        this.redis = stringRedisTemplate;
        this.redisson = redisson;
    }

    // ========= utilities / keys =========

    String filterKey(String country, String language, String os, String browser) {
        return String.format("campaign:filters:%s:%s:%s:%s",
                (os == null ? "any" : os),
                (browser == null ? "any" : browser),
                (language == null ? "any" : language),
                (country == null ? "any" : country));
    }

    private String membershipKey(int campaignId) {
        return "campaign:zsetkeys:" + campaignId; // SET of all ZSET keys this id was inserted into
    }

    private String budgetKey(int campaignId) {
        return "campaign:budget:" + campaignId; // HASH {remaining, bid} in CENTS
    }

    private String deltaKey(int campaignId) {
        return "campaign:delta:" + campaignId;  // STRING INCRBY cents to flush to DB
    }

    private static long toCents(BigDecimal x) {
        return x.movePointRight(2).longValueExact();
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

    // ========= write side (warming/maintenance) =========

    public void seedBudget(Campaign c) {
        int id = c.getCampaignId();
        long rem = toCents(c.getRemainingBudget());
        long bid = toCents(c.getBiddingRate());
        String bkey = budgetKey(id);
        redis.opsForHash().put(bkey, "remaining", Long.toString(rem));
        redis.opsForHash().put(bkey, "bid", Long.toString(bid));
    }

    /** Add campaign to a specific (os, browser, language, country) combo. */
    public void addCampaign(Campaign campaign, String country, String language, String os, String browser) {
        String key = filterKey(country, language, os, browser);
        String member = String.valueOf(campaign.getCampaignId());
        double score = (double) toCents(campaign.getBiddingRate()); // rank by integer cents
        redis.opsForZSet().add(key, member, score);
        redis.opsForSet().add(membershipKey(campaign.getCampaignId()), key);
    }

    /** Warm ALL wildcard permutations for a campaign (any in each dimension). */
    public void addAllCombos(Campaign c) {
        Set<String> countries = emptySafe(c.getFilters().getCountries());
        Set<String> languages = emptySafe(c.getFilters().getLanguages());
        Set<String> oses      = emptySafe(c.getFilters().getOsList());
        Set<String> browsers  = emptySafe(c.getFilters().getBrowsers());

        List<String> cc = withAny(countries);
        List<String> ll = withAny(languages);
        List<String> oo = withAny(oses);
        List<String> bb = withAny(browsers);

        for (String country : cc)
            for (String lang : ll)
                for (String os : oo)
                    for (String br : bb)
                        addCampaign(c, countryOrNull(country), langOrNull(lang), osOrNull(os), brOrNull(br));
    }

    private static Set<String> emptySafe(Set<String> s) { return (s == null ? Set.of() : s); }
    private static List<String> withAny(Set<String> s) {
        List<String> r = new ArrayList<>(s);
        r.add("any");
        return r;
    }
    private static String countryOrNull(String s){ return "any".equals(s) ? null : s; }
    private static String langOrNull(String s){ return "any".equals(s) ? null : s; }
    private static String osOrNull(String s){ return "any".equals(s) ? null : s; }
    private static String brOrNull(String s){ return "any".equals(s) ? null : s; }

    /** Keep key hot after serve (idempotent). */
    public void updateCampaignBudget(Campaign c, String country, String language, String os, String browser) {
        addCampaign(c, country, language, os, browser);
    }

    /** Remove id from every ZSET it belongs to. */
    public void removeCampaignEverywhere(Integer campaignId) {
        String mKey = membershipKey(campaignId);
        Set<String> keys = redis.opsForSet().members(mKey);
        String member = String.valueOf(campaignId);
        if (keys != null) {
            for (String k : keys) {
                try { redis.opsForZSet().remove(k, member); }
                catch (Exception e) { log.warn("ZREM failed: campaign {} from key {}", campaignId, k, e); }
            }
        }
        redis.delete(mKey);
    }

    // ========= single-round-trip Lua: select + spend =========

    private static final String SELECT_SPEND_LUA = """
        -- KEYS[1] = zset key: campaign:filters:<os>:<br>:<lang>:<country>
        -- ARGV[1] = nonce
        local top = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
        if (not top) or (#top == 0) then return {0} end
        local topScore = tonumber(top[2])
        if (not topScore) then return {0} end
        local ties = redis.call('ZRANGEBYSCORE', KEYS[1], topScore, topScore)
        if (not ties) or (#ties == 0) then return {0} end

        local nonce = ARGV[1]
        if (not nonce) or (nonce == '') then
          local t = redis.call('TIME'); nonce = t[1] .. t[2]
        end
        local h = redis.sha1hex(nonce)
        local n = tonumber(string.sub(h, 1, 8), 16)
        local idx = (n % #ties) + 1
        local cid = ties[idx]

        local bkey = 'campaign:budget:' .. cid
        local rem = redis.call('HGET', bkey, 'remaining')
        local bid = redis.call('HGET', bkey, 'bid')
        if (not rem) or (not bid) then return {0} end

        local remNum = tonumber(rem)
        local bidNum = tonumber(bid)
        if (not remNum) or (not bidNum) then return {0} end

        if remNum < bidNum then
          return {3, cid, rem} -- insufficient
        end

        local newRem = remNum - bidNum
        redis.call('HSET', bkey, 'remaining', tostring(newRem))
        redis.call('INCRBY', 'campaign:delta:' .. cid, bidNum)

        if newRem <= 0 then
          return {2, cid, '0'}
        else
          return {1, cid, tostring(newRem)}
        end
    """;

    private String selectSpendSha;

    @PostConstruct
    public void loadScripts() {
        this.selectSpendSha = redisson.getScript(StringCodec.INSTANCE).scriptLoad(SELECT_SPEND_LUA);
    }

    public static final class SelectionResult {
        public int code;           // 0=no candidate, 1=served>0, 2=served==0, 3=insufficient
        public Integer campaignId; // nullable
        public Long remaining;     // cents, nullable
    }

    public SelectionResult pickAndSpendOne(String country, String language, String os, String browser) {
        String key = filterKey(country, language, os, browser);
        @SuppressWarnings("unchecked")
        List<Object> res = (List<Object>) redisson.getScript(StringCodec.INSTANCE)
                .evalSha(RScript.Mode.READ_WRITE,
                        selectSpendSha,
                        RScript.ReturnType.MULTI,
                        Collections.singletonList(key),
                        java.util.UUID.randomUUID().toString());

        SelectionResult r = new SelectionResult();
        if (res == null || res.isEmpty()) { r.code = 0; return r; }
        r.code = ((Number) res.get(0)).intValue();
        if (res.size() > 1 && res.get(1) != null) r.campaignId = Integer.valueOf(res.get(1).toString());
        if (res.size() > 2 && res.get(2) != null) r.remaining = Long.valueOf(res.get(2).toString());
        return r;
    }

    // ========= optional debug helpers =========

    public Set<ZSetOperations.TypedTuple<String>> debugRangeWithScores(String key) {
        return redis.opsForZSet().reverseRangeWithScores(key, 0, -1);
    }
    public String debugFilterKey(String country, String language, String os, String browser) {
        return filterKey(country, language, os, browser);
    }
}