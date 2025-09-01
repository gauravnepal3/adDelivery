package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import jakarta.annotation.PostConstruct;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;

@Service
public class CampaignCacheService {

    private static final Logger log = LoggerFactory.getLogger(CampaignCacheService.class);

    private final StringRedisTemplate zsetRedis;
    private final RedissonClient redisson;

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

    private String membershipKey(int campaignId) { return "campaign:zsetkeys:" + campaignId; }

    private static long toCents(BigDecimal bid) { return bid.movePointRight(2).longValueExact(); }

    private Integer parseMember(Object member) {
        if (member == null) return null;
        String s = member.toString();
        if (s.length() >= 2 && s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
            s = s.substring(1, s.length() - 1);
        }
        return Integer.valueOf(s);
    }

    // ------------------- write ops -------------------

    public void addCampaign(Campaign campaign, String country, String language, String os, String browser) {
        String key = filterKey(country, language, os, browser);
        String member = String.valueOf(campaign.getCampaignId());
        double score = (double) toCents(campaign.getBiddingRate());
        zsetRedis.opsForZSet().add(key, member, score);
        zsetRedis.opsForSet().add(membershipKey(campaign.getCampaignId()), key);
    }

    public void updateCampaignBudget(Campaign campaign, String country, String language, String os, String browser) {
        addCampaign(campaign, country, language, os, browser);
    }

    public void removeCampaignEverywhere(Integer campaignId) {
        String mKey = membershipKey(campaignId);
        Set<String> keys = zsetRedis.opsForSet().members(mKey);
        String member = String.valueOf(campaignId);
        if (keys != null) {
            for (String k : keys) {
                try { zsetRedis.opsForZSet().remove(k, member); }
                catch (Exception e) { log.warn("ZREM failed: {} from {}", campaignId, k, e); }
            }
        }
        zsetRedis.delete(mKey);
    }

    // ------------------- select + spend in one Lua -------------------

    private static final String SELECT_SPEND_LUA = """
    -- KEYS[1] = zset key: campaign:filters:<os>:<br>:<lang>:<country>
    -- ARGV[1] = nonce (optional), for deterministic random tie-break
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
      return {3, cid, rem} -- insufficient for one more
    end

    local newRem = remNum - bidNum
    redis.call('HSET', bkey, 'remaining', tostring(newRem))
    redis.call('INCRBY', 'campaign:delta:' .. cid, bidNum)

    if newRem <= 0 then
      return {2, cid, '0'} -- spent and hit zero
    else
      return {1, cid, tostring(newRem)} -- spent and still > 0
    end
    """;

    private String selectSpendSha;

    @PostConstruct
    public void loadScripts() {
        this.selectSpendSha = redisson.getScript(StringCodec.INSTANCE).scriptLoad(SELECT_SPEND_LUA);
    }

    public static final class SelectionResult {
        public int code;            // 0=no candidate, 1=served>0, 2=served==0, 3=insufficient
        public Integer campaignId;  // nullable
        public Long remaining;      // cents, nullable
    }

    /** One RT to Redis, atomic selection+spend. */
    public SelectionResult pickAndSpendOne(String country, String language, String os, String browser) {
        String key = filterKey(country, language, os, browser);
        @SuppressWarnings("unchecked")
        List<Object> res = (List<Object>) redisson.getScript(StringCodec.INSTANCE)
                .evalSha(RScript.Mode.READ_WRITE,
                        selectSpendSha,
                        RScript.ReturnType.MULTI,
                        Collections.singletonList(key),
                        UUID.randomUUID().toString());

        SelectionResult r = new SelectionResult();
        if (res == null || res.isEmpty()) { r.code = 0; return r; }
        r.code = ((Number) res.get(0)).intValue();
        if (res.size() > 1 && res.get(1) != null) r.campaignId = Integer.valueOf(res.get(1).toString());
        if (res.size() > 2 && res.get(2) != null) r.remaining = Long.valueOf(res.get(2).toString());
        return r;
    }

    // ------- keep this client-side fallback for debugging if you want -------
    public Integer getBestCampaignIdClient(String country, String language, String os, String browser) {
        String key = filterKey(country, language, os, browser);
        Set<ZSetOperations.TypedTuple<String>> topOne =
                zsetRedis.opsForZSet().reverseRangeWithScores(key, 0, 0);
        if (topOne == null || topOne.isEmpty()) return null;
        Double topScore = topOne.iterator().next().getScore();
        if (topScore == null) return null;
        Set<String> ties = zsetRedis.opsForZSet().rangeByScore(key, topScore, topScore);
        if (ties == null || ties.isEmpty()) return null;
        int pick = new Random().nextInt(ties.size());
        String chosen = ties.stream().skip(pick).findFirst().orElse(null);
        return parseMember(chosen);
    }
}