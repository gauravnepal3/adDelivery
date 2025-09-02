package com.gaurav.adDeliveryTesting.service;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class ServeScriptService {

    private final RedissonClient redisson;
    private final CampaignCacheService cache; // for filter key builder

    public ServeScriptService(RedissonClient redisson, CampaignCacheService cache) {
        this.redisson = redisson;
        this.cache = cache;
    }

    // Return code: 0=no candidate or no budget; 1=spent; 2=spent and exhausted
    public record ServeResult(int code, Integer id, Long newRemainingCents) {}

    private static final String SERVE_LUA = """
-- KEYS[1] = zset key: campaign:filters:{os}:{browser}:{lang}:{country}
-- KEYS[2] = touched set key: campaign:touched
-- KEYS[3] = round-robin key for this filter (string key), e.g. rr:{os}:{browser}:{lang}:{country}
-- ARGV[1] = budgetKeyPrefix, e.g., "campaign:budget:"
-- ARGV[2] = deltaKeyPrefix,  e.g., "campaign:delta:"

-- 1) Find top score (highest bid)
local top = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if (not top) or (#top == 0) then return {0} end
local topScore = tonumber(top[2])
if (not topScore) then return {0} end

-- 2) Gather all ties at that score
local ties = redis.call('ZRANGEBYSCORE', KEYS[1], topScore, topScore)
if (not ties) or (#ties == 0) then return {0} end

-- 3) Round-robin among ties (deterministic, cheap)
local rr = redis.call('INCR', KEYS[3])
local idx = ((rr - 1) % #ties) + 1
local id = ties[idx]

-- 4) Spend using HINCRBY (optimistic)
local budgetKey = ARGV[1] .. id
local deltaKey  = ARGV[2] .. id

local newRem = redis.call('HINCRBY', budgetKey, 'remaining', -topScore)
if (not newRem) then
  -- field/key missing → revert to previous state
  return {0, id, nil}
end

if newRem < 0 then
  -- not enough budget: revert and report no-spend
  redis.call('HINCRBY', budgetKey, 'remaining', topScore)
  return {0, id, newRem + topScore}
end

-- 5) Record delta and touch set
redis.call('INCRBY', deltaKey, topScore)
redis.call('SADD', KEYS[2], id)

if newRem <= 0 then return {2, id, 0} end
return {1, id, newRem}
""";

    public ServeResult pickAndSpend(String country, String language, String os, String browser) {
        String zsetKey = cache.debugFilterKey(country, language, os, browser);
        String rrKey   = "campaign:rr:" + (os == null ? "any" : os)
                + ":" + (browser == null ? "any" : browser)
                + ":" + (language == null ? "any" : language)
                + ":" + (country == null ? "any" : country);

        // Defensive: ensure none of the KEYS are null
        if (zsetKey == null || rrKey == null) {
            return new ServeResult(0, null, null);
        }

        @SuppressWarnings("unchecked")
        List<Object> res = (List<Object>) redisson.getScript(StringCodec.INSTANCE).eval(
                RScript.Mode.READ_WRITE,
                SERVE_LUA,
                RScript.ReturnType.MULTI,
                // >>> THREE KEYS here <<<
                java.util.Arrays.asList(zsetKey, "campaign:touched", rrKey),
                // ARGV
                "campaign:budget:",
                "campaign:delta:"
        );

        if (res == null || res.isEmpty()) return new ServeResult(0, null, null);
        int code = (res.get(0) instanceof Number n) ? n.intValue() : Integer.parseInt(res.get(0).toString());
        Integer id = (res.size() > 1 && res.get(1) != null) ? Integer.valueOf(res.get(1).toString()) : null;
        Long newRem = (res.size() > 2 && res.get(2) != null) ? Long.valueOf(res.get(2).toString()) : null;
        return new ServeResult(code, id, newRem);
    }
    private static int toInt(Object o) {
        if (o == null) return 0;
        return (o instanceof Number n) ? n.intValue() : Integer.parseInt(o.toString());
    }
}