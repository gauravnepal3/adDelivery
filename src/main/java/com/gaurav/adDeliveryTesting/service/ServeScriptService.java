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
        -- ARGV[1] = budgetKeyPrefix, e.g., "campaign:budget:"
        -- ARGV[2] = deltaKeyPrefix,  e.g., "campaign:delta:"

        -- Get top score (highest bid)
        local top = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
        if (not top) or (#top == 0) then return {0} end
        local topScore = tonumber(top[2])
        if (not topScore) then return {0} end

        -- Collect all ties at that score
        local ties = redis.call('ZRANGEBYSCORE', KEYS[1], topScore, topScore)
        if (not ties) or (#ties == 0) then return {0} end

        -- Pick random among ties
        local t = redis.call('TIME')
        local seed = tonumber(t[1]) * 1000000 + tonumber(t[2])
        math.randomseed(seed)
        local idx = math.random(#ties)
        local id = ties[idx]

        -- Spend 'bid' equal to the ZSET score (in integer cents)
        local bid = topScore

        local budgetKey = ARGV[1] .. id      -- campaign:budget:{id}
        local deltaKey  = ARGV[2] .. id      -- campaign:delta:{id}

        local rem = redis.call('HGET', budgetKey, 'remaining')
        if not rem then return {0, id, nil} end

        local remNum = tonumber(rem)
        if remNum < bid then return {0, id, rem} end

        local newRem = remNum - bid
        redis.call('HSET', budgetKey, 'remaining', tostring(newRem))
        redis.call('INCRBY', deltaKey, bid)
        redis.call('SADD', KEYS[2], id)

        if newRem <= 0 then return {2, id, '0'} end
        return {1, id, tostring(newRem)}
    """;

    /** Executes pick+spend in ONE RTT. */
    public ServeResult pickAndSpend(String country, String language, String os, String browser) {
        String zsetKey = cache.debugFilterKey(country, language, os, browser);
        @SuppressWarnings("unchecked")
        List<Object> res = (List<Object>) redisson.getScript(StringCodec.INSTANCE).eval(
                RScript.Mode.READ_WRITE,
                SERVE_LUA,
                RScript.ReturnType.MULTI,
                Arrays.asList(zsetKey, "campaign:touched"),
                "campaign:budget:",
                "campaign:delta:"
        );

        if (res == null || res.isEmpty()) return new ServeResult(0, null, null);
        int code = toInt(res.get(0));
        Integer id = (res.size() > 1 && res.get(1) != null) ? Integer.valueOf(res.get(1).toString()) : null;
        Long newRem = (res.size() > 2 && res.get(2) != null) ? Long.valueOf(res.get(2).toString()) : null;
        return new ServeResult(code, id, newRem);
    }

    private static int toInt(Object o) {
        if (o == null) return 0;
        return (o instanceof Number n) ? n.intValue() : Integer.parseInt(o.toString());
    }
}