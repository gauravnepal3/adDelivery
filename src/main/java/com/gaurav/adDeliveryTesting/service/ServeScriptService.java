package com.gaurav.adDeliveryTesting.service;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Single-RTT pick+spend Redis path with a cheaper tie-break:
 *  - Finds top score
 *  - Uses ZCOUNT for number of ties
 *  - Uses RR counter + ZREVRANGEBYSCORE LIMIT 1 to fetch exactly one id
 *  - Spends via HINCRBY (optimistic), records delta & touched
 *
 * KEYS:
 *   KEYS[1] = ZSET key: campaign:filters:{os}:{browser}:{lang}:{country}
 *   KEYS[2] = touched set: "campaign:touched"
 *   KEYS[3] = RR key for this filter: campaign:rr:{os}:{browser}:{lang}:{country}
 * ARGV:
 *   ARGV[1] = budgetKeyPrefix (e.g., "campaign:budget:")
 *   ARGV[2] = deltaKeyPrefix  (e.g., "campaign:delta:")
 */
@Service
public class ServeScriptService {

    private static final String TOUCHED_SET = "campaign:touched";
    private static final String BUDGET_PREFIX = "campaign:budget:";
    private static final String DELTA_PREFIX = "campaign:delta:";

    // Cheaper tie logic: ZCOUNT + ZREVRANGEBYSCORE ... LIMIT 1
    private static final String SERVE_LUA = """
        -- KEYS[1] = zset key: campaign:filters:{os}:{browser}:{lang}:{country}
        -- KEYS[2] = touched set key
        -- KEYS[3] = round-robin key (string)
        -- ARGV[1] = budgetKeyPrefix, e.g., "campaign:budget:"
        -- ARGV[2] = deltaKeyPrefix,  e.g., "campaign:delta:"
        
        -- 1) Top score
        local top = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
        if (not top) or (#top == 0) then return {0} end
        local topScore = tonumber(top[2])
        if (not topScore) then return {0} end
        
        -- 2) Count ties (avoid fetching large arrays)
        local tieCount = redis.call('ZCOUNT', KEYS[1], topScore, topScore)
        if (not tieCount) or (tieCount == 0) then return {0} end
        
        -- 3) Round-robin index and fetch exactly one id
        local rr = redis.call('INCR', KEYS[3])
        local offset = (rr - 1) % tieCount
        local ids = redis.call('ZREVRANGEBYSCORE', KEYS[1], topScore, topScore, 'LIMIT', offset, 1)
        if (not ids) or (#ids == 0) then return {0} end
        local id = ids[1]
        
        -- 4) Spend using HINCRBY (optimistic)
        local budgetKey = ARGV[1] .. id
        local deltaKey  = ARGV[2] .. id
        
        local newRem = redis.call('HINCRBY', budgetKey, 'remaining', -topScore)
        if (not newRem) then
          -- missing field/key → treat as no-spend
          return {0, id, nil}
        end
        
        if newRem < 0 then
          -- insufficient budget → revert
          redis.call('HINCRBY', budgetKey, 'remaining', topScore)
          return {0, id, newRem + topScore}
        end
        
        -- 5) Record delta & touched
        redis.call('INCRBY', deltaKey, topScore)
        redis.call('SADD', KEYS[2], id)
        
        if newRem <= 0 then return {2, id, 0} end
        return {1, id, newRem}
        """;

    private final RedissonClient redisson;
    private final CampaignCacheService cache; // to build the exact ZSET key consistently

    public ServeScriptService(RedissonClient redisson, CampaignCacheService cache) {
        this.redisson = redisson;
        this.cache = cache;
    }

    /**
     * Execute the single-RTT script for given dimensions.
     * @return code: 0=no serve, 1=served, 2=served & exhausted; id: campaignId; newRemainingCents (nullable)
     */
    public ServeResult pickAndSpend(String country, String language, String os, String browser) {
        final String zsetKey = cache.debugFilterKey(country, language, os, browser);
        if (zsetKey == null) return new ServeResult(0, null, null);

        final String rrKey = buildRrKey(country, language, os, browser);

        List<Object> res = eval(zsetKey, TOUCHED_SET, rrKey, BUDGET_PREFIX, DELTA_PREFIX);

        if (res == null || res.isEmpty()) {
            return new ServeResult(0, null, null);
        }

        final int code = toInt(res.get(0));
        final Integer id = (res.size() > 1 ? toIntOrNull(res.get(1)) : null);
        final Long newRem = (res.size() > 2 ? toLongOrNull(res.get(2)) : null);

        return new ServeResult(code, id, newRem);
    }

    private List<Object> eval(String zsetKey, String touchedKey, String rrKey,
                              String budgetPrefix, String deltaPrefix) {
        // IMPORTANT: pass THREE KEYS (zset, touched, rr), then ARGVs
        return redisson.getScript(StringCodec.INSTANCE).eval(
                RScript.Mode.READ_WRITE,
                SERVE_LUA,
                RScript.ReturnType.MULTI,
                Arrays.asList(zsetKey, touchedKey, rrKey),
                budgetPrefix, deltaPrefix
        );
    }

    private static String buildRrKey(String country, String language, String os, String browser) {
        return "campaign:rr:" + part(os) + ":" + part(browser) + ":" + part(language) + ":" + part(country);
    }

    private static String part(String s) {
        return (s == null || s.isBlank()) ? "any" : s;
    }

    private static int toInt(Object o) {
        if (o == null) return 0;
        if (o instanceof Number n) return n.intValue();
        return Integer.parseInt(o.toString());
    }

    private static Integer toIntOrNull(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.intValue();
        String s = o.toString();
        if (s.isEmpty()) return null;
        return Integer.valueOf(s);
    }

    private static Long toLongOrNull(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.longValue();
        String s = o.toString();
        if (s.isEmpty()) return null;
        return Long.valueOf(s);
    }

    /** Result tuple from script: code, id, newRemainingCents */
    public static final class ServeResult {
        public final int code;           // 0=no-serve, 1=served, 2=served-exhausted
        public final Integer campaignId; // nullable when code==0
        public final Long newRemaining;  // may be null when budget missing

        public ServeResult(int code, Integer campaignId, Long newRemaining) {
            this.code = code;
            this.campaignId = campaignId;
            this.newRemaining = newRemaining;
        }

        @Override public String toString() {
            return "ServeResult{code=" + code + ", id=" + campaignId + ", newRem=" + newRemaining + "}";
        }

        @Override public int hashCode() { return Objects.hash(code, campaignId, newRemaining); }
        @Override public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof ServeResult other)) return false;
            return code == other.code &&
                    Objects.equals(campaignId, other.campaignId) &&
                    Objects.equals(newRemaining, other.newRemaining);
        }
    }
}