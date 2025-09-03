package com.gaurav.adDeliveryTesting.service;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Service
public class ServeScriptService {

    private static final String TOUCHED_SET  = "campaign:touched";
    private static final String BUDGET_PREF  = "campaign:budget:";
    private static final String DELTA_PREF   = "campaign:delta:";

    // KEYS:
    //   KEYS[1] = ZSET key (os/device/lang/country)
    //   KEYS[2] = touched set
    //   KEYS[3] = rr key
    //
    // ARGV:
    //   1 = budget prefix
    //   2 = delta  prefix
    //   3 = ip
    //   4 = domain (lowercased)
    //   5 = browser
    //   6 = iabCode  (e.g. "IAB1-1"; pass empty for "no iab constraint")
    //
    // We'll derive allow/block set keys inside Lua to avoid sending many KEYS.
    private static final String LUA = """
  local zsetKey     = KEYS[1]
  local touchedKey  = KEYS[2]
  local rrKey       = KEYS[3]

  local budgetPref  = ARGV[1]
  local deltaPref   = ARGV[2]
  local ip          = ARGV[3]
  local domain      = ARGV[4]
  local browser     = ARGV[5]
  local iab         = ARGV[6]

  -- find top score
  local top = redis.call('ZREVRANGE', zsetKey, 0, 0, 'WITHSCORES')
  if (not top) or (#top == 0) then return {0} end
  local topScore = tonumber(top[2])
  if (not topScore) then return {0} end

  local tieCount = redis.call('ZCOUNT', zsetKey, topScore, topScore)
  if (not tieCount) or (tieCount == 0) then return {0} end

  local rr = redis.call('INCR', rrKey)
  local start = (rr - 1) % tieCount

  local function isAllowed(allowKey, value)
    -- '*' => allow all
    local hasStar = redis.call('SISMEMBER', allowKey, '*')
    if hasStar == 1 then return true end
    -- empty set => no constraint => allow
    local sz = redis.call('SCARD', allowKey)
    if (not sz) or (sz == 0) then return true end
    if (value == nil) or (value == '') then return false end
    local hit = redis.call('SISMEMBER', allowKey, value)
    return hit == 1
  end

  local function isBlocked(blockKey, value)
    local sz = redis.call('SCARD', blockKey)
    if (not sz) or (sz == 0) then return false end
    if (value == nil) or (value == '') then return false end
    local hit = redis.call('SISMEMBER', blockKey, value)
    return hit == 1
  end

  for i = 0, tieCount - 1 do
    local offset = (start + i) % tieCount
    local ids = redis.call('ZREVRANGEBYSCORE', zsetKey, topScore, topScore, 'LIMIT', offset, 1)
    if ids and (#ids > 0) then
      local id = ids[1]

      local allowBrowserKey = 'campaign:allow:browser:' .. id
      local allowIabKey     = 'campaign:allow:iab:' .. id
      local allowIpKey      = 'campaign:allow:ip:' .. id
      local allowDomKey     = 'campaign:allow:domain:' .. id

      local blockIpKey      = 'campaign:block:ip:' .. id
      local blockDomKey     = 'campaign:block:domain:' .. id

      local pass = true
      if pass and (not isAllowed(allowBrowserKey, browser)) then pass = false end
      if pass and (iab ~= nil and iab ~= '') and (not isAllowed(allowIabKey, iab)) then pass = false end
      if pass and (not isAllowed(allowIpKey, ip)) then pass = false end
      if pass and (not isAllowed(allowDomKey, domain)) then pass = false end
      if pass and isBlocked(blockIpKey, ip) then pass = false end
      if pass and isBlocked(blockDomKey, domain) then pass = false end

      if pass then
        local budgetKey = budgetPref .. id
        local deltaKey  = deltaPref  .. id

        local newRem = redis.call('HINCRBY', budgetKey, 'remaining', -topScore)
        if (not newRem) then
          -- missing field/key => treat as no-serve
        else
          if newRem < 0 then
            redis.call('HINCRBY', budgetKey, 'remaining', topScore)
          else
            redis.call('INCRBY', deltaKey, topScore)
            redis.call('SADD', touchedKey, id)
            if newRem <= 0 then return {2, id, 0} end
            return {1, id, newRem}
          end
        end
      end
    end
  end

  return {0}
""";

    private final RedissonClient redisson;

    public ServeScriptService(RedissonClient redisson) {
        this.redisson = redisson;
    }

    public ServeResult pickAndSpend(String country, String language, String device, String os,
                                    String ip, String domain, String browser, String iab) {
        final String zsetKey = CampaignCacheService.zsetKey(country, language, device, os);
        final String rrKey   = CampaignCacheService.rrKey(country, language, device, os);

        List<Object> res = redisson.getScript(StringCodec.INSTANCE).eval(
                RScript.Mode.READ_WRITE,
                LUA,
                RScript.ReturnType.MULTI,
                Arrays.asList(zsetKey, TOUCHED_SET, rrKey),
                BUDGET_PREF, DELTA_PREF,
                nvl(ip), nvl(lower(domain)), nvl(browser), nvl(iab)
        );

        if (res == null || res.isEmpty()) return new ServeResult(0, null, null);

        int code = toInt(res.get(0));
        Integer id = (res.size() > 1 ? toIntOrNull(res.get(1)) : null);
        Long newRem = (res.size() > 2 ? toLongOrNull(res.get(2)) : null);
        return new ServeResult(code, id, newRem);
    }

    private static String nvl(String s) { return (s == null ? "" : s); }
    private static String lower(String s) { return (s == null ? null : s.toLowerCase()); }

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

    public static final class ServeResult {
        public final int code;
        public final Integer campaignId;
        public final Long newRemaining;
        public ServeResult(int code, Integer campaignId, Long newRemaining) {
            this.code = code; this.campaignId = campaignId; this.newRemaining = newRemaining;
        }
        @Override public String toString() { return "ServeResult{code=" + code + ", id=" + campaignId + ", newRem=" + newRemaining + "}"; }
        @Override public int hashCode() { return Objects.hash(code, campaignId, newRemaining); }
        @Override public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof ServeResult other)) return false;
            return code == other.code &&
                    java.util.Objects.equals(campaignId, other.campaignId) &&
                    java.util.Objects.equals(newRemaining, other.newRemaining);
        }
    }
}