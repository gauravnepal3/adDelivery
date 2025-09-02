package com.gaurav.adDeliveryTesting.service;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BudgetCounterService {
    @Autowired
    private RedissonClient redisson;

    // KEYS[1] = campaign:budget:{id}
    // KEYS[2] = campaign:delta:{id}
    // KEYS[3] = campaign:touched
    // ARGV[1] = bidCents
    // ARGV[2] = campaignId
    private static final String SPEND_LUA = """
-- KEYS[1] = campaign:budget:{id}
-- KEYS[2] = campaign:delta:{id}
-- KEYS[3] = campaign:touched
-- ARGV[1] = bidCents
-- ARGV[2] = campaignId
local rem = redis.call('HGET', KEYS[1], 'remaining')
if not rem then return {0, nil} end
local remNum = tonumber(rem)
local bid = tonumber(ARGV[1])
if remNum < bid then return {0, rem} end
local newRem = remNum - bid
redis.call('HSET', KEYS[1], 'remaining', tostring(newRem))
redis.call('INCRBY', KEYS[2], bid)
redis.call('SADD', KEYS[3], ARGV[2])
if newRem <= 0 then return {2, '0'} end
return {1, tostring(newRem)}
""";

    public int trySpendCents(int campaignId, long bidCents) {
        String budgetKey = "campaign:budget:" + campaignId;
        String deltaKey  = "campaign:delta:" + campaignId;
        String touchedKey = "campaign:touched";

        var script = redisson.getScript(StringCodec.INSTANCE);

        @SuppressWarnings("unchecked")
        java.util.List<Object> res = (java.util.List<Object>) script.eval(
                RScript.Mode.READ_WRITE,
                SPEND_LUA,
                RScript.ReturnType.MULTI,
                java.util.Arrays.asList(budgetKey, deltaKey, touchedKey),
                String.valueOf(bidCents),
                String.valueOf(campaignId)
        );

        Number code = (Number) res.get(0);
        return code == null ? 0 : code.intValue();
    }
}