package com.gaurav.adDeliveryTesting.service;

import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class BudgetCounterService {
    @Autowired
    private RedissonClient redisson;

    private static final String SPEND_LUA = """
    -- KEYS[1] = campaign:budget:{id}
    -- KEYS[2] = campaign:delta:{id}
    -- ARGV[1] = bidCents
    local rem = redis.call('HGET', KEYS[1], 'remaining')
    if not rem then return {0, nil} end
    local remNum = tonumber(rem)
    local bid = tonumber(ARGV[1])
    if remNum < bid then return {0, rem} end
    local newRem = remNum - bid
    redis.call('HSET', KEYS[1], 'remaining', tostring(newRem))
    redis.call('INCRBY', KEYS[2], bid)
    if newRem <= 0 then return {2, '0'} end
    return {1, tostring(newRem)}
    """;


    public int trySpendCents(int campaignId, long bidCents) {
        String budgetKey = "campaign:budget:" + campaignId;
        String deltaKey  = "campaign:delta:" + campaignId;

        RScript script = redisson.getScript(StringCodec.INSTANCE);

        @SuppressWarnings("unchecked")
        List<Object> res = (List<Object>) script.eval(
                RScript.Mode.READ_WRITE, SPEND_LUA, RScript.ReturnType.MULTI,
                Arrays.asList(budgetKey, deltaKey), String.valueOf(bidCents)
        );

        Number code = (Number) res.get(0);
        return code == null ? 0 : code.intValue();
    }
}