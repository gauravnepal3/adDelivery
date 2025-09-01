package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
public class CampaignCacheService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private String getKey(String country, String language, String os, String browser) {
        return String.format("campaign:filters:%s:%s:%s:%s",
                os != null ? os : "any",
                browser != null ? browser : "any",
                language != null ? language : "any",
                country != null ? country : "any");
    }

    public void addCampaign(Campaign campaign, String country, String language, String os, String browser) {
        String key = getKey(country, language, os, browser);
        redisTemplate.opsForZSet().add(key, campaign.getCampaignId(), campaign.getBiddingRate().doubleValue());
    }

    public Integer getTopCampaignId(String country, String language, String os, String browser) {
        String key = getKey(country, language, os, browser);
        Set<Object> result = redisTemplate.opsForZSet().reverseRange(key, 0, 0);
        if (result == null || result.isEmpty()) return null;
        return (Integer) result.iterator().next();
    }

    public void updateCampaignBudget(Campaign campaign, String country, String language, String os, String browser) {
        addCampaign(campaign, country, language, os, browser);
    }
}