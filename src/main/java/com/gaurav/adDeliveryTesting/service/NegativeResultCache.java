package com.gaurav.adDeliveryTesting.service;


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
class NegativeResultCache {
    private final com.github.benmanes.caffeine.cache.Cache<String, Boolean> cache =
            com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                    .maximumSize(500_000)
                    .expireAfterWrite(java.time.Duration.ofSeconds(3))
                    .build();

    boolean recentlyMissed(String key) { return cache.getIfPresent(key) != null; }
    void markMiss(String key) { cache.put(key, Boolean.TRUE); }
    void markMiss(String key, long ttlMs) {
        // tiny ad-hoc TTL: store a sub-key with millis if you want,
        // or keep single cache with 3s and rely on normal expiry.
        cache.put(key, Boolean.TRUE);
    }
}
