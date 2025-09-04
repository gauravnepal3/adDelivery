// src/main/java/com/gaurav/warmAdDeliver/service/PositivePickCache.java
package com.gaurav.adDeliveryTesting.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.stereotype.Component;

import java.time.Duration;

// PositivePickCache.java
@Component
class PositivePickCache {
    private final com.github.benmanes.caffeine.cache.Cache<String, Integer> cache =
            com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
                    .maximumSize(500_000)
                    .expireAfterWrite(java.time.Duration.ofMillis(1000))
                    .build();

    Integer get(String key){ return cache.getIfPresent(key); }
    void put(String key, Integer id){ cache.put(key, id); }
    void invalidate(String key){ cache.invalidate(key); }
}