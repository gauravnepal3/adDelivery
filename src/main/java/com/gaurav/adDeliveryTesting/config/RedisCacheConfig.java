package com.gaurav.adDeliveryTesting.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.redisson.config.Config;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@Configuration
public class RedisCacheConfig {

    @Bean
    public CacheManager cacheManager(RedisConnectionFactory cf) {
        RedisCacheConfiguration base = RedisCacheConfiguration.defaultCacheConfig()
                .disableCachingNullValues()
                .entryTtl(Duration.ofMinutes(5));

        return RedisCacheManager.builder(cf)
                .cacheDefaults(base)
                .withInitialCacheConfigurations(Map.of(
                        "campaign", base  // name MUST match @Cacheable value
                ))
                .build();
    }

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(RedisProperties props) {
        String address = "redis://" + props.getHost() + ":" + props.getPort();
        Config cfg = new Config();

        // --- Tune thread pools ---
        cfg.setThreads(32);        // Redisson worker threads (internal tasks, callbacks, pub/sub handling)
        cfg.setNettyThreads(64);   // Netty event loop threads for networking

        var single = cfg.useSingleServer()
                .setAddress(address)
                .setConnectionPoolSize(2048)             // connections per client
                .setConnectionMinimumIdleSize(64)
                .setSubscriptionConnectionPoolSize(256) // for pub/sub if used
                .setIdleConnectionTimeout(10000)
                .setConnectTimeout(1000)
                .setTimeout(1000)
                .setRetryAttempts(1)
                .setRetryInterval(100);

        if (props.getPassword() != null && !props.getPassword().isBlank()) {
            single.setPassword(props.getPassword());
        }

        return Redisson.create(cfg);
    }

}