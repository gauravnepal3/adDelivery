package com.gaurav.adDeliveryTesting.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;

import org.redisson.config.Config;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.time.Duration;
import java.util.Map;

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
        Config cfg = new Config();
        cfg.setThreads(54);        // adjust to ~1–2x cores
        cfg.setNettyThreads(96);

        var single = cfg.useSingleServer()
                .setAddress((props.getSsl().isEnabled() ? "rediss://" : "redis://") + props.getHost() + ":" + props.getPort())
                .setPassword((props.getPassword() != null && !props.getPassword().isBlank()) ? props.getPassword() : null)
                .setConnectionPoolSize(3000)            // bigger pool to avoid "Unable to acquire connection"
                .setConnectionMinimumIdleSize(256)
                .setSubscriptionConnectionPoolSize(128)
                .setIdleConnectionTimeout(8000)
                .setConnectTimeout(600)
                .setTimeout(700)                        // hard cap latency
                .setRetryAttempts(1)
                .setRetryInterval(100)
                .setPingConnectionInterval(0);          // reduce chatter

        return Redisson.create(cfg);
    }

}