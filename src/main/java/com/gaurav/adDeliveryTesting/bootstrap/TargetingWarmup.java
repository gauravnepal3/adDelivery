package com.gaurav.adDeliveryTesting.bootstrap;

import com.gaurav.adDeliveryTesting.service.WarmService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TargetingWarmup {

    private final WarmService warmService;

    /** Auto-warm on startup in pages & pipelined batches (fast + low memory). */
    @EventListener(ApplicationReadyEvent.class)
    public void warmOnStartup() {
        try {
            int pageSize = 5000;      // how many IDs per page from DB
            int batchLoadSize = 1000; // how many full entities to load & pipeline per chunk
            int count = warmService.warmAllPaged(pageSize, batchLoadSize);
            log.info("Warmup finished. Total campaigns indexed: {}", count);
        } catch (Exception e) {
            log.error("Warmup on startup failed", e);
        }
    }
}