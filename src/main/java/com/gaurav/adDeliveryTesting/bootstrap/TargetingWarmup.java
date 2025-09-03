// src/main/java/com/gaurav/adDeliveryTesting/bootstrap/TargetingWarmup.java
package com.gaurav.adDeliveryTesting.bootstrap;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(value = "adserve.warmup.enabled", havingValue = "true", matchIfMissing = false)
public class TargetingWarmup {
    // leave empty or keep a tiny manual method if you want a button later
}