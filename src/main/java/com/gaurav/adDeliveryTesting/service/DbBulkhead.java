package com.gaurav.adDeliveryTesting.service;
import org.springframework.stereotype.Component;

@Component
class DbBulkhead {
    // keep strictly below Hikari maximumPoolSize to preserve some headroom
    private final java.util.concurrent.Semaphore sem =
            new java.util.concurrent.Semaphore(32, true);

    boolean enter() { return sem.tryAcquire(); }
    void leave() { sem.release(); }
}