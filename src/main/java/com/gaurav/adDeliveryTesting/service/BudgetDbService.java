// src/main/java/com/gaurav/adDeliveryTesting/service/BudgetDbService.java
package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.repo.BudgetRepo;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Service
public class BudgetDbService {

    private final BudgetRepo repo;

    public BudgetDbService(BudgetRepo repo) {
        this.repo = repo;
    }

    /** Returns new remaining budget or null if not enough funds. */
    @Transactional // IMPORTANT: write transaction (NOT read-only)
    public BigDecimal trySpendAndGetRemaining(int campaignId, BigDecimal delta) {
        int updated = repo.trySpend(campaignId, delta);
        if (updated == 0) return null;             // insufficient budget or missing campaign
        return repo.getRemaining(campaignId);      // read the fresh remaining inside same TX
    }
}