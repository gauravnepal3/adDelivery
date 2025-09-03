// src/main/java/com/gaurav/adDeliveryTesting/repo/BudgetRepo.java
package com.gaurav.adDeliveryTesting.repo;

import com.gaurav.adDeliveryTesting.model.Campaign;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;

public interface BudgetRepo extends org.springframework.data.repository.Repository<com.gaurav.adDeliveryTesting.model.Campaign, Integer> {

    @org.springframework.data.jpa.repository.Modifying(clearAutomatically = true, flushAutomatically = true)
    @org.springframework.data.jpa.repository.Query(value = """
        UPDATE campaign
        SET remaining_budget = remaining_budget - :delta
        WHERE campaign_id = :id
          AND remaining_budget >= :delta
        """, nativeQuery = true)
    int trySpend(@org.springframework.data.repository.query.Param("id") int id,
                 @org.springframework.data.repository.query.Param("delta") java.math.BigDecimal delta);

    @org.springframework.data.jpa.repository.Query("select c.remainingBudget from Campaign c where c.campaignId = :id")
    java.math.BigDecimal getRemaining(@org.springframework.data.repository.query.Param("id") int id);
}