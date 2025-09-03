// src/main/java/com/gaurav/adDeliveryTesting/repo/BudgetRepo.java
package com.gaurav.adDeliveryTesting.repo;

import com.gaurav.adDeliveryTesting.model.Campaign;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;

public interface BudgetRepo extends Repository<Campaign, Integer> {

    // Atomic spend: subtract if enough remaining. Returns 1 if updated, 0 if not.
    @Modifying
    @Query(value = """
        UPDATE campaign
        SET remaining_budget = remaining_budget - :delta
        WHERE campaign_id = :id
          AND remaining_budget >= :delta
        """, nativeQuery = true)
    int trySpend(@Param("id") int id, @Param("delta") BigDecimal delta);

    // Read the (new) remaining balance when needed
    @Query("select c.remainingBudget from Campaign c where c.campaignId = :id")
    BigDecimal getRemaining(@Param("id") int id);
}