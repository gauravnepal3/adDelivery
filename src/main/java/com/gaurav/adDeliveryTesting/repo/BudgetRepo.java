// src/main/java/.../repo/BudgetRepo.java
package com.gaurav.adDeliveryTesting.repo;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;

public interface BudgetRepo extends Repository<com.gaurav.adDeliveryTesting.model.Campaign, Integer> {

    @Modifying
    @Query(value = """
        UPDATE campaign
        SET remaining_budget = remaining_budget - :bid
        WHERE campaign_id = :id
          AND remaining_budget >= :bid
        RETURNING remaining_budget
        """, nativeQuery = true)
    BigDecimal trySpend(
            @Param("id") int id,
            @Param("bid") BigDecimal bid);
}