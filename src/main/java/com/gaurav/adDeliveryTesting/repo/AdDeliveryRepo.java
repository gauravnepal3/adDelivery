package com.gaurav.adDeliveryTesting.repo;

import com.gaurav.adDeliveryTesting.model.Campaign;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface AdDeliveryRepo extends JpaRepository<Campaign,Integer> {
    @Lock(LockModeType.PESSIMISTIC_WRITE) // keep if you still use the old path elsewhere
    @Query("select c from Campaign c where c.campaignId = :id")
    Optional<Campaign> findByIdForUpdate(@Param("id") Integer id);

    @Query("select c.campaignId from Campaign c")
    List<Integer> findAllIds();
}
