package com.gaurav.adDeliveryTesting.repo;

import com.gaurav.adDeliveryTesting.model.Campaign;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AdDeliveryRepo extends JpaRepository<Campaign,Integer> {

}
