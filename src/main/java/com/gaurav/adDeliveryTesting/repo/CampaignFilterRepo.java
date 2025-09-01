package com.gaurav.adDeliveryTesting.repo;

import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CampaignFilterRepo extends JpaRepository<CampaignFilters,Integer> {

}
