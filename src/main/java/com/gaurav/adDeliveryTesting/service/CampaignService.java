package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CampaignService {
    @Autowired
    private AdDeliveryRepo campaignRepository;

    @Autowired
    private CampaignCacheService cacheService;

    @Transactional
    public Campaign saveCampaign(Campaign campaign) {
        Campaign saved = campaignRepository.save(campaign);

        for (String country : saved.getFilters().getCountries()) {
            for (String language : saved.getFilters().getLanguages()) {
                for (String os : saved.getFilters().getOsList()) {
                    for (String browser : saved.getFilters().getBrowsers()) {
                        cacheService.addCampaign(saved, country, language, os, browser);
                    }
                }
            }
        }
        return saved;
    }
}
