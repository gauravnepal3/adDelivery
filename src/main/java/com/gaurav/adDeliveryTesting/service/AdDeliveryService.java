package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.repo.CampaignFilterRepo;
import com.gaurav.adDeliveryTesting.utils.UserAgentParser;
import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
public class AdDeliveryService implements Serializable {

    @Autowired
    private AdDeliveryRepo repo;

    @Autowired
    private CampaignFilterRepo filterRepo;

    @Autowired
    private UserAgentParser parser;

    @Autowired
    private CampaignCacheService cacheService;

    @Cacheable(value = "campaign", key = "'campaign'")
    public List<Campaign> getCampaign(){
        return repo.findAll();
    }

    @Transactional
    public Optional<Campaign> serveAd(String country, String language, String os, String browser) {
        // 1. Try Redis first
        Integer campaignId = cacheService.getTopCampaignId(country, language, os, browser);

        if (campaignId != null) {
            Optional<Campaign> campaignOpt = repo.findById(campaignId);
            if (campaignOpt.isPresent()) {
                Campaign campaign = campaignOpt.get();

                if (campaign.getRemainingBudget().compareTo(BigDecimal.ZERO) > 0) {
                    campaign.setRemainingBudget(campaign.getRemainingBudget().subtract(campaign.getBiddingRate()));
                    repo.save(campaign);

                    cacheService.updateCampaignBudget(campaign, country, language, os, browser);
                    return Optional.of(campaign);
                }
            }
        }

        // 2. DB Fallback if Redis is empty
        List<CampaignFilters> filters = filterRepo.findAll();

        List<Campaign> matching = filters.stream()
                .filter(f -> (country == null || f.getCountries().contains(country)))
                .filter(f -> (language == null || f.getLanguages().contains(language)))
                .filter(f -> (os == null || f.getOsList().contains(os)))
                .filter(f -> (browser == null || f.getBrowsers().contains(browser)))
                .map(CampaignFilters::getCampaign)
                .filter(c -> c.getRemainingBudget().compareTo(BigDecimal.ZERO) > 0)
                .toList();

        Optional<Campaign> selected = matching.stream()
                .max(Comparator.comparing(Campaign::getBiddingRate));

        selected.ifPresent(c -> {
            // Deduct bid
            c.setRemainingBudget(c.getRemainingBudget().subtract(c.getBiddingRate()));
            repo.save(c);

            // **IMPORTANT: Save this exact combination into Redis for faster next time**
            cacheService.addCampaign(c, country, language, os, browser);

            // (Optional) Warm with all filter combinations too
            for (String ctry : c.getFilters().getCountries()) {
                for (String lang : c.getFilters().getLanguages()) {
                    for (String osItem : c.getFilters().getOsList()) {
                        for (String br : c.getFilters().getBrowsers()) {
                            cacheService.addCampaign(c, ctry, lang, osItem, br);
                        }
                    }
                }
            }
        });

        return selected;
    }
}
