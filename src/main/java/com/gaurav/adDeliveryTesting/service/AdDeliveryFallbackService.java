package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.responseDto.ServeResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class AdDeliveryFallbackService {

    @Autowired
    private  AdDeliveryRepo repo;
    @Autowired
    private  CampaignCacheService cacheService;
    @Autowired
    private  BudgetCounterService budgetCounterService;



    @Transactional(readOnly = true)
    public Optional<ServeResponseDTO> serveAdFallback(String country, String language, String os, String browser) {
        List<Campaign> campaigns = repo.findAllWithFilters();  // eager: filters + element collections

        var matching = campaigns.stream()
                .filter(c -> {
                    CampaignFilters f = c.getFilters();
                    if (f == null) return false;
                    return (country  == null || f.getCountries().contains(country)) &&
                            (language == null || f.getLanguages().contains(language)) &&
                            (os       == null || f.getOsList().contains(os)) &&
                            (browser  == null || f.getBrowsers().contains(browser));
                })
                .filter(c -> c.getRemainingBudget().compareTo(BigDecimal.ZERO) > 0)
                .toList();

        if (matching.isEmpty()) return Optional.empty();

        BigDecimal topBid = matching.stream()
                .map(Campaign::getBiddingRate)
                .max(BigDecimal::compareTo).orElse(BigDecimal.ZERO);

        var topCampaigns = matching.stream()
                .filter(c -> c.getBiddingRate().compareTo(topBid) == 0)
                .toList();
        if (topCampaigns.isEmpty()) return Optional.empty();

        Campaign chosen = topCampaigns.get(ThreadLocalRandom.current().nextInt(topCampaigns.size()));

        // Warm ZSETs once
        CampaignFilters cf = chosen.getFilters();
        if (cf != null) {
            for (String ctry : cf.getCountries())
                for (String lang : cf.getLanguages())
                    for (String osItem : cf.getOsList())
                        for (String br : cf.getBrowsers()) {
                            cacheService.addCampaign(chosen, ctry, lang, osItem, br);
                        }
        }

        long bidCents = com.gaurav.adDeliveryTesting.utils.MoneyUtils.toCents(chosen.getBiddingRate());
        int rc = budgetCounterService.trySpendCents(chosen.getCampaignId(), bidCents);
        if (rc == 0) return Optional.empty();
        if (rc == 2) cacheService.removeCampaignEverywhere(chosen.getCampaignId());

        return Optional.of(new ServeResponseDTO(
                chosen.getCampaignId(),
                chosen.getDeliveryLink(),
                chosen.getBiddingRate(),
                chosen.getRemainingBudget().subtract(chosen.getBiddingRate())
        ));
    }
}
