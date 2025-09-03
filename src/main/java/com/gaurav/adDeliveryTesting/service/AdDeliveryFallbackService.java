package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.model.CampaignFilters;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.responseDto.ServeResponseDTO;
import com.gaurav.adDeliveryTesting.utils.MoneyUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class AdDeliveryFallbackService {

    private final AdDeliveryRepo repo;
    private final CampaignCacheService cache;
    private final BudgetCounterService budget;

    public AdDeliveryFallbackService(AdDeliveryRepo repo,
                                     CampaignCacheService cache,
                                     BudgetCounterService budget) {
        this.repo = repo;
        this.cache = cache;
        this.budget = budget;
    }

    @Transactional(readOnly = true)
    public Optional<ServeResponseDTO> serveAdFallback(String country,
                                                      String language,
                                                      String device,
                                                      String os,
                                                      String ip,
                                                      String domain,
                                                      String browser,
                                                      String iab) {
        final String dom = domain == null ? "" : domain.trim().toLowerCase();
        final String br  = browser == null ? "" : browser.trim();
        final String iabCode = iab == null ? "" : iab.trim();
        final String ipAddr  = ip == null ? "" : ip.trim();

        List<Campaign> campaigns = repo.findAllWithFilters();

        var matching = campaigns.stream()
                .filter(c -> {
                    CampaignFilters f = c.getFilters();
                    if (f == null) return false;

                    if (country  != null && !f.getCountries().contains(country)) return false;
                    if (language != null && !f.getLanguages().contains(language)) return false;
                    if (device   != null && !f.getDevices().contains(device))     return false;
                    if (os       != null && !f.getOsList().contains(os))          return false;

                    if (!f.getBrowsers().isEmpty() && !br.isEmpty() && !f.getBrowsers().contains(br)) return false;
                    if (!iabCode.isEmpty() && !f.getIabCategory().isEmpty() && !f.getIabCategory().contains(iabCode)) return false;

                    if (!f.getAllowedIP().isEmpty() && !ipAddr.isEmpty() && !f.getAllowedIP().contains(ipAddr)) return false;
                    if (!f.getAllowedDomain().isEmpty() && !dom.isEmpty() &&
                            !f.getAllowedDomain().stream().anyMatch(d -> d != null && d.equalsIgnoreCase(dom))) return false;

                    if (!f.getExcludedIP().isEmpty() && !ipAddr.isEmpty() && f.getExcludedIP().contains(ipAddr)) return false;
                    if (!f.getExcludedDomain().isEmpty() && !dom.isEmpty() &&
                            f.getExcludedDomain().stream().anyMatch(d -> d != null && d.equalsIgnoreCase(dom))) return false;

                    return true;
                })
                .filter(c -> c.getRemainingBudget().compareTo(BigDecimal.ZERO) > 0)
                .toList();

        if (matching.isEmpty()) return Optional.empty();

        BigDecimal topBid = matching.stream().map(Campaign::getBiddingRate).max(BigDecimal::compareTo).orElse(BigDecimal.ZERO);
        var topCampaigns = matching.stream().filter(c -> c.getBiddingRate().compareTo(topBid) == 0).toList();
        if (topCampaigns.isEmpty()) return Optional.empty();

        Campaign chosen = topCampaigns.get(ThreadLocalRandom.current().nextInt(topCampaigns.size()));

        CampaignFilters cf = chosen.getFilters();
        if (cf != null) {
            for (String ctry : cf.getCountries())
                for (String lang : cf.getLanguages())
                    for (String dev  : cf.getDevices())
                        for (String osItem : cf.getOsList())
                            cache.addCampaignToZset(chosen, ctry, lang, dev, osItem);
        }

        long bidCents = MoneyUtils.toCents(chosen.getBiddingRate());
        int rc = budget.trySpendCents(chosen.getCampaignId(), bidCents);
        if (rc == 0) return Optional.empty();
        if (rc == 2) cache.removeCampaignEverywhere(chosen.getCampaignId());

        return Optional.of(new ServeResponseDTO(
                chosen.getCampaignId(),
                chosen.getDeliveryLink(),
                chosen.getBiddingRate(),
                chosen.getRemainingBudget().subtract(chosen.getBiddingRate())
        ));
    }
}