package com.gaurav.adDeliveryTesting.service;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.utils.UserAgentParser;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AdDeliveryService {

    private final AdDeliveryRepo repo;
    private final CampaignCacheService cache;

    // local hot cache for id -> deliveryLink (warmed at startup)
    private final Map<Integer, String> deliveryLinks = new ConcurrentHashMap<>();

    public AdDeliveryService(AdDeliveryRepo repo, UserAgentParser parser, CampaignCacheService cache) {
        this.repo = repo;
        this.cache = cache;
    }

    /** Called by BudgetWarmup to fill in-memory delivery link cache. */
    public void putDeliveryLink(int id, String link) {
        deliveryLinks.put(id, link);
    }



    public Optional<Campaign> serveAd(String country, String language, String os, String browser) {
        // normalize "others" -> null => wildcard
        if ("others".equalsIgnoreCase(os)) os = null;
        if ("others".equalsIgnoreCase(browser)) browser = null;
        if ("others".equalsIgnoreCase(language)) language = null;
        if ("others".equalsIgnoreCase(country)) country = null;

        // Try a short fallback chain of keys (specific → broader)
        List<String[]> keys = List.of(
                new String[]{os, browser, language, country},
                new String[]{os, null,    language, country},
                new String[]{null, browser, language, country},
                new String[]{null, null,    language, country},
                new String[]{null, null,    null,     country},
                new String[]{null, null,    null,     null}
        );

        CampaignCacheService.SelectionResult sel = null;
        for (String[] k : keys) {
            sel = cache.pickAndSpendOne(k[3], k[2], k[0], k[1]); // country, language, os, browser
            if (sel.code == 1 || sel.code == 2) break;           // served
            // code 0 = no candidate; 3 = insufficient (try broader)
        }

        if (sel == null || !(sel.code == 1 || sel.code == 2) || sel.campaignId == null) {
            return Optional.empty();
        }

        // Hydrate response WITHOUT hitting DB (just provide deliveryLink for controller)
        Integer cid = sel.campaignId;
        String link = deliveryLinks.get(cid);

        if (link == null) {
            // Fallback (rare): read once and cache it
            return repo.findById(cid).map(c -> {
                deliveryLinks.put(cid, c.getDeliveryLink());
                Campaign out = new Campaign();
                out.setCampaignId(c.getCampaignId());
                out.setDeliveryLink(c.getDeliveryLink());
                return out;
            });
        } else {
            Campaign out = new Campaign();
            out.setCampaignId(cid);
            out.setDeliveryLink(link);
            return Optional.of(out);
        }
    }
}