package com.gaurav.adDeliveryTesting.controller;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.responseDto.CampaignResponseDto;
import com.gaurav.adDeliveryTesting.service.AdDeliveryService;
import com.gaurav.adDeliveryTesting.utils.UserAgentParser;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.coyote.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1")
public class AdDeliveryController {

    @Autowired
    private AdDeliveryService service;

    @Autowired
    private UserAgentParser parser;

    @GetMapping("/campaigns")
    public ResponseEntity<List<Campaign>> getCampaign(){
        return new ResponseEntity<>(service.getCampaign(), HttpStatus.OK);
    }

    @GetMapping("/serve")
    public ResponseEntity<?> serveAd(HttpServletRequest request) {
        String country  = trimToNull(request.getHeader("X-Country"));
        String language = trimToNull(parser.parseLanguage(request.getHeader("Accept-Language")));
        String ua       = request.getHeader("User-Agent");
        String os       = trimToNull(parser.parseOS(ua));
        String browser  = trimToNull(parser.parseBrowser(ua));

        log.warn("Country:"+country+" Language:"+language+" OS:"+os+" Browser:"+browser);
        return service.serveAd(country, language, os, browser)
                .<ResponseEntity<?>>map(c -> ResponseEntity.ok(Map.of(
                        "campaignId", c.getCampaignId(),
                        "deliveryLink", c.getDeliveryLink(),
                        "biddingRate", c.getBiddingRate(),
                        "remainingBudget", c.getRemainingBudget()
                )))
                .orElseGet(() -> ResponseEntity.noContent().build());
    }


    @GetMapping("/serveByParams")
    public ResponseEntity<?> serveByParams(@RequestParam("country") String countryDate, @RequestParam("language") String languageDate,
                                           @RequestParam("os") String osDate,@RequestParam("browser") String browserDate
                                           ) {
        String country  = trimToNull(countryDate);
        String language = trimToNull(languageDate);
        String os       = trimToNull(osDate);
        String browser  = trimToNull(browserDate);

        log.warn("Country:"+country+" Language:"+language+" OS:"+os+" Browser:"+browser);
        return service.serveAd(country, language, os, browser)
                .<ResponseEntity<?>>map(c -> ResponseEntity.ok(Map.of(
                        "campaignId", c.getCampaignId(),
                        "deliveryLink", c.getDeliveryLink(),
                        "biddingRate", c.getBiddingRate(),
                        "remainingBudget", c.getRemainingBudget()
                )))
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
