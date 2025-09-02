package com.gaurav.adDeliveryTesting.controller;

import com.gaurav.adDeliveryTesting.bootstrap.BudgetWarmup;
import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.responseDto.ServeResponseDTO;
import com.gaurav.adDeliveryTesting.service.AdDeliveryService;
import com.gaurav.adDeliveryTesting.utils.UserAgentParser;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
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

    @Autowired private  BudgetWarmup warmup;

    @GetMapping("/campaigns")
    public ResponseEntity<List<Campaign>> getCampaign(){
        return new ResponseEntity<>(service.getCampaign(), HttpStatus.OK);
    }

    @GetMapping("/serve")
    public ResponseEntity<?> serveAd(HttpServletRequest request) {
        final String country  = request.getHeader("X-Country");
        final String language = parser.parseLanguage(request.getHeader("Accept-Language"));
        final String ua       = request.getHeader("User-Agent");
        final String os       = parser.parseOS(ua);
        final String browser  = parser.parseBrowser(ua);

        // Remove this:
        // log.warn("Country:"+country+" Language:"+language+" OS:"+os+" Browser:"+browser);

        return service.serveAd(country, language, os, browser)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    @GetMapping("/test")
    public ResponseEntity<String> tester() {
        return new ResponseEntity<>("Hello World",HttpStatus.OK);
    }

    private static String trimToNull(String s) {
        if (s == null) return null;
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }

    @GetMapping("/serveByParams")
    public ResponseEntity<?> serveByParams(@RequestParam String country,
                                           @RequestParam String language,
                                           @RequestParam String os,
                                           @RequestParam String browser) {
        return service.serveAd(country, language, os, browser)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }



        @PostMapping("/warm") public String warm() { warmup.warm(); return "ok"; }


}