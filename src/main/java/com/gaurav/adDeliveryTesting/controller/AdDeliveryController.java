package com.gaurav.adDeliveryTesting.controller;

import com.gaurav.adDeliveryTesting.bootstrap.TargetingWarmup;
import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.repo.AdDeliveryRepo;
import com.gaurav.adDeliveryTesting.responseDto.ServeResponseDTO;
import com.gaurav.adDeliveryTesting.service.AdDeliveryService;
import com.gaurav.adDeliveryTesting.service.CampaignCacheService;
import com.gaurav.adDeliveryTesting.utils.DomainUtils;
import com.gaurav.adDeliveryTesting.utils.UserAgentParser;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1")
public class AdDeliveryController {

    private final AdDeliveryService service;
    private final UserAgentParser parser;
    @Autowired
    private  CampaignCacheService cache;
    @Autowired
    private  AdDeliveryRepo repo;


    public AdDeliveryController(AdDeliveryService service, UserAgentParser parser) {
        this.service = service;
        this.parser = parser;
    }

    @GetMapping("/campaigns")
    public ResponseEntity<List<Campaign>> getCampaign(){
        return new ResponseEntity<>(service.getCampaign(), HttpStatus.OK);
    }

    @GetMapping("/serve")
    public ResponseEntity<?> serveAd(HttpServletRequest req) {
        final String country  = header(req, "X-Country");
        final String language = parser.parseLanguage(req.getHeader("Accept-Language"));
        final String ua       = req.getHeader("User-Agent");
        final String os       = parser.parseOS(ua);
        final String device   = parser.parseDevice(ua, header(req, "X-Device"));
        final String browser  = parser.parseBrowser(ua);

        final String ip       = clientIp(req);
        final String domain   = DomainUtils.extractHost(header(req, "X-Domain"), req.getHeader("Origin"), req.getHeader("Referer"));
        final String iab      = header(req, "X-IAB"); // optional

        return service.serve(country, language, device, os, ip, domain, browser, iab)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    @GetMapping("/test")
    public ResponseEntity<String> tester(){
        return new ResponseEntity<>("Hello World",HttpStatus.OK);
    }

    @GetMapping("/serveByParams")
    public ResponseEntity<?> serveByParams(@RequestParam String country,
                                           @RequestParam String language,
                                           @RequestParam String device,
                                           @RequestParam String os,
                                           @RequestParam(required = false) String browser,
                                           @RequestParam(required = false) String domain,
                                           @RequestParam(required = false) String ip,
                                           @RequestParam(required = false, name = "iab") String iab) {

        // normalize
        String d = DomainUtils.extractHost(domain, null, null);
        return service.serve(country, language, device, os,
                        nullToEmpty(ip), d, nullToEmpty(browser), nullToEmpty(iab))
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    private static String header(HttpServletRequest req, String name) {
        String v = req.getHeader(name);
        return (v == null || v.isBlank()) ? null : v.trim();
    }
    private static String nullToEmpty(String s) { return s == null ? "" : s; }

    private static String clientIp(HttpServletRequest req) {
        String xff = req.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            int comma = xff.indexOf(',');
            return (comma >= 0) ? xff.substring(0, comma).trim() : xff.trim();
        }
        return req.getRemoteAddr();
    }
}