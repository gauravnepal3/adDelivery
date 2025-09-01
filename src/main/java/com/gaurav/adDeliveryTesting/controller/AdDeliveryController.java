package com.gaurav.adDeliveryTesting.controller;

import com.gaurav.adDeliveryTesting.model.Campaign;
import com.gaurav.adDeliveryTesting.responseDto.CampaignResponseDto;
import com.gaurav.adDeliveryTesting.service.AdDeliveryService;
import com.gaurav.adDeliveryTesting.utils.UserAgentParser;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.coyote.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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
    public ResponseEntity<String> serveAd(
            HttpServletRequest request
    ){
        String country = request.getHeader("X-Country");       // optional
        String language =parser.parseLanguage(request.getHeader("Accept-Language"));     // optional
        String os =parser.parseOS(request.getHeader("User-Agent"));           // parse OS from User-Agent
        String browser =parser.parseBrowser(request.getHeader("User-Agent"));      // parse browser from User-Agent
        // Call service
        return service.serveAd(country, language, os, browser)
                .map(c -> ResponseEntity.ok("Ad served: Campaign " + c.getDeliveryLink()))
                .orElseGet(() -> ResponseEntity.status(404).body("No eligible campaign found."));
    }
}
