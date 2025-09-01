package com.gaurav.adDeliveryTesting.utils;

import org.springframework.stereotype.Service;

@Service
public class UserAgentParser {

    public String parseOS(String userAgent) {
        userAgent = userAgent.toLowerCase();
        if (userAgent.contains("windows")) return "Windows";
        if (userAgent.contains("macintosh") || userAgent.contains("mac os")) return "Mac";
        if (userAgent.contains("android")) return "Android";
        if (userAgent.contains("iphone") || userAgent.contains("ipad") || userAgent.contains("ios")) return "iOS";
        return "Other";
    }

    public String parseBrowser(String userAgent) {
        userAgent = userAgent.toLowerCase();
        if (userAgent.contains("chrome") && !userAgent.contains("edge")) return "Chrome";
        if (userAgent.contains("firefox")) return "Firefox";
        if (userAgent.contains("safari") && !userAgent.contains("chrome")) return "Safari";
        if (userAgent.contains("edge") || userAgent.contains("edg")) return "Edge";
        return "Other";
    }


    public String parseLanguage(String acceptedLanguage) {
        if (acceptedLanguage.contains("en-US")) return "en-US";
        return "Other";
    }


}
