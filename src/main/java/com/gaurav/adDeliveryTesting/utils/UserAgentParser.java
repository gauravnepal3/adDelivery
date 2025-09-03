package com.gaurav.adDeliveryTesting.utils;

import org.springframework.stereotype.Service;

@Service
public class UserAgentParser {

    public String parseOS(String userAgent) {
        if (userAgent == null) return "Others";
        String ua = userAgent.toLowerCase();
        if (ua.contains("windows")) return "Windows";
        if (ua.contains("macintosh") || ua.contains("mac os")) return "Mac";
        if (ua.contains("android")) return "Android";
        if (ua.contains("iphone") || ua.contains("ipad") || ua.contains("ios")) return "iOS";
        if (ua.contains("smarttv") || ua.contains("hbbtv") || ua.contains("tizen") || ua.contains("webos")) return "TV";
        return "Others";
    }

    public String parseBrowser(String userAgent) {
        if (userAgent == null) return "Others";
        String ua = userAgent.toLowerCase();
        if (ua.contains("edg") || ua.contains("edge")) return "Edge";
        if (ua.contains("chrome") && !ua.contains("edge") && !ua.contains("edg")) return "Chrome";
        if (ua.contains("firefox")) return "Firefox";
        if (ua.contains("safari") && !ua.contains("chrome")) return "Safari";
        return "Others";
    }

    /** Prefer explicit X-Device header; fallback to UA heuristic. */
    public String parseDevice(String userAgent, String override) {
        if (override != null && !override.isBlank()) return override.trim();
        if (userAgent == null) return "Desktop";
        String ua = userAgent.toLowerCase();
        if (ua.contains("iphone") || ua.contains("android") && ua.contains("mobile")) return "Mobile";
        if (ua.contains("ipad") || (ua.contains("android") && !ua.contains("mobile"))) return "Mobile"; // treat tablets as Mobile unless you store "Tablet"
        if (ua.contains("smarttv") || ua.contains("hbbtv") || ua.contains("tizen") || ua.contains("webos")) return "TV";
        // Default desktop class; if you store "Laptop" rather than "Desktop", map here:
        if (ua.contains("macintosh") || ua.contains("windows") || ua.contains("linux")) return "Desktop";
        return "Desktop";
    }

    public String parseLanguage(String acceptedLanguage) {
        if (acceptedLanguage == null || acceptedLanguage.isBlank()) return "Others";
        // Simple: take first token
        String first = acceptedLanguage.split(",")[0].trim();
        return first.isEmpty() ? "Others" : first;
    }
}