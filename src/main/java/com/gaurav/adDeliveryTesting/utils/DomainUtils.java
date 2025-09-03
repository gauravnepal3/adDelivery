package com.gaurav.adDeliveryTesting.utils;

import java.net.URI;

public final class DomainUtils {
    private DomainUtils() {}

    public static String extractHost(String xDomain, String origin, String referer) {
        String host = firstHost(xDomain);
        if (host == null) host = urlHost(origin);
        if (host == null) host = urlHost(referer);
        return host == null ? "" : host.toLowerCase();
    }

    private static String firstHost(String s) {
        if (s == null || s.isBlank()) return null;
        String t = s.trim().toLowerCase();
        if (t.startsWith("http://") || t.startsWith("https://")) return urlHost(t);
        int slash = t.indexOf('/');
        return (slash > 0) ? t.substring(0, slash) : t;
    }

    private static String urlHost(String url) {
        if (url == null || url.isBlank()) return null;
        try { return URI.create(url).getHost(); } catch (Exception ignored) { return null; }
    }
}