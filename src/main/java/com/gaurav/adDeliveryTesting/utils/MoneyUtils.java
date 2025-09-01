package com.gaurav.adDeliveryTesting.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class MoneyUtils {
    private MoneyUtils() {}
    public static long toCents(BigDecimal amount) {
        if (amount == null) return 0L;
        return amount.setScale(2, RoundingMode.UNNECESSARY).movePointRight(2).longValueExact();
    }
    public static BigDecimal fromCents(long cents) {
        return BigDecimal.valueOf(cents, 2);
    }
}
