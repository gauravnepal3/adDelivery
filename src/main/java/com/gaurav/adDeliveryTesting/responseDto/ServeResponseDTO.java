package com.gaurav.adDeliveryTesting.responseDto;

import java.math.BigDecimal;

public record ServeResponseDTO(
        int campaignId,
        String deliveryLink,
        BigDecimal biddingRate,
        BigDecimal remainingBudget
) {
}
