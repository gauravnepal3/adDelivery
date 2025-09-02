package com.gaurav.adDeliveryTesting.responseDto;

public record CampaignResponseDto(
    int campaignId,
    String deliveryLink,
    long bidCents,
    long remainingCents){
}
