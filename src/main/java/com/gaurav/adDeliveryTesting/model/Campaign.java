package com.gaurav.adDeliveryTesting.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
@Entity
@AllArgsConstructor
@NoArgsConstructor
public class Campaign implements Serializable {

    @Id
    private int campaignId;
    private String deliveryLink;
    private BigDecimal totalBudget;
    private BigDecimal remainingBudget;
    private BigDecimal biddingRate;

    @OneToOne(fetch = FetchType.LAZY, mappedBy = "campaign", cascade = CascadeType.ALL, optional = false)
    private CampaignFilters filters;
}
