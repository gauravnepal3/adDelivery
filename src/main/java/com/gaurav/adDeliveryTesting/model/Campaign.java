package com.gaurav.adDeliveryTesting.model;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
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

    @OneToOne(mappedBy = "campaign", cascade = CascadeType.ALL)
    private CampaignFilters filters;
}
