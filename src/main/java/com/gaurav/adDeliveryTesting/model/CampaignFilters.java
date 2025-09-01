package com.gaurav.adDeliveryTesting.model;

import jakarta.persistence.*;
import lombok.Data;

import java.io.Serializable;


@Data
@Entity
public class CampaignFilters implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "campaign_id", nullable = false, unique = true)
    private Campaign campaign;

    @ElementCollection
    @CollectionTable(name = "campaign_countries", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "country")
    private java.util.Set<String> countries = new java.util.HashSet<>();

    @ElementCollection
    @CollectionTable(name = "campaign_languages", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "language")
    private java.util.Set<String> languages = new java.util.HashSet<>();

    @ElementCollection
    @CollectionTable(name = "campaign_oses", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "os")
    private java.util.Set<String> osList = new java.util.HashSet<>();

    @ElementCollection
    @CollectionTable(name = "campaign_browsers", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "browser")
    private java.util.Set<String> browsers = new java.util.HashSet<>();
}
