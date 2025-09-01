package com.gaurav.adDeliveryTesting.model;

import jakarta.persistence.*;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
@Entity
public class CampaignFilters implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @ManyToOne(fetch = FetchType.LAZY)
    private Campaign campaign;

    @ElementCollection
    @CollectionTable(name = "campaign_countries", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "country")
    private List<String> countries;

    @ElementCollection
    @CollectionTable(name = "campaign_languages", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "language")
    private List<String> languages;

    @ElementCollection
    @CollectionTable(name = "campaign_oses", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "os")
    private List<String> osList;

    @ElementCollection
    @CollectionTable(name = "campaign_browsers", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "browser")
    private List<String> browsers;
}
