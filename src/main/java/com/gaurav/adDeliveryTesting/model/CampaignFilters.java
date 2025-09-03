package com.gaurav.adDeliveryTesting.model;

import jakarta.persistence.*;
import lombok.Data;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Data
@Entity
public class CampaignFilters implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "campaign_id", nullable = false, unique = true)
    private Campaign campaign;

    // Coarse targeting (in ZSET key)
    @ElementCollection
    @CollectionTable(name = "campaign_countries", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "country")
    private Set<String> countries = new HashSet<>();

    @ElementCollection
    @CollectionTable(name = "campaign_languages", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "language")
    private Set<String> languages = new HashSet<>();

    @ElementCollection
    @CollectionTable(name = "campaign_devices", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "device")
    private Set<String> devices = new HashSet<>(); // e.g., Laptop, Desktop, Mobile, TV

    @ElementCollection
    @CollectionTable(name = "campaign_oses", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "os")
    private Set<String> osList = new HashSet<>();

    // Browser is now an ALLOW list (NOT in ZSET key)
    @ElementCollection
    @CollectionTable(name = "campaign_browsers", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "browser")
    private Set<String> browsers = new HashSet<>();

    // IAB allow list
    @ElementCollection
    @CollectionTable(name = "campaign_iab_category_filter", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "iab_category")
    private Set<String> iabCategory = new HashSet<>();

    // OPTIONAL allow lists (empty = no restriction)
    @ElementCollection
    @CollectionTable(name = "campaign_allowed_domain", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "allowed_domain")
    private Set<String> allowedDomain = new HashSet<>();

    @ElementCollection
    @CollectionTable(name = "campaign_allowed_ip", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "allowed_ip")
    private Set<String> allowedIP = new HashSet<>();

    // Exclude lists (empty = none blocked)
    @ElementCollection
    @CollectionTable(name = "campaign_excluded_domain", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "excluded_domain")
    private Set<String> excludedDomain = new HashSet<>();

    @ElementCollection
    @CollectionTable(name = "campaign_excluded_ip", joinColumns = @JoinColumn(name = "filter_id"))
    @Column(name = "excluded_ip")
    private Set<String> excludedIP = new HashSet<>();
}