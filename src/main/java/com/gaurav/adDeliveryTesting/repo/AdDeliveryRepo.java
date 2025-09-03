package com.gaurav.adDeliveryTesting.repo;

import com.gaurav.adDeliveryTesting.model.Campaign;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.List;

public interface AdDeliveryRepo extends JpaRepository<Campaign, Integer> {

    // Paged version (used by warmup)
    @EntityGraph(attributePaths = {
            "filters",
            "filters.countries",
            "filters.languages",
            "filters.devices",
            "filters.osList",
            "filters.browsers",
            "filters.iabCategory",
            "filters.allowedDomain",
            "filters.allowedIP",
            "filters.excludedDomain",
            "filters.excludedIP"
    })
    @Query("select c from Campaign c")
    Page<Campaign> findAllWithFilters(Pageable pageable);

    @Query("select c.campaignId from Campaign c")
    List<Integer> findAllIds();

    @EntityGraph(attributePaths = {
            "filters",
            "filters.countries",
            "filters.languages",
            "filters.devices",
            "filters.osList",
            "filters.browsers",
            "filters.iabCategory",
            "filters.allowedDomain",
            "filters.allowedIP",
            "filters.excludedDomain",
            "filters.excludedIP"
    })
    @Query("select c from Campaign c where c.campaignId in :ids")
    List<Campaign> findBatchWithFilters(@Param("ids") Collection<Integer> ids);
    // List version (used by fallback)
    @EntityGraph(attributePaths = {
            "filters",
            "filters.countries",
            "filters.languages",
            "filters.devices",
            "filters.osList",
            "filters.browsers",
            "filters.iabCategory",
            "filters.allowedDomain",
            "filters.allowedIP",
            "filters.excludedDomain",
            "filters.excludedIP"
    })
    @Query("select c from Campaign c")
    List<Campaign> findAllWithFilters();
}