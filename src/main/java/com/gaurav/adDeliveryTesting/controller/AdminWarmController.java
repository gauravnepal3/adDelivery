package com.gaurav.adDeliveryTesting.controller;

import com.gaurav.adDeliveryTesting.service.WarmService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/admin")
public class AdminWarmController {

    private final WarmService warm;

    public AdminWarmController(WarmService warm) {
        this.warm = warm;
    }

    /** Warm a single campaign completely. */
    @PostMapping("/reindex/{id}")
    public ResponseEntity<String> reindexOne(@PathVariable int id) {
        boolean ok = warm.warmOne(id);
        if (!ok) return ResponseEntity.notFound().build();
        return ResponseEntity.ok("Reindexed campaign " + id);
    }

    /**
     * Warm all in pages. Example:
     * POST /admin/warm-all?pageSize=5000&batchLoadSize=1000
     */
    @PostMapping("/warm-all")
    public ResponseEntity<String> warmAll(@RequestParam(defaultValue = "5000") int pageSize,
                                          @RequestParam(defaultValue = "1000") int batchLoadSize) {
        int n = warm.warmAllPaged(pageSize, batchLoadSize);
        return ResponseEntity.ok("Warmed " + n + " campaigns");
    }
}