package com.example.kf2sql.quarkus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * In-memory allocator for V1. Simpler than the file-backed plain-Java variant —
 * Quarkus manages the singleton lifecycle, so in-memory state survives across
 * subscription messages within the same JVM. Restarts lose state (acceptable for PoC).
 *
 * V2: swap to Panache entity or compacted Kafka topic.
 */
@ApplicationScoped
public class StatementNameAllocator {

    private final Map<String, AtomicInteger> sequences = new ConcurrentHashMap<>();
    private final Map<String, String> currentNames = new ConcurrentHashMap<>();

    public String next(String vehicleId) {
        int seq = sequences
                .computeIfAbsent(vehicleId, v -> new AtomicInteger(0))
                .incrementAndGet();
        String name = "kf-flt-" + shorten(vehicleId) + "-" + seq;
        currentNames.put(vehicleId, name);
        return name;
    }

    public String current(String vehicleId) {
        return currentNames.get(vehicleId);
    }

    private static String shorten(String vehicleId) {
        String alnum = vehicleId.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
        return alnum.length() <= 8 ? alnum : alnum.substring(0, 8);
    }
}
