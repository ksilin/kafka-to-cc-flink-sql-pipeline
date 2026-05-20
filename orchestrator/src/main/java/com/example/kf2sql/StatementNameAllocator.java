package com.example.kf2sql;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Allocates deterministic, monotonically increasing Flink statement names per vehicleId.
 *
 * <p>Naming: {@code kf-flt-<vehicleShort>-<seq>}, where {@code vehicleShort} is the first
 * 8 characters of {@code vehicleId} after stripping non-alphanumeric characters and
 * lowercasing.
 *
 * <p>Persistence: a single JSON file storing
 * {@code {"vehicles": {"<vehicleId>": {"current": "kf-flt-...-N", "seq": N}}}}.
 *
 * <p>V1 PoC concession: allocator is single-process and not concurrency-safe. Two parallel
 * orchestrators would race on this file. V2 will swap to a compacted Kafka topic
 * (CONTRACT §10).
 */
public class StatementNameAllocator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path statePath;
    private final Map<String, Entry> entries;

    public StatementNameAllocator(Path statePath) throws IOException {
        this.statePath = statePath;
        this.entries = load(statePath);
    }

    /** Allocate the next name for the given vehicleId and persist. */
    public String next(String vehicleId) throws IOException {
        Entry entry = entries.computeIfAbsent(vehicleId, v -> new Entry(0, null));
        int seq = entry.seq + 1;
        String name = "kf-flt-" + shortenVehicleId(vehicleId) + "-" + seq;
        entries.put(vehicleId, new Entry(seq, name));
        save();
        return name;
    }

    /** Return the most recently allocated name for vehicleId, or null if none. */
    public String current(String vehicleId) {
        Entry entry = entries.get(vehicleId);
        return entry == null ? null : entry.current;
    }

    private static String shortenVehicleId(String vehicleId) {
        String alnum = vehicleId.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
        return alnum.length() <= 8 ? alnum : alnum.substring(0, 8);
    }

    private void save() throws IOException {
        Map<String, Object> root = Map.of("vehicles", entries);
        Files.writeString(statePath, MAPPER.writeValueAsString(root));
    }

    private static Map<String, Entry> load(Path statePath) throws IOException {
        if (!Files.exists(statePath)) {
            return new HashMap<>();
        }
        var root = MAPPER.readValue(
            Files.readString(statePath),
            new TypeReference<Map<String, Map<String, Entry>>>() {});
        Map<String, Entry> vehicles = root.get("vehicles");
        return vehicles == null ? new HashMap<>() : new HashMap<>(vehicles);
    }

    public record Entry(int seq, String current) {}
}
