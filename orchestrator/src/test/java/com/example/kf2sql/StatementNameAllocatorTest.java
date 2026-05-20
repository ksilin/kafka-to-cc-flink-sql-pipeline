package com.example.kf2sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 07.4 RED tests for {@link StatementNameAllocator}.
 *
 * Allocator gives each subscription a deterministic, monotonically increasing statement name
 * per vehicleId, persisted to a JSON file (V1 PoC; V2 swaps to a compacted Kafka topic).
 *
 * Naming convention: {@code kf-flt-<vehicleShort>-<seq>} where vehicleShort = first 8 chars
 * of vehicleId after stripping non-alphanumeric chars.
 */
class StatementNameAllocatorTest {

    @Test
    void firstCallReturnsSeq1(@TempDir Path tmp) throws IOException {
        var alloc = new StatementNameAllocator(tmp.resolve("state.json"));
        assertEquals("kf-flt-vehiclef-1", alloc.next("vehicle-fixture-001"));
    }

    @Test
    void incrementsAcrossInvocations(@TempDir Path tmp) throws IOException {
        var statePath = tmp.resolve("state.json");
        var alloc1 = new StatementNameAllocator(statePath);
        assertEquals("kf-flt-vehiclef-1", alloc1.next("vehicle-fixture-001"));
        // New allocator instance reads the same file and continues from seq 2
        var alloc2 = new StatementNameAllocator(statePath);
        assertEquals("kf-flt-vehiclef-2", alloc2.next("vehicle-fixture-001"));
    }

    @Test
    void independentPerVehicleId(@TempDir Path tmp) throws IOException {
        var alloc = new StatementNameAllocator(tmp.resolve("state.json"));
        assertEquals("kf-flt-vehiclef-1", alloc.next("vehicle-fixture-001"));
        assertEquals("kf-flt-vehiclef-1", alloc.next("vehicle-fixture-002"));
        // Both have prefix "vehiclef" because both shorten to first-8-alnum;
        // production V2 will use a hash to avoid collision; V1 PoC accepts collision.
        assertEquals("kf-flt-vehiclef-2", alloc.next("vehicle-fixture-001"));
    }

    @Test
    void currentReturnsLastAllocated(@TempDir Path tmp) throws IOException {
        var alloc = new StatementNameAllocator(tmp.resolve("state.json"));
        assertNull(alloc.current("vehicle-fixture-001"),
            "No allocation yet → null");
        alloc.next("vehicle-fixture-001");
        assertEquals("kf-flt-vehiclef-1", alloc.current("vehicle-fixture-001"));
    }

    @Test
    void persistsToFileAsJson(@TempDir Path tmp) throws IOException {
        var statePath = tmp.resolve("state.json");
        var alloc = new StatementNameAllocator(statePath);
        alloc.next("vehicle-fixture-001");
        alloc.next("vehicle-fixture-001");
        String content = Files.readString(statePath);
        assertTrue(content.contains("\"vehicle-fixture-001\""), "vehicleId key persisted");
        assertTrue(content.contains("\"seq\":2"), "seq=2 after two allocations");
    }
}
