package com.example.kf2sql.quarkus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StatementNameAllocatorTest {

    @Test
    void firstCallReturnsSeq1() {
        var alloc = new StatementNameAllocator();
        assertEquals("kf-flt-vehiclef-1", alloc.next("vehicle-fixture-001"));
    }

    @Test
    void incrementsAcrossInvocations() {
        var alloc = new StatementNameAllocator();
        assertEquals("kf-flt-vehiclef-1", alloc.next("vehicle-fixture-001"));
        assertEquals("kf-flt-vehiclef-2", alloc.next("vehicle-fixture-001"));
    }

    @Test
    void currentReturnsLastAllocated() {
        var alloc = new StatementNameAllocator();
        assertNull(alloc.current("V1"));
        alloc.next("V1");
        assertEquals("kf-flt-v1-1", alloc.current("V1"));
    }
}
