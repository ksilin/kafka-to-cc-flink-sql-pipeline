package com.example.kf2sql;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 07.3 RED tests for {@link Subscription}.
 * Subscription is a record with Jackson parsing; tests pin the JSON contract from
 * .planning/phases/05-kafka-variant-sql-validation/CONTRACT.md §5.
 */
class SubscriptionTest {

    @Test
    void parse_validJson() {
        String json = "{\"vehicleId\":\"V1\",\"correlationId\":\"c1\",\"dataIdList\":[\"100\",\"200\"]}";
        Subscription sub = Subscription.fromJson(json);
        assertEquals("V1", sub.vehicleId());
        assertEquals("c1", sub.correlationId());
        assertEquals(List.of("100", "200"), sub.dataIdList());
    }

    @Test
    void parse_emptyDataIdList() {
        String json = "{\"vehicleId\":\"V1\",\"correlationId\":\"c2\",\"dataIdList\":[]}";
        Subscription sub = Subscription.fromJson(json);
        assertTrue(sub.dataIdList().isEmpty());
        assertTrue(sub.isUnsubscribe(),
            "Empty dataIdList = unsubscribe per CONTRACT §2");
    }

    @Test
    void parse_invalidJsonThrows() {
        assertThrows(RuntimeException.class,
            () -> Subscription.fromJson("not json"));
    }

    @Test
    void parse_F1FixtureBytes() {
        // Mirrors test-data/fixtures/F1-subscription.json
        String json = "{\"vehicleId\":\"vehicle-fixture-001\",\"correlationId\":\"f1-corr-0001\",\"dataIdList\":[\"100\",\"200\"]}";
        Subscription sub = Subscription.fromJson(json);
        assertEquals("vehicle-fixture-001", sub.vehicleId());
        assertEquals(List.of("100", "200"), sub.dataIdList());
        assertFalse(sub.isUnsubscribe());
    }

    @Test
    void mdcCsv_quotedDigits() {
        Subscription sub = new Subscription("V1", "c1", List.of("100", "200"));
        assertEquals("100, 200", sub.mdcCsv());
    }
}
