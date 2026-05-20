package com.example.kf2sql.quarkus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void parse_validJson() throws Exception {
        String json = """
                {"vehicleId":"V1","correlationId":"c1","dataIdList":["100","200"]}""";
        Subscription sub = MAPPER.readValue(json, Subscription.class);
        assertEquals("V1", sub.vehicleId());
        assertEquals("c1", sub.correlationId());
        assertEquals(List.of("100", "200"), sub.dataIdList());
    }

    @Test
    void parse_emptyDataIdList() throws Exception {
        String json = """
                {"vehicleId":"V1","correlationId":"c2","dataIdList":[]}""";
        Subscription sub = MAPPER.readValue(json, Subscription.class);
        assertTrue(sub.isUnsubscribe());
    }

    @Test
    void parse_F1FixtureBytes() throws Exception {
        String json = """
                {"vehicleId":"vehicle-fixture-001","correlationId":"f1-corr-0001","dataIdList":["100","200"]}""";
        Subscription sub = MAPPER.readValue(json, Subscription.class);
        assertEquals("vehicle-fixture-001", sub.vehicleId());
        assertEquals(List.of("100", "200"), sub.dataIdList());
        assertFalse(sub.isUnsubscribe());
    }

    @Test
    void mdcCsv_formats() {
        Subscription sub = new Subscription("V1", "c1", List.of("100", "200"));
        assertEquals("100, 200", sub.mdcCsv());
    }

    @Test
    void isUnsubscribe_nullList() {
        Subscription sub = new Subscription("V1", "c1", null);
        assertTrue(sub.isUnsubscribe());
    }
}
