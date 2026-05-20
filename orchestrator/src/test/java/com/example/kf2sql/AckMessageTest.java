package com.example.kf2sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AckMessageTest {

    @Test
    void success_subscribed() {
        AckMessage ack = AckMessage.success("c1", "subscribed");
        String json = ack.toJson();
        assertTrue(json.contains("\"correlationID\":\"c1\""),
            "field name must be correlationID with capital D per obsidian schema");
        assertTrue(json.contains("\"status\":\"Success\""));
        assertTrue(json.contains("\"details\":\"subscribed\""));
    }

    @Test
    void error_carriesDetailsString() {
        AckMessage ack = AckMessage.error("c2", "flink statement creation failed: SQL parse error");
        assertEquals("Error", ack.status());
        assertTrue(ack.details().contains("SQL parse error"));
    }
}
