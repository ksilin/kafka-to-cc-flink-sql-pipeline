package com.example.kf2sql.quarkus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AckMessageTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void success_subscribed() throws Exception {
        AckMessage ack = AckMessage.success("c1", "subscribed");
        String json = MAPPER.writeValueAsString(ack);
        assertTrue(json.contains("\"correlationID\":\"c1\""),
                "field name must be correlationID with capital D");
        assertTrue(json.contains("\"status\":\"Success\""));
        assertTrue(json.contains("\"details\":\"subscribed\""));
    }

    @Test
    void error_carriesDetails() {
        AckMessage ack = AckMessage.error("c2", "flink failed: SQL parse error");
        assertEquals("Error", ack.status());
        assertTrue(ack.details().contains("SQL parse error"));
    }
}
