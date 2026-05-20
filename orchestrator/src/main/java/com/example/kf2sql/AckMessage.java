package com.example.kf2sql;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * ACK message produced back to downstream per CONTRACT §4.
 *
 * Field name {@code correlationID} (uppercase D) matches the obsidian-note schema.
 */
public record AckMessage(
    @JsonProperty("correlationID") String correlationId,
    String status,
    String details
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static AckMessage success(String correlationId, String details) {
        return new AckMessage(correlationId, "Success", details);
    }

    public static AckMessage error(String correlationId, String details) {
        return new AckMessage(correlationId, "Error", details);
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("AckMessage serialization failed", e);
        }
    }
}
