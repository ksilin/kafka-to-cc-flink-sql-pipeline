package com.example.kf2sql.quarkus;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ACK message produced back to downstream per CONTRACT §4.
 * Field name {@code correlationID} (uppercase D) matches the obsidian-note schema.
 */
public record AckMessage(
        @JsonProperty("correlationID") String correlationId,
        String status,
        String details) {

    public static AckMessage success(String correlationId, String details) {
        return new AckMessage(correlationId, "Success", details);
    }

    public static AckMessage error(String correlationId, String details) {
        return new AckMessage(correlationId, "Error", details);
    }
}
