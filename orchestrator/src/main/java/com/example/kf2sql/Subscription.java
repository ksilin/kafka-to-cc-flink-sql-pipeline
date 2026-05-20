package com.example.kf2sql;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Subscription message from downstream. JSON shape per CONTRACT §5:
 * <pre>
 * { "vehicleId": "...", "correlationId": "...", "dataIdList": ["mdc1","mdc2"] }
 * </pre>
 *
 * Empty {@code dataIdList} = unsubscribe.
 */
public record Subscription(String vehicleId, String correlationId, List<String> dataIdList) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Subscription fromJson(String json) {
        try {
            return MAPPER.readValue(json, Subscription.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public boolean isUnsubscribe() {
        return dataIdList == null || dataIdList.isEmpty();
    }

    /** Comma-space separated mdc_id list for SQL IN clause: "100, 200, 300". */
    public String mdcCsv() {
        return String.join(", ", dataIdList);
    }
}
