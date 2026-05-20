package com.example.kf2sql.quarkus;

import java.util.List;

/**
 * Subscription message from downstream per CONTRACT §5.
 * Reuses the same shape as the plain-Java variant.
 */
public record Subscription(String vehicleId, String correlationId, List<String> dataIdList) {

    public boolean isUnsubscribe() {
        return dataIdList == null || dataIdList.isEmpty();
    }

    public String mdcCsv() {
        return String.join(", ", dataIdList);
    }
}
