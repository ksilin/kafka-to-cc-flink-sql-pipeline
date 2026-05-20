package com.example.kf2sql.quarkus;

/**
 * Identical to the plain-Java variant. Pure function — no framework dependency.
 */
public class SqlGenerator {

    private final String template;

    public SqlGenerator(String template) {
        this.template = template;
    }

    public String fromSubscription(Subscription sub, String inputTopic, String outputTopic) {
        if (sub.isUnsubscribe()) {
            throw new IllegalArgumentException(
                    "Empty dataIdList = unsubscribe; must not call SqlGenerator. vehicleId=" + sub.vehicleId());
        }
        return template
                .replace("__VEHICLE_ID__", sub.vehicleId())
                .replace("__MDC_ID_CSV__", sub.mdcCsv())
                .replace("__INPUT_TOPIC__", "`" + inputTopic + "`")
                .replace("__OUTPUT_TOPIC__", "`" + outputTopic + "`");
    }
}
