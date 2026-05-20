package com.example.kf2sql;

/**
 * Generates a concrete CC Flink SQL filter statement from a subscription + topic names.
 * Pure function: same inputs ⇒ same SQL bytes.
 *
 * Token replacement on the template at sql/01-filter-template.sql:
 * <ul>
 *   <li>{@code __VEHICLE_ID__}   → vehicleId (no quotes added by replacement; the template
 *       wraps the placeholder in single quotes already)</li>
 *   <li>{@code __MDC_ID_CSV__}   → comma-space CSV of integer mdc_ids</li>
 *   <li>{@code __INPUT_TOPIC__}  → backtick-wrapped topic identifier</li>
 *   <li>{@code __OUTPUT_TOPIC__} → backtick-wrapped topic identifier</li>
 * </ul>
 *
 * No SQL escaping is done on inputs — the orchestrator's caller is responsible for
 * validating the subscription before reaching here. V1 trusts CC's parser to reject
 * malformed identifiers; V2 will add a registry-backed validator (SQ-Q6).
 */
public class SqlGenerator {

    private final String template;

    public SqlGenerator(String template) {
        this.template = template;
    }

    public String fromSubscription(Subscription sub, String inputTopic, String outputTopic) {
        if (sub.isUnsubscribe()) {
            throw new IllegalArgumentException(
                "Empty dataIdList = unsubscribe; orchestrator must not call SqlGenerator. "
                + "vehicleId=" + sub.vehicleId());
        }
        return template
            .replace("__VEHICLE_ID__", sub.vehicleId())
            .replace("__MDC_ID_CSV__", sub.mdcCsv())
            .replace("__INPUT_TOPIC__", "`" + inputTopic + "`")
            .replace("__OUTPUT_TOPIC__", "`" + outputTopic + "`");
    }
}
