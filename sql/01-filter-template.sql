-- kafka-to-sql-filter / SQL template
--
-- Filters telemetry output records by (vehicleId, mdc_id IN list).
-- Handles BOTH output formats:
--   - Flat (signal packaging OFF): one signal per record, fields at payload.*
--   - Nested (signal packaging ON): payload.signals[] array with multiple signals
--
-- COALESCE trick: if signals[] exists, UNNEST it. Otherwise wrap the flat payload
-- as a one-element array so the same CROSS JOIN UNNEST works for both formats.
--
-- Tokens (replace before submit):
--   __VEHICLE_ID__   string literal, e.g. 'vehicle-fixture-001'
--   __MDC_ID_CSV__   integer CSV, e.g. 100, 200
--   __INPUT_TOPIC__  identifier, e.g. `kf-input-test`
--   __OUTPUT_TOPIC__ identifier, e.g. `kf-data-test`
--
-- Confluent Cloud Flink rules applied (../CLAUDE.md):
--   - No CREATE TABLE; rely on Schema Registry inferred tables.
--   - JSON_VALUE / JSON_QUERY on raw VARBINARY (CAST val AS STRING).
--   - CROSS JOIN UNNEST with COALESCE fallback for format-agnostic handling.
--   - JSON_QUERY 'strict' mode (not 'lax') — lax returns [] not NULL for missing keys.
--   - No SET sql.state-ttl; pass --property at statement creation time if needed.
--   - INSERT INTO column order = alphabetical of inferred sink columns.
--
-- Signal-level fields (from signal_str): containerMsgId, g_id, mdc_id, name,
--   timestamp, unit, value
-- Parent-level fields (from $.payload.*): containerId, containerMsgCount,
--   correlationId, ingest_timestamp, orderId, orderVersion

INSERT INTO __OUTPUT_TOPIC__
SELECT
    CAST(ROW(
        JSON_VALUE(CAST(val AS STRING), '$.payload.containerId'),
        JSON_VALUE(CAST(val AS STRING), '$.payload.containerMsgCount'),
        JSON_VALUE(signal_str, '$.containerMsgId'),
        JSON_VALUE(CAST(val AS STRING), '$.payload.correlationId'),
        JSON_VALUE(signal_str, '$.g_id'),
        JSON_VALUE(CAST(val AS STRING), '$.payload.ingest_timestamp'),
        JSON_VALUE(signal_str, '$.mdc_id'),
        JSON_VALUE(signal_str, '$.name'),
        JSON_VALUE(CAST(val AS STRING), '$.payload.orderId'),
        JSON_VALUE(CAST(val AS STRING), '$.payload.orderVersion'),
        JSON_VALUE(signal_str, '$.timestamp'),
        JSON_VALUE(signal_str, '$.unit'),
        JSON_VALUE(signal_str, '$.value')
    ) AS ROW<
        `containerId`       STRING,
        `containerMsgCount` STRING,
        `containerMsgId`    STRING,
        `correlationId`     STRING,
        `g_id`              STRING,
        `ingest_timestamp`  STRING,
        `mdc_id`            STRING,
        `name`              STRING,
        `orderId`           STRING,
        `orderVersion`      STRING,
        `timestamp`         STRING,
        `unit`              STRING,
        `value`             STRING
    >) AS `payload`,
    '' AS payloadUrl,
    JSON_VALUE(CAST(val AS STRING), '$.time') AS `time`,
    CAST(
        JSON_QUERY(CAST(val AS STRING), '$.useCaseIds' RETURNING ARRAY<STRING>)
        AS ARRAY<STRING>
    ) AS useCaseIds,
    JSON_VALUE(CAST(val AS STRING), '$.userId') AS userId,
    JSON_VALUE(CAST(val AS STRING), '$.userMessageId') AS userMessageId,
    JSON_VALUE(CAST(val AS STRING), '$.vehicleId') AS vehicleId
FROM __INPUT_TOPIC__
CROSS JOIN UNNEST(
    COALESCE(
        CAST(
            JSON_QUERY(CAST(val AS STRING), 'strict $.payload.signals[*]'
                RETURNING ARRAY<STRING>)
            AS ARRAY<STRING>
        ),
        ARRAY[JSON_QUERY(CAST(val AS STRING), '$.payload')]
    )
) AS T(signal_str)
WHERE JSON_VALUE(CAST(val AS STRING), '$.vehicleId') = '__VEHICLE_ID__'
  AND CAST(JSON_VALUE(signal_str, '$.mdc_id') AS BIGINT) IN (__MDC_ID_CSV__);
