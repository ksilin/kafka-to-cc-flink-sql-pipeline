-- kafka-to-sql-filter / Concrete F1 SQL
-- Generated from 01-filter-template.sql with bindings:
--   __VEHICLE_ID__   = vehicle-fixture-001
--   __MDC_ID_CSV__   = 100, 200
--   __INPUT_TOPIC__  = `kf-input-test`
--   __OUTPUT_TOPIC__ = `kf-data-test`
-- Subscription source: test-data/fixtures/F1-subscription.json

INSERT INTO `kf-data-test`
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
FROM `kf-input-test`
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
WHERE JSON_VALUE(CAST(val AS STRING), '$.vehicleId') = 'vehicle-fixture-001'
  AND CAST(JSON_VALUE(signal_str, '$.mdc_id') AS BIGINT) IN (100, 200);
