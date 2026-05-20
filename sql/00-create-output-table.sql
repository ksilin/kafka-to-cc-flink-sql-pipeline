-- kafka-to-sql-filter / Phase 05 / DDL
--
-- Creates the sink topic + registers a JSON Schema in Schema Registry, in one DDL.
-- This is CC-Flink-native (NOT Apache Flink): the table backs a Kafka topic with
-- the corresponding key/value schemas registered automatically.
--
-- Source: https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html
-- See also: .planning/phases/05-kafka-variant-sql-validation/RESEARCH.md (R2)
--
-- Format choice: 'json-registry' (NOT 'json'). The latter fails on CC with
-- "Unsupported format: json" — verified by original variant 2026-04-22, see HANDOFF.md.
-- The 'json-registry' suffix binds the format to a JSON Schema registered in SR.
--
-- Column types: all STRING for sub-fields. Confluent Cloud silently NULLs columns
-- when the source JSON has a numeric value but the column is declared STRING — and
-- vice versa. See HANDOFF.md "Typed table with polymorphic value field".
-- All-STRING here is a deliberate concession; tighten in V2 if downstream confirms typed
-- expectations (STAKEHOLDER-QUESTIONS Q12 + the all-STRING addendum in CONTRACT §8).

CREATE TABLE `kf-data-test` (
  `payload` ROW<
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
  >,
  `payloadUrl`     STRING,
  `time`           STRING,
  `useCaseIds`     ARRAY<STRING>,
  `userId`         STRING,
  `userMessageId`  STRING,
  `vehicleId`      STRING
) WITH (
  'value.format'                     = 'json-registry',
  'value.json-registry.id-encoding'  = 'header',
  'changelog.mode'                   = 'append'
);
-- id-encoding = header: schema ID goes in Kafka record header, payload is clean JSON.
-- Downstream consumers don't need SR integration to read the data.
-- Flink's read path auto-resolves via header → prefix → fallback chain regardless.
-- See docs/v1-follow-up-investigations.md F4 for rationale.
