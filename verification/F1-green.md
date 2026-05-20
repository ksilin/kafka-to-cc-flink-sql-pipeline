# F1-GREEN — Verification (filter statement live)

**Date:** 2026-04-30 21:27 GMT+2
**Compute pool:** `lfcp-kknvdm` (env `env-nvv5xz`, cluster `lkc-6w3rv2`, AWS eu-central-1)
**Phase:** GREEN — filter statement runs, sink contains the 9 expected records, diff is empty.

## Setup performed (delta from RED)

| Step | Result |
|---|---|
| Cleanup + recreate input topic + run setup DDL | OK |
| Submit filter statement `kafka-variant-f1-green` from `sql/01-filter-F1.sql` | **RUNNING** |
| Produce `F1-input.jsonl` (10 records) → `kf-input-test` | OK |
| Consume `kf-data-test` from-beginning, `--value-format jsonschema`, take first 9 | 9 records |
| Diff `F1-expected.jsonl` (9) vs actual (9) | **OK: 9 records match** |

## SQL parse traps hit

First submission of `01-filter-F1.sql` failed with:
> SQL parse failed. Encountered "timestamp" at line 36, column 9.

Cause: `timestamp` (and `value`, `time`) are reserved words in CC Flink SQL. The CREATE TABLE DDL had them backquoted but the SELECT-side `ROW<...>` alias did not. Backquoted them in `01-filter-F1.sql`, `01-filter-F1-update.sql`, and `01-filter-template.sql`.

Adding to the dialect-trap awareness: any field name in a `ROW<...>` cast that overlaps a reserved word must be backquoted, even if the same name is fine elsewhere as a JSON path argument.

## Consumer wire format

`confluent kafka topic consume` defaults to `--value-format string` and reads raw bytes. Sink topic uses CP wire format (magic byte `0x00` + 4-byte schema ID + JSON), so default consume produces non-UTF-8 bytes. Switched harness to `--value-format jsonschema` which strips the wire-format prefix and outputs the raw JSON document.

## Pass criteria met

- [x] filter statement reaches `RUNNING`
- [x] sink contains exactly the 9 expected records
- [x] canonical multiset diff is empty (`OK: 9 records match`)
- [x] every record is a properly nested JSON object (`payload` is an object, not a stringified blob)

## Validated invariants from CONTRACT.md §8

- Output `payload` is a real nested ROW (CC serializes it as a nested JSON object), not a stringified JSON.
- All 13 `payload.*` sub-fields are STRING-typed and round-trip correctly from numeric source JSON.
- `useCaseIds` is emitted as a JSON array.
- Top-level keys: `payload`, `payloadUrl`, `time`, `useCaseIds`, `userId`, `userMessageId`, `vehicleId` — alphabetical order, matching the inferred sink table.

Next: EMPTY — drop everything, recreate, no filter statement, expect 0 records on sink.
