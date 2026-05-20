# F1-RED — Verification (no filter statement)

**Date:** 2026-04-30 21:22 GMT+2
**Compute pool:** `lfcp-kknvdm` (env `env-nvv5xz`, cluster `lkc-6w3rv2`, AWS eu-central-1)
**Phase:** RED — sanity-check that without a filter statement the sink stays empty and the diff harness reports a failure.

## Setup performed

| Step | Result |
|---|---|
| `confluent environment use env-nvv5xz` | OK |
| `confluent kafka cluster use lkc-6w3rv2` | OK |
| Cleanup (drop pre-existing topics + statements + subjects) | OK |
| Create input topic `kf-input-test` (1 partition, no schema) | OK |
| Submit DDL `kf-setup-output-table` (CREATE TABLE `kf-data-test`) | **COMPLETED** — first time the `'value.format' = 'json-registry'` form was confirmed working on this env |
| Produce `F1-input.jsonl` (10 records) → `kf-input-test` | OK |
| Consume `kf-data-test` from-beginning, timeout 15s | 0 lines (expected) |
| Diff `F1-expected.jsonl` (9) vs actual (0) | DIFF reported, 9 missing |
| Harness exit | 0 (RED outcome confirmed) |

## Notes

- The DDL took ~30 s to reach `RUNNING` and registered the JSON Schema in SR (subject `kf-data-test-value` auto-created by CC).
- The sink topic `kf-data-test` was created by the DDL — no separate `confluent kafka topic create` step needed.
- API key for Kafka produce/consume: from CLI keychain (active api-key for the cluster).

## Pass criteria met

- [x] sink stays empty without a filter statement
- [x] diff harness reports DIFF (9 missing, 0 extra)
- [x] harness exits 0 to mark RED-correctness

Next: GREEN — submit filter statement, expect 9 records, diff is empty.
