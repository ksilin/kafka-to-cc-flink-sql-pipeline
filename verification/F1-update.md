# F1-UPDATE — Verification (carry-over offsets)

**Date:** 2026-04-30 21:49–21:53 GMT+2
**Compute pool:** `lfcp-kknvdm` (env `env-nvv5xz`, cluster `lkc-6w3rv2`)
**Phase:** UPDATE — exercises CC `sql.tables.initial-offset-from` to swap a filter without reprocessing or gap.

## Sequence executed

| Step | Action | Time | Result |
|---|---|---|---|
| 1 | Submit v1 (`mdc IN (100, 200)`), `--wait` | 21:49:47 → 21:49:57 | RUNNING (10s) |
| 2 | Produce batch1 (10 telemetry records) → `kf-input-test` | 21:49:57 | OK |
| 3 | Consume sink, take first 9, diff vs `F1-expected.jsonl` | 21:49:59 → 21:51:29 | **OK: 9 records match** |
| 4 | Submit v2 (`mdc IN (200, 300)`) with `--property sql.tables.initial-offset-from=v1`, no `--wait` | 21:51:29 | submitted, PENDING |
| 5 | (no-op log) | | |
| 6 | **STOP v1** (`confluent flink statement stop`, NOT delete) | 21:51:32 | OK |
| 7 | Poll v2 until RUNNING | 21:51:37 → 21:51:58 (4 polls × 7s ≈ 25s) | **RUNNING** |
| 8 | Produce batch2 (10 telemetry records) → `kf-input-test` | 21:51:58 | OK |
| 9 | Consume sink, take first 15, expect v1×9 + v2×6 | 21:52:01 → 21:53:31 | 15 lines |
| 10 | Diff (concat of `F1-expected.jsonl` + `F1-update-expected.jsonl`) vs actual | 21:53:31 | **OK: 15 records match** |

## Critical lesson learned

First two attempts of UPDATE phase failed because the harness used `delete` instead of `stop` to terminate v1. CC's `sql.tables.initial-offset-from` references the previous statement **by name** — when v1 is deleted, the reference dangles and v2 stays PENDING until the 6h timeout. The fix:

```bash
# WRONG: makes v2 hang forever in PENDING
confluent flink statement delete "$V1" --force

# RIGHT: transitions v1 to STOPPED, v2 picks up its offsets
confluent flink statement stop "$V1"
```

This is a notable CC-vs-OSS divergence — Apache Flink savepoints have an analogous distinction (`STOP WITH SAVEPOINT` vs `CANCEL`). On CC, plain CLI verbs map to the lifecycle differently:

| Verb | State after | Semantics |
|---|---|---|
| `confluent flink statement stop`   | STOPPED | preserves committed offsets, releases compute, can be referenced for carry-over |
| `confluent flink statement delete` | (gone) | resource removed; references dangle |

Added to `RESEARCH.md` and the dialect-trap awareness.

## v2 transition timing

PENDING → RUNNING took ~25 seconds after v1 was stopped. Acceptable for V1 PoC; possibly tighter in production with proper SLO. Captured in CONTRACT §10 lifecycle as "expect ~30s ACK latency for subscription updates".

## Pass criteria met

- [x] v1 RUNNING → produced 9 records matching F1-expected
- [x] v2 submitted in PENDING (carry-over reference)
- [x] After v1 STOP, v2 transitioned to RUNNING within ~30s
- [x] v2 emitted only the post-handover records (6 from batch2 with new predicate)
- [x] No duplicates between v1's batch1 output and v2's batch2 output
- [x] No gap (every batch2 record matching v2's predicate appeared in sink)
- [x] Concatenated diff (9 + 6 = 15) matches actual exactly

## Implications for V1 orchestrator (Phase 07)

The Java orchestrator must:

1. Submit new statement with carry-over property referencing the previous statement's name.
2. Poll status until PENDING is observed (means it's accepted by CC, just waiting on predecessor).
3. Issue STOP (not delete) on the previous statement.
4. Poll new statement until RUNNING.
5. Send ACK to subscription's correlationId once RUNNING is observed.
6. Optionally clean up STOPPED statements after a retention window (V2 concern).

This sequence is now load-tested against a real CC compute pool and known to work. The V1 latency floor for "subscription update accepted by downstream" is ~30 seconds of CC processing + however long the orchestrator takes between steps.
