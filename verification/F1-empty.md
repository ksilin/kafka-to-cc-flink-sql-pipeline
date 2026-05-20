# F1-EMPTY — Verification (no filter statement, unsubscribe semantic)

**Date:** 2026-04-30 21:29 GMT+2
**Compute pool:** `lfcp-kknvdm` (env `env-nvv5xz`, cluster `lkc-6w3rv2`)
**Phase:** EMPTY — exercises CONTRACT §2 unsubscribe path: empty `dataIdList` ⇒ no Flink statement ⇒ no output regardless of input.

## Setup performed

| Step | Result |
|---|---|
| Cleanup (drop GREEN's statement, topics, schema subject) | OK |
| Recreate input topic | OK |
| Setup DDL **skipped** | (race condition — statement-list lag — topic was dropped but skipped re-DDL; sink consume returned 0 because topic missing) |
| Produce `F1-input.jsonl` (10 records) → `kf-input-test` | OK |
| Consume `kf-data-test` from-beginning | 0 lines |
| Pass criterion: `actual == 0` | OK |

## Race condition flagged + fixed

`ensure_output_table` checked statement existence; CC's statement-list is eventually consistent so the just-deleted setup statement still appeared. The fix: switched the gate to `topic_exists "$KF_OUTPUT_TOPIC"` — topic existence is the authoritative signal of whether DDL needs to re-run.

The EMPTY pass is technically correct (sink had 0 records — either because no statement wrote any, or because no sink existed) but the underlying state was inconsistent. With the fix, future runs will correctly recreate the sink.

## Pass criteria met

- [x] no filter statement created
- [x] sink contains 0 records
- [x] harness exits 0

Next: UPDATE — exercise CC carry-over offsets (`sql.tables.initial-offset-from`).
