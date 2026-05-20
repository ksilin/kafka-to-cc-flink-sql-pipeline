# kafka-to-cc-flink-sql-pipeline

Kafka-driven pipeline that translates **subscription messages** into Confluent Cloud Flink SQL statements, filtering telemetry output by `vehicleId + mdc_id` and producing flat per-signal records.

## What's here

| Path | Purpose |
|---|---|
| `sql/` | Flink SQL DDL + parameterized DML templates |
| `orchestrator/` | Plain-Java orchestrator (CLI-driven) |
| `orchestrator-quarkus/` | Quarkus variant (reactive Kafka consumer) |
| `test-data/` | Fixtures, generators, demo scripts |
| `tests/` | Verification harness against real Confluent Cloud |
| `verification/` | Run logs from CC validation |
| `terraform/` | Infrastructure-as-code for CC resources |
| `config/` | Per-machine env var template |

## Quick start

```bash
cp config/env.example.sh config/env.sh
$EDITOR config/env.sh           # set CC env / cluster / pool IDs
source config/env.sh

# Local sanity check
python3 tests/diff_jsonl.py test-data/fixtures/F1-expected.jsonl test-data/fixtures/F1-expected.jsonl
# -> "OK: 9 records match"

# Real CC verification (creates topics, runs Flink statements)
./tests/verify-phase1.sh green
```

## Key constraint

This runs on **Confluent Cloud Flink SQL** -- not Apache Flink. The dialect differs significantly. See `sql/README.md` for format choices and column-type rationale.
