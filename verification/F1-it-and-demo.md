# Phase 08 — IT + Demo Verification

**Date:** 2026-05-05 12:56–13:01 GMT+2
**Compute pool:** `lfcp-kknvdm` (env `env-nvv5xz`, cluster `lkc-6w3rv2`)

## 08.1 — FlinkLifecycleCcIT against real CC

```
mvn verify -DccIntegration=true

[INFO] Running com.example.kf2sql.FlinkLifecycleCcIT
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 17.01 s
[INFO] BUILD SUCCESS
```

| Test | Result | Notes |
|---|---|---|
| `submit_ddl_reachesCompleted`        | ✅ | DDL submitted via FlinkLifecycle.submit() with `--wait`, completed in ~10s |
| `describe_returnsCompletedPhase`     | ✅ | `FlinkLifecycle.describe()` returns the literal status string |
| `waitForRunning_throwsOnCompletedNotRunning` | ✅ | A COMPLETED DDL is not RUNNING; polling for RUNNING times out as expected |

### Bug found + fixed

First IT run failed `describe_returnsCompletedPhase` (returned `UNKNOWN`). Cause: `FlinkLifecycle.describe()` parsed `status.phase` (nested object) but the actual CC describe schema returns `status` as a plain string ("COMPLETED", "RUNNING", ...). Older docs / different endpoints may use the nested form.

**Fix** ([FlinkLifecycle.java:88-94](../orchestrator/src/main/java/com/example/kf2sql/FlinkLifecycle.java)):
```java
JsonNode status = root.get("status");
if (status == null) return "UNKNOWN";
if (status.isTextual()) return status.asText();              // current CC schema
JsonNode phase = status.get("phase");                         // legacy nested schema
return phase == null ? "UNKNOWN" : phase.asText();
```

Added to lessons docs as a CC schema gotcha.

## 08.2 — End-to-end demo runner

```
./kafka-to-sql-filter/test-data/run-fixture-demo.sh

[12:58:24] pre-flight: env=env-nvv5xz cluster=lkc-6w3rv2 pool=lfcp-kknvdm
[12:58:26] cleanup: drop demo statements + topics + subjects
[12:58:57] creating input topic kf-input-test
[12:59:12] submitting DDL kf-setup-output-table
[12:59:17] step 1: SUBSCRIBE V1 + [100, 200]
           ACK1: {"status":"Success","details":"subscribed","correlationID":"demo-c1"}
[12:59:42] step 2: UPDATE V1 + [200, 300] (carry-over offsets)
           ACK2: {"status":"Success","details":"updated","correlationID":"demo-c2"}
[13:00:20] step 3: UNSUBSCRIBE V1
           ACK3: {"status":"Success","details":"unsubscribed","correlationID":"demo-c3"}
[13:00:24] demo OK — 3 ACKs received in sequence
```

| Step | Action | Wall time | ACK details |
|---|---|---|---|
| 1 | SUBSCRIBE V1 + [100, 200]   | ~25 s (DDL had completed earlier; this measures filter create + RUNNING) | subscribed |
| 2 | UPDATE V1 + [200, 300]      | ~38 s (v2 PENDING → v1 stop → v2 RUNNING ~25-30s) | updated |
| 3 | UNSUBSCRIBE V1              | ~4 s (just stop)                                | unsubscribed |

## What was validated end-to-end

- Java orchestrator parses subscription JSON, generates SQL byte-equal to the F1 fixture.
- Java orchestrator invokes `confluent flink statement create` with the right flag schema (no `--cloud`/`--region` per CONTRACT, with `--environment`).
- Carry-over offsets work via the Java path: v2 created with `sql.tables.initial-offset-from`, v1 stopped, v2 transitioned to RUNNING.
- ACK serialization preserves `correlationID` field name (capital D) per obsidian schema.
- Statement-name allocator persists across the three CLI invocations (the demo runs each step in a fresh JVM but reads the same state file).
- File-mode dispatch returns 0 on Success ACK; non-zero on Error ACK.

## What was NOT validated here (still TODO)

- **Kafka mode** of the orchestrator. Not exercised by this demo — the demo uses file-mode (one orchestrator JVM per subscription). Kafka mode requires a real subscription topic + ack topic + Kafka API key.
- **Data flow.** This demo asserts ACKs only — no fixture data was produced, so we didn't verify that records actually flow through the filter. Phase 05's `verify-phase1.sh green` covered that, but only via the bash harness, not via the orchestrator.
- **Multi-vehicle scenarios.** Demo uses one vehicleId throughout.
- **Concurrent subscriptions.** Allocator's file persistence is not concurrency-safe (per CONTRACT §10).
