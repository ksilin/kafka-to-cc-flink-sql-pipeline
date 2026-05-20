#!/usr/bin/env python3
"""Generate deterministic telemetry output fixtures for the kafka-to-sql-filter variant.

Supports both telemetry output formats:
  --format flat   (default) — one signal per record, fields at payload.*
  --format nested — payload.signals[] array, multiple signals per message

Modes:
  fixture F1   — reproduces test-data/fixtures/F1-input.jsonl (flat) or
                 F1-input-nested.jsonl (nested) byte-for-byte
  custom       — parameterized: --records 'V1+100:speed:42/km/h,V1+200:mileage:12345/km;V2+300:battery:50/%'

Record syntax (--records):
  Records separated by ';'
  Each record: <vehicle>+<signal>[,<signal>...]
  Each signal: <mdc>:<name>:<value>/<unit>
  Multi-signal records produce one output line per signal (flat, not nested).

Examples:

  $ generate-telemetry-fixture.py --fixture F1 > /tmp/gen.jsonl
  $ diff <(sort /tmp/gen.jsonl) <(sort fixtures/F1-input.jsonl) && echo OK

  $ generate-telemetry-fixture.py --custom \\
      --records 'V1+100:speed:42/km/h;V1+200:mileage:12345/km;V2+100:speed:99/km/h' \\
      --user u1 --time 2026-04-30T08:00:00Z
"""
import argparse
import json
import sys
from typing import Iterator


# F1 fixture spec: matches test-data/fixtures/F1-input.jsonl exactly.
# Tuples: (vehicleId, base_time_offset_s, signals)
# Each signal: (g_id, mdc_id, name, value, unit, containerMsgId, timestamp_offset_ms)
F1_RECORDS = [
    ("vehicle-fixture-001", 1, [(1, 100, "speed", "42", "km/h", 1, 0)]),
    ("vehicle-fixture-001", 2, [(2, 200, "mileage", "12345", "km", 1, 0)]),
    ("vehicle-fixture-001", 3, [
        (1, 100, "speed", "55", "km/h", 1, 0),
        (2, 200, "mileage", "67890", "km", 2, 100),
        (3, 300, "battery_toc", "77.5", "%", 3, 200),
    ]),
    ("vehicle-fixture-001", 4, [
        (3, 300, "battery_toc", "50.0", "%", 1, 0),
        (9, 999, "unknown", "0", "none", 2, 100),
    ]),
    ("vehicle-fixture-002", 5, [
        (1, 100, "speed", "99", "km/h", 1, 0),
        (2, 200, "mileage", "500", "km", 2, 100),
    ]),
    ("vehicle-fixture-001", 6, [(1, 100, "speed", "60", "km/h", 1, 0)]),
    ("vehicle-fixture-002", 7, [(3, 300, "battery_toc", "22.2", "%", 1, 0)]),
    ("vehicle-fixture-001", 8, [
        (2, 200, "mileage", "88888", "km", 1, 0),
        (9, 999, "unknown", "1", "none", 2, 100),
    ]),
    # Record 9: empty signals — no output (producer never sends zero-signal records)
    ("vehicle-fixture-001", 10, [
        (1, 100, "speed", "70", "km/h", 1, 0),
        (1, 100, "speed", "71", "km/h", 2, 100),
        (2, 200, "mileage", "99999", "km", 3, 200),
    ]),
]

F1_BASE_TIME_MS = 1745999940000
F1_USER_ID = "user-f1"
F1_USE_CASE = "uc-f1"
F1_SCHEMA_ID = "sch-f1-0001"


def fmt_iso_time(idx: int) -> str:
    return f"2026-04-30T08:00:{idx:02d}Z"


def make_flat_record(vehicle: str, idx: int, signal: tuple, num_signals: int,
                     base_ts: int) -> dict:
    g_id, mdc_id, name, value, unit, container_msg_id, ts_offset = signal
    return {
        "payloadUrl": None,
        "schemaId": F1_SCHEMA_ID,
        "time": fmt_iso_time(idx),
        "userId": F1_USER_ID,
        "userMessageId": f"umsg-f1-{idx:03d}",
        "vehicleId": vehicle,
        "useCaseIds": [F1_USE_CASE],
        "vehicleProperties": {},
        "payload": {
            "containerMsgCount": num_signals,
            "containerMsgId": container_msg_id,
            "orderVersion": 1,
            "timestamp": base_ts + ts_offset,
            "correlationId": "",
            "name": name,
            "unit": unit,
            "orderId": f"order-f1-{idx:03d}",
            "mdc_id": mdc_id,
            "g_id": g_id,
            "value": value,
            "ingest_timestamp": base_ts,
            "containerId": f"cont-f1-{idx:03d}",
        },
    }


def gen_f1() -> Iterator[dict]:
    for vehicle, idx, signals in F1_RECORDS:
        if not signals:
            continue
        base_ts = F1_BASE_TIME_MS + (idx - 1) * 1000
        for sig in signals:
            yield make_flat_record(vehicle, idx, sig, len(signals), base_ts)


def parse_custom_records(spec: str, user: str, time_iso: str,
                         base_ts_ms: int) -> Iterator[dict]:
    for idx, record_str in enumerate(spec.split(";"), start=1):
        if not record_str.strip():
            continue
        vehicle, _, signals_str = record_str.partition("+")
        if not vehicle:
            raise ValueError(f"missing vehicle in: {record_str!r}")
        signals = []
        for sig_idx, sig_str in enumerate(signals_str.split(","), start=1):
            if not sig_str.strip():
                continue
            mdc_str, _, rest = sig_str.partition(":")
            name, _, val_unit = rest.partition(":")
            value, _, unit = val_unit.partition("/")
            signals.append((1, int(mdc_str), name, value, unit, sig_idx,
                            (sig_idx - 1) * 100))
        base_ts = base_ts_ms + (idx - 1) * 1000
        for sig in signals:
            yield make_flat_record(vehicle, idx, sig, len(signals), base_ts)


def emit(msg: dict) -> None:
    sys.stdout.write(json.dumps(msg, separators=(",", ":")) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixture", choices=["F1"], help="emit a known fixture set")
    ap.add_argument("--custom", action="store_true", help="custom mode (use --records)")
    ap.add_argument("--records", help="custom records spec (see --help)")
    ap.add_argument("--user", default="user-custom")
    ap.add_argument("--time", default="2026-04-30T09:00:00Z")
    ap.add_argument("--base-ts", type=int, default=F1_BASE_TIME_MS)
    args = ap.parse_args()

    if args.fixture == "F1":
        for rec in gen_f1():
            emit(rec)
    elif args.custom:
        if not args.records:
            ap.error("--custom requires --records")
        for rec in parse_custom_records(args.records, args.user, args.time,
                                        args.base_ts):
            emit(rec)
    else:
        ap.error("specify --fixture F1 or --custom --records '...'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
