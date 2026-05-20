#!/usr/bin/env python3
"""Generate subscription messages for the kafka-to-sql-filter variant.

Output: one JSON object per line on stdout (JSONL).

Modes:
  single       — emit exactly one subscription with given vehicle / correlation / mdc list
  update       — alias for single (semantically: a follow-up subscription with new mdc list)
  unsubscribe  — emit one subscription with empty dataIdList
  batch        — emit N random subscriptions (deterministic seed for reproducibility)

Examples (matches CONTRACT §5):

  $ generate-subscription-msg.py --mode single \\
      --vehicle vehicle-fixture-001 --correlation f1-corr-0001 --mdc 100,200
  {"vehicleId":"vehicle-fixture-001","correlationId":"f1-corr-0001","dataIdList":["100","200"]}

  $ generate-subscription-msg.py --mode unsubscribe --vehicle V1 --correlation c2
  {"vehicleId":"V1","correlationId":"c2","dataIdList":[]}

  $ generate-subscription-msg.py --mode batch --count 5 --seed 42
  (5 deterministic random subscriptions)

Pass criterion (Phase 06.1): single mode with F1 inputs reproduces
test-data/fixtures/F1-subscription.json byte-for-byte (after newline strip).
"""
import argparse
import json
import random
import sys
import uuid
from typing import Iterator


VEHICLE_POOL = [
    "vehicle-fixture-001",
    "vehicle-fixture-002",
    "vehicle-fixture-003",
    "vehicle-fixture-004",
    "vehicle-fixture-005",
]

MDC_POOL = [100, 200, 300, 400, 500, 999]


def emit(msg: dict) -> None:
    sys.stdout.write(json.dumps(msg, separators=(",", ":")) + "\n")


def build(vehicle: str, correlation: str, mdc_csv: str) -> dict:
    if mdc_csv:
        data_id_list = [s.strip() for s in mdc_csv.split(",") if s.strip()]
    else:
        data_id_list = []
    return {
        "vehicleId": vehicle,
        "correlationId": correlation,
        "dataIdList": data_id_list,
    }


def random_subscriptions(count: int, seed: int) -> Iterator[dict]:
    rng = random.Random(seed)
    for _ in range(count):
        vehicle = rng.choice(VEHICLE_POOL)
        correlation = str(uuid.UUID(int=rng.getrandbits(128)))
        n_mdc = rng.randint(1, 4)
        mdc_csv = ",".join(str(m) for m in rng.sample(MDC_POOL, n_mdc))
        yield build(vehicle, correlation, mdc_csv)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["single", "update", "unsubscribe", "batch"], required=True)
    ap.add_argument("--vehicle", help="vehicleId (required for single/update/unsubscribe)")
    ap.add_argument("--correlation", help="correlationId (required for single/update/unsubscribe)")
    ap.add_argument("--mdc", help="comma-separated mdc_ids (required for single/update; ignored for unsubscribe)")
    ap.add_argument("--count", type=int, default=10, help="number of records for batch mode")
    ap.add_argument("--seed", type=int, default=0, help="RNG seed for batch mode")
    args = ap.parse_args()

    if args.mode in ("single", "update"):
        if not (args.vehicle and args.correlation and args.mdc):
            ap.error(f"--mode {args.mode} requires --vehicle, --correlation, --mdc")
        emit(build(args.vehicle, args.correlation, args.mdc))
    elif args.mode == "unsubscribe":
        if not (args.vehicle and args.correlation):
            ap.error("--mode unsubscribe requires --vehicle, --correlation")
        emit(build(args.vehicle, args.correlation, ""))
    elif args.mode == "batch":
        for sub in random_subscriptions(args.count, args.seed):
            emit(sub)
    return 0


if __name__ == "__main__":
    sys.exit(main())
