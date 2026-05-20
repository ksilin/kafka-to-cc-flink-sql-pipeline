#!/usr/bin/env python3
"""Canonical multiset diff between two JSONL files.

Each line is parsed as JSON, re-serialized with sort_keys=True, and the two
multisets are compared. Differences are printed with [+]/[-] markers.
Exit 0 if equal, 1 otherwise.

Usage:
    diff_jsonl.py expected.jsonl actual.jsonl
"""
import json
import sys
from collections import Counter


def canon(line: str) -> str:
    return json.dumps(json.loads(line), sort_keys=True, separators=(",", ":"))


def load(path: str) -> Counter:
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.rstrip("\n") for ln in f if ln.strip()]
    return Counter(canon(ln) for ln in lines)


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: diff_jsonl.py <expected.jsonl> <actual.jsonl>", file=sys.stderr)
        return 2
    expected_path, actual_path = sys.argv[1], sys.argv[2]
    expected = load(expected_path)
    actual = load(actual_path)
    if expected == actual:
        print(f"OK: {expected.total()} records match")
        return 0
    missing = expected - actual
    extra = actual - expected
    print(f"DIFF: expected={expected.total()} actual={actual.total()}")
    for line, count in sorted(missing.items()):
        for _ in range(count):
            print(f"[-] {line}")
    for line, count in sorted(extra.items()):
        for _ in range(count):
            print(f"[+] {line}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
