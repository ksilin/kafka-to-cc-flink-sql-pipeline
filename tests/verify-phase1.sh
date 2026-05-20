#!/usr/bin/env bash
# verify-phase1.sh — Drives the F1 fixture through real Confluent Cloud Flink and diffs.
#
# Per /CLAUDE.md: "no mocks for the SQL path." This script makes real CC writes:
# creates input topic, runs CREATE TABLE DDL (which creates the sink topic +
# registers JSON Schema), submits the filter Flink statement, produces messages,
# consumes from the sink, and compares against the fixture.
#
# Idempotent goal: re-running drops and recreates topics + statements.
#
# Configuration:
#   Source kafka-to-sql-filter/config/env.sh first (copy from env.example.sh).
#   The harness falls back to defaults from env.example.sh if vars are unset.
#
# Usage:
#   source ../config/env.sh
#   ./verify-phase1.sh red    # expect failure (no statement) — validates harness reports DIFF
#   ./verify-phase1.sh green  # full run with filter statement — expect OK
#   ./verify-phase1.sh empty  # no filter statement — expect 0 records
#   ./verify-phase1.sh update # carry-over offsets test (CC sql.tables.initial-offset-from)
#   ./verify-phase1.sh clean  # drop topics + statements + schema subjects, exit
set -euo pipefail

PHASE="${1:-green}"

# ─── Locate dirs and source env if present ────────────────────────────────────
DIR="$(cd "$(dirname "$0")/.." && pwd)"
if [[ -f "$DIR/config/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "$DIR/config/env.sh"
fi

# ─── Defaults (mirror config/env.example.sh) ──────────────────────────────────
CC_ENV_ID="${CC_ENV_ID:-env-nvv5xz}"
CC_CLUSTER_ID="${CC_CLUSTER_ID:-lkc-6w3rv2}"
CC_COMPUTE_POOL="${CC_COMPUTE_POOL:-lfcp-kknvdm}"
CC_CLOUD="${CC_CLOUD:-aws}"
CC_REGION="${CC_REGION:-eu-central-1}"

KF_INPUT_TOPIC="${KF_INPUT_TOPIC:-kf-input-test}"
KF_OUTPUT_TOPIC="${KF_OUTPUT_TOPIC:-kf-data-test}"
KF_SETUP_STATEMENT_NAME="${KF_SETUP_STATEMENT_NAME:-kf-setup-output-table}"
KF_F1_STATEMENT_PREFIX="${KF_F1_STATEMENT_PREFIX:-kafka-variant-f1}"

SETUP_SQL="$DIR/sql/00-create-output-table.sql"
FILTER_SQL="$DIR/sql/01-filter-F1.sql"
INPUT_FIXTURE="$DIR/test-data/fixtures/F1-input.jsonl"
EXPECTED_FIXTURE="$DIR/test-data/fixtures/F1-expected.jsonl"

ACTUAL_FILE="$(mktemp)"
trap 'rm -f "$ACTUAL_FILE"' EXIT

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

require() {
  command -v "$1" >/dev/null || { log "missing dependency: $1"; exit 2; }
}

require confluent
require python3

# ─── CC wrappers ──────────────────────────────────────────────────────────────
# `create` uses environment context (set via `confluent environment use`).
# `list` / `delete` / `describe` require --cloud and --region.
cc_flink_op() {
  confluent flink statement "$@" \
    --cloud "$CC_CLOUD" --region "$CC_REGION"
}

flink_create() {
  # Args: <statement-name> <sql-file> [<extra-property=value> ...]
  # CLI takes --sql (string), not --sql-file. Read the file contents.
  # CLI takes --property as comma-separated key=val pairs in one flag.
  local name="$1" sql_path="$2"
  shift 2
  local sql_content
  sql_content="$(cat "$sql_path")"
  local property_args=()
  if [[ $# -gt 0 ]]; then
    local joined
    joined="$(IFS=','; echo "$*")"
    property_args=( --property "$joined" )
  fi
  confluent flink statement create "$name" \
    --sql "$sql_content" \
    --compute-pool "$CC_COMPUTE_POOL" \
    --database "$CC_CLUSTER_ID" \
    --environment "$CC_ENV_ID" \
    "${property_args[@]}" \
    --wait
}

# Existence checks via JSON output (CLI tables have leading whitespace + table chars)
statement_exists() {
  cc_flink_op list --output json 2>/dev/null \
    | grep -q "\"name\": \"$1\""
}

topic_exists() {
  confluent kafka topic list --cluster "$CC_CLUSTER_ID" --output json 2>/dev/null \
    | grep -q "\"name\": \"$1\""
}

subject_exists() {
  # subject list does not support --output json reliably; match left-trimmed line
  confluent schema-registry subject list 2>/dev/null \
    | awk '{$1=$1};1' \
    | grep -qx "$1"
}

drop_statement() {
  local name="$1"
  if statement_exists "$name"; then
    log "deleting statement $name"
    cc_flink_op delete "$name" --force >/dev/null 2>&1 || true
  fi
}

drop_topic() {
  local t="$1"
  if topic_exists "$t"; then
    log "deleting topic $t"
    confluent kafka topic delete "$t" --cluster "$CC_CLUSTER_ID" --force >/dev/null 2>&1 || true
  fi
}

drop_subject() {
  local s="$1"
  if subject_exists "$s"; then
    log "deleting schema subject $s (hard)"
    confluent schema-registry subject delete "$s" --force >/dev/null 2>&1 || true
    confluent schema-registry subject delete "$s" --permanent --force >/dev/null 2>&1 || true
  fi
}

cleanup() {
  drop_statement "${KF_F1_STATEMENT_PREFIX}-red"
  drop_statement "${KF_F1_STATEMENT_PREFIX}-green"
  drop_statement "${KF_F1_STATEMENT_PREFIX}-empty"
  drop_statement "${KF_F1_STATEMENT_PREFIX}-update-v1"
  drop_statement "${KF_F1_STATEMENT_PREFIX}-update-v2"
  drop_statement "$KF_SETUP_STATEMENT_NAME"
  drop_topic "$KF_INPUT_TOPIC"
  drop_topic "$KF_OUTPUT_TOPIC"
  drop_subject "${KF_OUTPUT_TOPIC}-value"
}

# ─── Subroutines ──────────────────────────────────────────────────────────────
ensure_input_topic() {
  if ! topic_exists "$KF_INPUT_TOPIC"; then
    log "creating input topic $KF_INPUT_TOPIC (raw bytes, no schema)"
    confluent kafka topic create "$KF_INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" --partitions 1 >/dev/null
  fi
}

ensure_output_table() {
  # CREATE TABLE creates topic + registers JSON Schema in one DDL.
  # Decide on TOPIC existence, not statement — CC list-statements is eventually
  # consistent, so deleted statements linger and would falsely skip DDL.
  if topic_exists "$KF_OUTPUT_TOPIC"; then
    log "output topic $KF_OUTPUT_TOPIC already exists; reusing topic + schema"
    return
  fi
  # Drop a stale completed-DDL statement if present (CC rejects duplicate names).
  drop_statement "$KF_SETUP_STATEMENT_NAME"
  log "submitting DDL $KF_SETUP_STATEMENT_NAME (creates $KF_OUTPUT_TOPIC + json-registry schema)"
  flink_create "$KF_SETUP_STATEMENT_NAME" "$SETUP_SQL"
}

# ─── Phase dispatch ───────────────────────────────────────────────────────────
if [[ "$PHASE" == "clean" ]]; then
  cleanup
  log "cleanup complete"
  exit 0
fi

# Auto-login if session expired (F6: non-interactive login via env vars)
if ! confluent organization list >/dev/null 2>&1; then
  if [[ -n "${CONFLUENT_CLOUD_EMAIL:-}" && -n "${CONFLUENT_CLOUD_PASSWORD:-}" ]]; then
    log "CLI session expired — re-authenticating via CONFLUENT_CLOUD_EMAIL"
    confluent login --save >/dev/null 2>&1
  else
    log "FATAL: CLI session expired. Either:"
    log "  1. Set CONFLUENT_CLOUD_EMAIL + CONFLUENT_CLOUD_PASSWORD env vars (auto-re-login)"
    log "  2. Run: confluent login --save"
    exit 2
  fi
fi

log "pre-flight: env=$CC_ENV_ID cluster=$CC_CLUSTER_ID pool=$CC_COMPUTE_POOL"
confluent environment use "$CC_ENV_ID" >/dev/null
confluent kafka cluster use "$CC_CLUSTER_ID" >/dev/null

cleanup
ensure_input_topic
ensure_output_table

case "$PHASE" in
  red)
    log "RED: skipping filter statement (expected: diff fails, sink empty)"
    log "producing $INPUT_FIXTURE -> $KF_INPUT_TOPIC"
    confluent kafka topic produce "$KF_INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" < "$INPUT_FIXTURE"
    log "consuming $KF_OUTPUT_TOPIC (timeout 15s)"
    timeout 15 confluent kafka topic consume "$KF_OUTPUT_TOPIC" \
      --cluster "$CC_CLUSTER_ID" --from-beginning --print-key=false --value-format jsonschema 2>/dev/null | grep -v '^%' \
      > "$ACTUAL_FILE" 2>/dev/null || true
    log "RED actual lines: $(wc -l < "$ACTUAL_FILE")"
    if python3 "$DIR/tests/diff_jsonl.py" "$EXPECTED_FIXTURE" "$ACTUAL_FILE"; then
      log "RED unexpectedly OK — diff matched without filter statement; aborting"
      exit 1
    else
      log "RED confirmed: diff fails as expected"
      exit 0
    fi
    ;;
  green)
    NAME="${KF_F1_STATEMENT_PREFIX}-green"
    log "GREEN: submitting filter statement $NAME"
    flink_create "$NAME" "$FILTER_SQL"
    log "producing $INPUT_FIXTURE -> $KF_INPUT_TOPIC"
    confluent kafka topic produce "$KF_INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" < "$INPUT_FIXTURE"
    EXPECTED_COUNT=$(wc -l < "$EXPECTED_FIXTURE")
    log "consuming $KF_OUTPUT_TOPIC (waiting up to 60s for $EXPECTED_COUNT records)"
    timeout 60 confluent kafka topic consume "$KF_OUTPUT_TOPIC" \
      --cluster "$CC_CLUSTER_ID" --from-beginning --print-key=false --value-format jsonschema 2>/dev/null | grep -v '^%' \
      | head -n "$EXPECTED_COUNT" > "$ACTUAL_FILE" || true
    log "GREEN actual lines: $(wc -l < "$ACTUAL_FILE")"
    python3 "$DIR/tests/diff_jsonl.py" "$EXPECTED_FIXTURE" "$ACTUAL_FILE"
    ;;
  empty)
    log "EMPTY: per CONTRACT §2, empty dataIdList = unsubscribe = no filter statement = no output"
    confluent kafka topic produce "$KF_INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" < "$INPUT_FIXTURE"
    timeout 15 confluent kafka topic consume "$KF_OUTPUT_TOPIC" \
      --cluster "$CC_CLUSTER_ID" --from-beginning --print-key=false --value-format jsonschema 2>/dev/null | grep -v '^%' \
      > "$ACTUAL_FILE" 2>/dev/null || true
    n=$(wc -l < "$ACTUAL_FILE")
    log "EMPTY actual lines: $n"
    [[ "$n" -eq 0 ]] || { log "EMPTY failed: expected 0 records, got $n"; exit 1; }
    log "EMPTY OK"
    ;;
  update)
    # Carry-over offsets test. Requires sql/01-filter-F1-update.sql (different mdc_id list).
    UPDATE_SQL="$DIR/sql/01-filter-F1-update.sql"
    if [[ ! -f "$UPDATE_SQL" ]]; then
      log "UPDATE: missing $UPDATE_SQL — write the v2 filter SQL first; skipping"
      exit 0
    fi
    V1="${KF_F1_STATEMENT_PREFIX}-update-v1"
    V2="${KF_F1_STATEMENT_PREFIX}-update-v2"
    UPDATE_EXPECTED="$DIR/test-data/fixtures/F1-update-expected.jsonl"

    log "UPDATE step 1: submit v1 ($V1, predicate mdc IN (100, 200))"
    flink_create "$V1" "$FILTER_SQL"

    log "UPDATE step 2: produce batch1 -> v1 should emit 9 records"
    confluent kafka topic produce "$KF_INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" < "$INPUT_FIXTURE"

    log "UPDATE step 3: wait for sink to receive 9 records from v1"
    EXPECTED_V1=$(wc -l < "$EXPECTED_FIXTURE")
    timeout 90 confluent kafka topic consume "$KF_OUTPUT_TOPIC" \
      --cluster "$CC_CLUSTER_ID" --from-beginning --print-key=false --value-format jsonschema 2>/dev/null | grep -v '^%' \
      | head -n "$EXPECTED_V1" > "$ACTUAL_FILE" || true
    log "UPDATE: v1 emitted $(wc -l < "$ACTUAL_FILE") records (expected $EXPECTED_V1)"
    if ! python3 "$DIR/tests/diff_jsonl.py" "$EXPECTED_FIXTURE" "$ACTUAL_FILE"; then
      log "UPDATE FAILED: v1 batch1 diff"
      exit 1
    fi

    log "UPDATE step 4: submit v2 ($V2) with carry-over from $V1, NO --wait (PENDING is expected)"
    SQL_CONTENT="$(cat "$UPDATE_SQL")"
    confluent flink statement create "$V2" \
      --sql "$SQL_CONTENT" \
      --compute-pool "$CC_COMPUTE_POOL" \
      --database "$CC_CLUSTER_ID" \
      --environment "$CC_ENV_ID" \
      --property "sql.tables.initial-offset-from=$V1"
    log "UPDATE step 5: v2 submitted; expect status=PENDING because v1 still running"

    log "UPDATE step 6: stop v1 (must be STOP, not DELETE — CC requires referenced statement to be STOPPED for carry-over)"
    confluent flink statement stop "$V1" \
      --cloud "$CC_CLOUD" --region "$CC_REGION" >/dev/null 2>&1 || true

    log "UPDATE step 7: poll v2 until RUNNING (timeout 5min)"
    for i in $(seq 1 60); do
      STATUS=$(cc_flink_op describe "$V2" --output json 2>/dev/null \
        | grep -E '"status":[[:space:]]*"' | head -1 \
        | sed -E 's/.*"status":[[:space:]]*"([A-Z]+)".*/\1/')
      log "UPDATE: v2 status=$STATUS (poll $i/60)"
      [[ "$STATUS" == "RUNNING" ]] && break
      [[ "$STATUS" == "FAILED" ]] && { log "UPDATE FAILED: v2 entered FAILED"; exit 1; }
      sleep 5
    done
    [[ "$STATUS" == "RUNNING" ]] || { log "UPDATE FAILED: v2 not RUNNING after 5min, last status=$STATUS"; exit 1; }

    log "UPDATE step 8: produce batch2 -> v2 picks up at v1's last offset, emits 6 records"
    confluent kafka topic produce "$KF_INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" < "$INPUT_FIXTURE"

    log "UPDATE step 9: consume sink, expect 9 (v1 batch1) + 6 (v2 batch2) = 15 total"
    TOTAL_EXPECTED=$(($(wc -l < "$EXPECTED_FIXTURE") + $(wc -l < "$UPDATE_EXPECTED")))
    timeout 90 confluent kafka topic consume "$KF_OUTPUT_TOPIC" \
      --cluster "$CC_CLUSTER_ID" --from-beginning --print-key=false --value-format jsonschema 2>/dev/null | grep -v '^%' \
      | head -n "$TOTAL_EXPECTED" > "$ACTUAL_FILE" || true
    log "UPDATE: total $(wc -l < "$ACTUAL_FILE") records (expected $TOTAL_EXPECTED)"

    log "UPDATE step 10: split + diff each side"
    cat "$EXPECTED_FIXTURE" "$UPDATE_EXPECTED" > "$ACTUAL_FILE.combined-expected"
    python3 "$DIR/tests/diff_jsonl.py" "$ACTUAL_FILE.combined-expected" "$ACTUAL_FILE"
    rm -f "$ACTUAL_FILE.combined-expected"
    ;;
  *)
    log "unknown phase: $PHASE"
    exit 2
    ;;
esac
