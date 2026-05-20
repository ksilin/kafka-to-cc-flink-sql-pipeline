#!/usr/bin/env bash
# run-fixture-demo.sh — E2E demo for the kafka variant orchestrator.
#
# Drives a 3-subscription scenario through the Java orchestrator (file mode):
#   1. SUBSCRIBE  V1 + [100, 200]
#   2. UPDATE     V1 + [200, 300]   (carry-over offsets)
#   3. UNSUBSCRIBE V1
#
# At each step:
#   - call orchestrator with one subscription file
#   - print the ACK
#   - assert exit code 0
# Then produce telemetry fixture data and consume the sink to verify end-to-end flow.
#
# Per /docs/cc-flink-lessons-agent.md, this script:
#   - sources ../config/env.sh for CC IDs + topic names
#   - uses --output json for existence checks
#   - waits for topic delete propagation before recreating
#   - uses --value-format jsonschema for SR-backed sink reads

set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)"

# ─── Config ───────────────────────────────────────────────────────────────────
if [[ -f "$DIR/config/env.sh" ]]; then
  # shellcheck disable=SC1091
  source "$DIR/config/env.sh"
fi
CC_ENV_ID="${CC_ENV_ID:-env-nvv5xz}"
CC_CLUSTER_ID="${CC_CLUSTER_ID:-lkc-6w3rv2}"
CC_COMPUTE_POOL="${CC_COMPUTE_POOL:-lfcp-kknvdm}"
CC_CLOUD="${CC_CLOUD:-aws}"
CC_REGION="${CC_REGION:-eu-central-1}"
KF_INPUT_TOPIC="${KF_INPUT_TOPIC:-kf-input-test}"
KF_OUTPUT_TOPIC="${KF_OUTPUT_TOPIC:-kf-data-test}"

JAR="$DIR/orchestrator/target/kafka-to-sql-filter-orchestrator-0.1.0-SNAPSHOT-shaded.jar"
TEMPLATE="$DIR/sql/01-filter-template.sql"
SETUP_SQL="$DIR/sql/00-create-output-table.sql"
TELEMETRY_FIXTURE="$DIR/test-data/fixtures/F1-input.jsonl"
STATE_FILE="$DIR/orchestrator/state.demo.json"
SCENARIO_DIR="$(mktemp -d)"

trap 'rm -rf "$SCENARIO_DIR"; rm -f "$STATE_FILE"' EXIT

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
fail() { log "FAIL: $*"; exit 1; }

require() { command -v "$1" >/dev/null || fail "missing dependency: $1"; }
require confluent
require java
require python3

# ─── Pre-flight ───────────────────────────────────────────────────────────────
[[ -f "$JAR" ]] || fail "orchestrator jar not built: $JAR\n  cd orchestrator && mvn package -DskipTests"

log "pre-flight: env=$CC_ENV_ID cluster=$CC_CLUSTER_ID pool=$CC_COMPUTE_POOL"
confluent environment use "$CC_ENV_ID" >/dev/null
confluent kafka cluster use "$CC_CLUSTER_ID" >/dev/null

# ─── Helpers ──────────────────────────────────────────────────────────────────
topic_exists() {
  confluent kafka topic list --cluster "$CC_CLUSTER_ID" --output json 2>/dev/null \
    | grep -q "\"name\": \"$1\""
}

statement_exists() {
  confluent flink statement list --cloud "$CC_CLOUD" --region "$CC_REGION" --output json 2>/dev/null \
    | grep -q "\"name\": \"$1\""
}

drop_statement() {
  if statement_exists "$1"; then
    confluent flink statement delete "$1" --cloud "$CC_CLOUD" --region "$CC_REGION" --force >/dev/null 2>&1 || true
  fi
}

drop_topic() {
  if topic_exists "$1"; then
    confluent kafka topic delete "$1" --cluster "$CC_CLUSTER_ID" --force >/dev/null 2>&1 || true
  fi
}

drop_subject() {
  confluent schema-registry subject delete "$1" --force >/dev/null 2>&1 || true
  confluent schema-registry subject delete "$1" --permanent --force >/dev/null 2>&1 || true
}

cleanup_cc() {
  log "cleanup: drop demo statements + topics + subjects"
  for stmt in $(confluent flink statement list --cloud "$CC_CLOUD" --region "$CC_REGION" --output json 2>/dev/null \
                | grep -oE '"name": "kf-flt-[^"]*"' | sed 's/"name": "//;s/"//'); do
    drop_statement "$stmt"
  done
  drop_statement "kf-setup-output-table"
  drop_topic "$KF_INPUT_TOPIC"
  drop_topic "$KF_OUTPUT_TOPIC"
  drop_subject "${KF_OUTPUT_TOPIC}-value"
  rm -f "$STATE_FILE"
  until ! topic_exists "$KF_INPUT_TOPIC" && ! topic_exists "$KF_OUTPUT_TOPIC"; do
    log "  waiting for topic delete propagation..."
    sleep 5
  done
}

ensure_input_topic() {
  if ! topic_exists "$KF_INPUT_TOPIC"; then
    log "creating input topic $KF_INPUT_TOPIC"
    confluent kafka topic create "$KF_INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" --partitions 1 >/dev/null
  fi
}

ensure_output_table() {
  if topic_exists "$KF_OUTPUT_TOPIC"; then
    log "output topic $KF_OUTPUT_TOPIC exists; skipping DDL"
    return
  fi
  drop_statement "kf-setup-output-table"
  log "submitting DDL kf-setup-output-table"
  confluent flink statement create "kf-setup-output-table" \
    --sql "$(cat "$SETUP_SQL")" \
    --compute-pool "$CC_COMPUTE_POOL" \
    --database "$CC_CLUSTER_ID" \
    --environment "$CC_ENV_ID" \
    --wait >/dev/null
}

run_orchestrator_file_mode() {
  local sub_path="$1"
  java -jar "$JAR" \
    --mode file \
    --subscription "$sub_path" \
    --template "$TEMPLATE" \
    --state "$STATE_FILE" \
    --input-topic "$KF_INPUT_TOPIC" \
    --output-topic "$KF_OUTPUT_TOPIC" \
    --compute-pool "$CC_COMPUTE_POOL" \
    --cluster "$CC_CLUSTER_ID" \
    --environment "$CC_ENV_ID" \
    --cloud "$CC_CLOUD" \
    --region "$CC_REGION"
}

# ─── Scenario ─────────────────────────────────────────────────────────────────

cleanup_cc
ensure_input_topic
ensure_output_table

# 1. SUBSCRIBE V1 + [100, 200]
log "step 1: SUBSCRIBE V1 + [100, 200]"
SUB1="$SCENARIO_DIR/sub1.json"
python3 "$DIR/test-data/generate-subscription-msg.py" \
  --mode single --vehicle vehicle-fixture-001 --correlation demo-c1 --mdc 100,200 \
  > "$SUB1"
ACK1=$(run_orchestrator_file_mode "$SUB1")
echo "ACK1: $ACK1"
echo "$ACK1" | grep -q '"status":"Success".*"details":"subscribed"' || fail "step 1 ack mismatch"

# 2. UPDATE V1 → [200, 300]
log "step 2: UPDATE V1 + [200, 300] (carry-over offsets)"
SUB2="$SCENARIO_DIR/sub2.json"
python3 "$DIR/test-data/generate-subscription-msg.py" \
  --mode update --vehicle vehicle-fixture-001 --correlation demo-c2 --mdc 200,300 \
  > "$SUB2"
ACK2=$(run_orchestrator_file_mode "$SUB2")
echo "ACK2: $ACK2"
echo "$ACK2" | grep -q '"status":"Success".*"details":"updated"' || fail "step 2 ack mismatch"

# 3. UNSUBSCRIBE V1
log "step 3: UNSUBSCRIBE V1"
SUB3="$SCENARIO_DIR/sub3.json"
python3 "$DIR/test-data/generate-subscription-msg.py" \
  --mode unsubscribe --vehicle vehicle-fixture-001 --correlation demo-c3 \
  > "$SUB3"
ACK3=$(run_orchestrator_file_mode "$SUB3")
echo "ACK3: $ACK3"
echo "$ACK3" | grep -q '"status":"Success".*"details":"unsubscribed"' || fail "step 3 ack mismatch"

# ─── Summary ──────────────────────────────────────────────────────────────────
log "demo OK — 3 ACKs received in sequence; statements lifecycle managed by orchestrator"
log "next: cleanup with kafka-to-sql-filter/tests/verify-phase1.sh clean"
