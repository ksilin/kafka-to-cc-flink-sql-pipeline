#!/usr/bin/env bash
# run-fixture-demo.sh — E2E demo for the kafka variant orchestrator.
#
# Drives a 3-subscription scenario through the Java orchestrator (kafka mode):
#   1. SUBSCRIBE  V1 + [100, 200]
#   2. UPDATE     V1 + [200, 300]   (carry-over offsets)
#   3. UNSUBSCRIBE V1
#
# At each step:
#   - produce subscription JSON to kf-sub-test
#   - run orchestrator in kafka mode (polls topic, writes ACK to kf-ack-test)
#   - consume and verify ACK from kf-ack-test
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
KF_SUBSCRIPTION_TOPIC="${KF_SUBSCRIPTION_TOPIC:-kf-sub-test}"
KF_ACK_TOPIC="${KF_ACK_TOPIC:-kf-ack-test}"

JAR="$DIR/orchestrator/target/kafka-to-sql-filter-orchestrator-0.1.0-SNAPSHOT-shaded.jar"
TEMPLATE="$DIR/sql/01-filter-template.sql"
SETUP_SQL="$DIR/sql/00-create-output-table.sql"
TELEMETRY_FIXTURE="$DIR/test-data/fixtures/F1-input.jsonl"
STATE_FILE="$DIR/orchestrator/state.demo.json"
KAFKA_PROPS="$DIR/config/ccloud.properties"

trap 'rm -f "$STATE_FILE"' EXIT

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
fail() { log "FAIL: $*"; exit 1; }

require() { command -v "$1" >/dev/null || fail "missing dependency: $1"; }
require confluent
require java
require python3

# ─── Pre-flight ───────────────────────────────────────────────────────────────
[[ -f "$JAR" ]] || fail "orchestrator jar not built: $JAR\n  cd orchestrator && mvn package -DskipTests"
[[ -f "$KAFKA_PROPS" ]] || fail "kafka properties not found: $KAFKA_PROPS\n  see config/README.md"

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
  drop_topic "$KF_SUBSCRIPTION_TOPIC"
  drop_topic "$KF_ACK_TOPIC"
  drop_subject "${KF_OUTPUT_TOPIC}-value"
  rm -f "$STATE_FILE"
  until ! topic_exists "$KF_INPUT_TOPIC" && ! topic_exists "$KF_OUTPUT_TOPIC"; do
    log "  waiting for topic delete propagation..."
    sleep 5
  done
}

ensure_topic() {
  if ! topic_exists "$1"; then
    log "creating topic $1"
    confluent kafka topic create "$1" --cluster "$CC_CLUSTER_ID" --partitions 1 >/dev/null
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

produce_subscription() {
  local sub_json="$1"
  echo "$sub_json" | confluent kafka topic produce "$KF_SUBSCRIPTION_TOPIC" \
    --cluster "$CC_CLUSTER_ID" >/dev/null 2>&1
}

run_orchestrator_kafka_mode() {
  # Orchestrator prints ACK JSON to stderr as: [orchestrator] {"correlationId":...}
  # Capture stderr separately so we can extract ACK for validation.
  local stderr_file
  stderr_file="$(mktemp)"
  java -jar "$JAR" \
    --mode kafka \
    --kafka-props "$KAFKA_PROPS" \
    --subscription-topic "$KF_SUBSCRIPTION_TOPIC" \
    --ack-topic "$KF_ACK_TOPIC" \
    --group-id "fixture-demo-$$" \
    --max-messages 1 \
    --max-poll-empty 6 \
    --template "$TEMPLATE" \
    --state "$STATE_FILE" \
    --input-topic "$KF_INPUT_TOPIC" \
    --output-topic "$KF_OUTPUT_TOPIC" \
    --compute-pool "$CC_COMPUTE_POOL" \
    --cluster "$CC_CLUSTER_ID" \
    --environment "$CC_ENV_ID" \
    --cloud "$CC_CLOUD" \
    --region "$CC_REGION" 2>"$stderr_file"
  local rc=$?
  cat "$stderr_file" >&2
  # Extract ACK JSON from [orchestrator] prefix line
  grep '\[orchestrator\]' "$stderr_file" | sed 's/\[orchestrator\] //' | head -n 1
  rm -f "$stderr_file"
  return $rc
}

consume_all_acks() {
  local expected_count="$1"
  # confluent consume is a streaming command; head -n closes the pipe early,
  # causing SIGPIPE / timeout exit — tolerate with || true.
  timeout 30 confluent kafka topic consume "$KF_ACK_TOPIC" \
    --cluster "$CC_CLUSTER_ID" --from-beginning --print-key=false \
    --value-format string 2>/dev/null \
    | grep --line-buffered -v '^%' \
    | grep --line-buffered -v '^Starting' \
    | head -n "$expected_count" || true
}

# ─── Scenario ─────────────────────────────────────────────────────────────────

cleanup_cc
ensure_topic "$KF_INPUT_TOPIC"
ensure_topic "$KF_SUBSCRIPTION_TOPIC"
ensure_topic "$KF_ACK_TOPIC"
ensure_output_table

# 1. SUBSCRIBE V1 + [100, 200]
log "step 1: SUBSCRIBE V1 + [100, 200]"
SUB1_JSON=$(python3 "$DIR/test-data/generate-subscription-msg.py" \
  --mode single --vehicle vehicle-fixture-001 --correlation demo-c1 --mdc 100,200)
log "  producing subscription to $KF_SUBSCRIPTION_TOPIC"
produce_subscription "$SUB1_JSON"
log "  running orchestrator (kafka mode)..."
ACK1=$(run_orchestrator_kafka_mode)
echo "ACK1: $ACK1"
echo "$ACK1" | grep -q '"status":"Success".*"details":"subscribed"' || fail "step 1 ack mismatch"

# 1b. Produce telemetry data and verify output on sink topic
log "step 1b: producing telemetry fixture data to $KF_INPUT_TOPIC"
confluent kafka topic produce "$KF_INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" < "$TELEMETRY_FIXTURE" 2>/dev/null
log "  waiting for Flink to process (20s)..."
sleep 20

log "step 1c: verifying output on $KF_OUTPUT_TOPIC (expect 9 records for mdc 100,200)"
ACTUAL_FILE="$(mktemp)"
timeout 60 confluent kafka topic consume "$KF_OUTPUT_TOPIC" \
  --cluster "$CC_CLUSTER_ID" --from-beginning --print-key=false \
  --value-format jsonschema 2>/dev/null \
  | grep --line-buffered -v '^%' \
  | grep --line-buffered -v '^Starting' \
  | head -n 9 > "$ACTUAL_FILE" || true
RECORD_COUNT=$(wc -l < "$ACTUAL_FILE")
log "  got $RECORD_COUNT records on $KF_OUTPUT_TOPIC"
[[ "$RECORD_COUNT" -eq 9 ]] || fail "expected 9 output records, got $RECORD_COUNT"
rm -f "$ACTUAL_FILE"

# 2. UPDATE V1 → [200, 300]
log "step 2: UPDATE V1 + [200, 300] (carry-over offsets)"
SUB2_JSON=$(python3 "$DIR/test-data/generate-subscription-msg.py" \
  --mode update --vehicle vehicle-fixture-001 --correlation demo-c2 --mdc 200,300)
log "  producing subscription to $KF_SUBSCRIPTION_TOPIC"
produce_subscription "$SUB2_JSON"
log "  running orchestrator (kafka mode)..."
ACK2=$(run_orchestrator_kafka_mode)
echo "ACK2: $ACK2"
echo "$ACK2" | grep -q '"status":"Success".*"details":"updated"' || fail "step 2 ack mismatch"

# 2b. Produce more telemetry data and verify new filter output
log "step 2b: producing more telemetry data to $KF_INPUT_TOPIC"
confluent kafka topic produce "$KF_INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" < "$TELEMETRY_FIXTURE" 2>/dev/null
log "  waiting for Flink to process (20s)..."
sleep 20

log "step 2c: verifying total output on $KF_OUTPUT_TOPIC (expect 9 + 6 = 15 records)"
ACTUAL_FILE="$(mktemp)"
timeout 60 confluent kafka topic consume "$KF_OUTPUT_TOPIC" \
  --cluster "$CC_CLUSTER_ID" --from-beginning --print-key=false \
  --value-format jsonschema 2>/dev/null \
  | grep --line-buffered -v '^%' \
  | grep --line-buffered -v '^Starting' \
  | head -n 15 > "$ACTUAL_FILE" || true
RECORD_COUNT=$(wc -l < "$ACTUAL_FILE")
log "  got $RECORD_COUNT records on $KF_OUTPUT_TOPIC"
[[ "$RECORD_COUNT" -eq 15 ]] || fail "expected 15 output records, got $RECORD_COUNT"
rm -f "$ACTUAL_FILE"

# 3. UNSUBSCRIBE V1
log "step 3: UNSUBSCRIBE V1"
SUB3_JSON=$(python3 "$DIR/test-data/generate-subscription-msg.py" \
  --mode unsubscribe --vehicle vehicle-fixture-001 --correlation demo-c3)
log "  producing subscription to $KF_SUBSCRIPTION_TOPIC"
produce_subscription "$SUB3_JSON"
log "  running orchestrator (kafka mode)..."
ACK3=$(run_orchestrator_kafka_mode)
echo "ACK3: $ACK3"
echo "$ACK3" | grep -q '"status":"Success".*"details":"unsubscribed"' || fail "step 3 ack mismatch"

# 4. Verify all 3 ACKs landed on the ACK topic
log "step 4: verifying all 3 ACKs on $KF_ACK_TOPIC"
ACKS=$(consume_all_acks 3)
ACK_COUNT=$(echo "$ACKS" | wc -l)
[[ "$ACK_COUNT" -eq 3 ]] || fail "expected 3 ACKs on topic, got $ACK_COUNT"
log "  $ACK_COUNT ACKs confirmed on $KF_ACK_TOPIC"

# ─── Summary ──────────────────────────────────────────────────────────────────
log "demo OK — 3 subscriptions via $KF_SUBSCRIPTION_TOPIC, 3 ACKs verified on $KF_ACK_TOPIC"
log "next: cleanup with kafka-to-sql-filter/tests/verify-phase1.sh clean"
