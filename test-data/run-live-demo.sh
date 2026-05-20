#!/usr/bin/env bash
# run-live-demo.sh — Observable live demo with persistent state
#
# Unlike verify-phase1.sh (automated diff, cleans up) and run-fixture-demo.sh
# (file mode, no Kafka topics for sub/ack), this script:
#
# 1. Creates ALL four topics (input, output, subscription, ack)
# 2. Creates the output table DDL
# 3. Produces telemetry fixture data to the input topic (data stays visible)
# 4. Walks you through subscribing, observing output, updating, unsubscribing
# 5. Does NOT clean up — you observe at your pace, clean manually when done
#
# Usage:
#   source config/env.sh
#   ./test-data/run-live-demo.sh setup      # create topics + DDL + produce data
#   ./test-data/run-live-demo.sh subscribe   # subscribe V1 to mdc 100,200
#   ./test-data/run-live-demo.sh observe     # consume output topic (live tail)
#   ./test-data/run-live-demo.sh update      # update V1 to mdc 200,300
#   ./test-data/run-live-demo.sh unsubscribe # stop filtering for V1
#   ./test-data/run-live-demo.sh produce     # produce more data (use after subscribe/update)
#   ./test-data/run-live-demo.sh status      # show all statements + topics
#   ./test-data/run-live-demo.sh clean       # tear down everything
set -euo pipefail

STEP="${1:-help}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Source env
[[ -f "$DIR/config/env.sh" ]] && source "$DIR/config/env.sh"
[[ -f "$DIR/config/cc_user_creds" ]] && source "$DIR/config/cc_user_creds" && \
  export CONFLUENT_CLOUD_EMAIL="${CC_USER:-}" CONFLUENT_CLOUD_PASSWORD="${CC_PW:-}"

CC_ENV_ID="${CC_ENV_ID:-env-nvv5xz}"
CC_CLUSTER_ID="${CC_CLUSTER_ID:-lkc-6w3rv2}"
CC_COMPUTE_POOL="${CC_COMPUTE_POOL:-lfcp-kknvdm}"
CC_CLOUD="${CC_CLOUD:-aws}"
CC_REGION="${CC_REGION:-eu-central-1}"

INPUT_TOPIC="${KF_INPUT_TOPIC:-kf-input-test}"
OUTPUT_TOPIC="${KF_OUTPUT_TOPIC:-kf-data-test}"
SUB_TOPIC="${KF_SUBSCRIPTION_TOPIC:-kf-sub-test}"
ACK_TOPIC="${KF_ACK_TOPIC:-kf-ack-test}"

ORCHESTRATOR_JAR="$DIR/orchestrator/target/kafka-to-sql-filter-orchestrator-0.1.0-SNAPSHOT-shaded.jar"
TEMPLATE="$DIR/sql/01-filter-template.sql"
STATE_FILE="/tmp/live-demo-state.json"
DDL_SQL="$DIR/sql/00-create-output-table.sql"
FIXTURE="$DIR/test-data/fixtures/F1-input.jsonl"

log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

ensure_login() {
  if ! confluent organization list >/dev/null 2>&1; then
    if [[ -n "${CONFLUENT_CLOUD_EMAIL:-}" && -n "${CONFLUENT_CLOUD_PASSWORD:-}" ]]; then
      confluent login --save >/dev/null 2>&1
    else
      echo "ERROR: CLI session expired. Run: confluent login --save"; exit 2
    fi
  fi
  confluent environment use "$CC_ENV_ID" >/dev/null 2>&1
  confluent kafka cluster use "$CC_CLUSTER_ID" >/dev/null 2>&1
}

topic_exists() {
  confluent kafka topic list --cluster "$CC_CLUSTER_ID" --output json 2>/dev/null \
    | grep -q "\"name\": \"$1\""
}

create_topic_if_missing() {
  if ! topic_exists "$1"; then
    log "Creating topic: $1"
    confluent kafka topic create "$1" --cluster "$CC_CLUSTER_ID" --partitions 1
  else
    log "Topic $1 already exists"
  fi
}

# Consume and pretty-print output topic records.
# Usage: consume_pretty [--from-beginning]
consume_pretty() {
  confluent kafka topic consume "$OUTPUT_TOPIC" \
    --cluster "$CC_CLUSTER_ID" \
    --print-key=false \
    --value-format jsonschema \
    "$@" 2>/dev/null \
    | grep --line-buffered -v '^%' \
    | grep --line-buffered -v '^Starting' \
    | python3 -u -c "
import sys, json, signal
signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
n = 0
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try:
        r = json.loads(line)
        n += 1
        mdc = r.get('payload',{}).get('mdc_id','?')
        name = r.get('payload',{}).get('name','?')
        vid = r.get('vehicleId','?')
        val = r.get('payload',{}).get('value','?')
        print(f'  #{n:2d}  vehicle={vid}  mdc_id={mdc:>5s}  signal={name}  value={val}')
    except: pass
print(f'\n  Total: {n} records')
" 2>/dev/null
}

case "$STEP" in

  # ─── SETUP: create all resources ─────────────────────────────────────────
  setup)
    ensure_login
    log "=== SETUP: Creating all resources ==="

    # Create topics that have NO Flink DDL (raw bytes, no schema)
    create_topic_if_missing "$INPUT_TOPIC"
    create_topic_if_missing "$SUB_TOPIC"
    create_topic_if_missing "$ACK_TOPIC"
    # NOTE: do NOT create $OUTPUT_TOPIC manually. The DDL below creates it
    # WITH the proper schema. Creating it first would auto-map it as raw
    # BYTES in the Flink catalog, and the DDL would fail with "table already exists".

    # Submit DDL for output table (CREATE TABLE creates topic + registers JSON Schema)
    log "Submitting output table DDL (creates $OUTPUT_TOPIC + schema)..."
    DDL_CONTENT="$(cat "$DDL_SQL")"
    confluent flink statement create "live-demo-ddl" \
      --sql "$DDL_CONTENT" \
      --compute-pool "$CC_COMPUTE_POOL" \
      --database "$CC_CLUSTER_ID" \
      --environment "$CC_ENV_ID" \
      --wait

    # Produce telemetry fixture data — this stays on the topic for observation
    log "Producing 10 telemetry fixture records to $INPUT_TOPIC..."
    confluent kafka topic produce "$INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" < "$FIXTURE"

    log "=== SETUP COMPLETE ==="
    echo ""
    echo "Topics created:"
    echo "  $INPUT_TOPIC    — 10 telemetry records (raw JSON, signals[] nested)"
    echo "  $OUTPUT_TOPIC   — sink for filtered flat per-signal records"
    echo "  $SUB_TOPIC      — subscription messages (you produce here)"
    echo "  $ACK_TOPIC      — ACK responses (orchestrator writes here)"
    echo ""
    echo "Next steps:"
    echo "  ./test-data/run-live-demo.sh subscribe    # create filter for vehicle-fixture-001 + mdc 100,200"
    echo "  ./test-data/run-live-demo.sh observe      # see matching records (CC Flink reads from earliest)"
    echo "  ./test-data/run-live-demo.sh produce      # send more data through"
    ;;

  # ─── SUBSCRIBE: create filter statement via orchestrator ─────────────────
  subscribe)
    ensure_login
    log "=== SUBSCRIBE: vehicle-fixture-001 + mdc_id IN (100, 200) ==="

    python3 "$DIR/test-data/generate-subscription-msg.py" \
      --mode single \
      --vehicle vehicle-fixture-001 \
      --correlation "live-demo-c1" \
      --mdc 100,200 \
      > /tmp/live-sub.json

    log "Subscription: $(cat /tmp/live-sub.json)"
    log "Running orchestrator (file mode)..."

    java -jar "$ORCHESTRATOR_JAR" \
      --mode file \
      --subscription /tmp/live-sub.json \
      --template "$TEMPLATE" \
      --state "$STATE_FILE" \
      --input-topic "$INPUT_TOPIC" \
      --output-topic "$OUTPUT_TOPIC" \
      --compute-pool "$CC_COMPUTE_POOL" \
      --cluster "$CC_CLUSTER_ID" \
      --environment "$CC_ENV_ID" \
      --cloud "$CC_CLOUD" \
      --region "$CC_REGION"

    log "=== SUBSCRIBE DONE ==="
    echo ""
    echo "A Flink filter statement is now RUNNING."
    echo "It reads from $INPUT_TOPIC and writes matching signals to $OUTPUT_TOPIC."
    echo ""
    echo "NOTE: CC Flink defaults to EARLIEST offset — the 10 records from setup"
    echo "are already being processed. Expect 9 matching records in $OUTPUT_TOPIC."
    echo ""
    echo "  ./test-data/run-live-demo.sh observe      # see records already in output"
    echo "  ./test-data/run-live-demo.sh produce      # send 10 more records (9 more matches)"
    echo ""
    echo "Or in the Flink shell, run: SELECT * FROM \`$OUTPUT_TOPIC\` LIMIT 10;"
    ;;

  # ─── PRODUCE: send more telemetry data ────────────────────────────────────────
  produce)
    ensure_login
    log "=== PRODUCE: 10 telemetry records to $INPUT_TOPIC ==="
    confluent kafka topic produce "$INPUT_TOPIC" --cluster "$CC_CLUSTER_ID" < "$FIXTURE"
    log "Produced. If a filter statement is RUNNING, output should appear shortly."
    echo ""
    echo "Observe with:"
    echo "  ./test-data/run-live-demo.sh observe"
    ;;

  # ─── OBSERVE: consume output topic ───────────────────────────────────────
  observe)
    ensure_login
    log "=== OBSERVE: all records in $OUTPUT_TOPIC from beginning (Ctrl-C to stop) ==="
    echo ""
    consume_pretty --from-beginning
    ;;

  # ─── OBSERVE-NEW: only records arriving after this point ────────────────
  observe-new)
    ensure_login
    log "=== OBSERVE-NEW: waiting for NEW records on $OUTPUT_TOPIC (Ctrl-C to stop) ==="
    echo "(Only records produced AFTER this command started will appear)"
    echo ""
    consume_pretty
    ;;

  # ─── UPDATE: change filter predicate with carry-over ─────────────────────
  update)
    ensure_login
    log "=== UPDATE: vehicle-fixture-001 + mdc_id IN (200, 300) ==="

    python3 "$DIR/test-data/generate-subscription-msg.py" \
      --mode update \
      --vehicle vehicle-fixture-001 \
      --correlation "live-demo-c2" \
      --mdc 200,300 \
      > /tmp/live-sub-update.json

    log "Updated subscription: $(cat /tmp/live-sub-update.json)"
    log "Running orchestrator (carry-over offsets)..."

    java -jar "$ORCHESTRATOR_JAR" \
      --mode file \
      --subscription /tmp/live-sub-update.json \
      --template "$TEMPLATE" \
      --state "$STATE_FILE" \
      --input-topic "$INPUT_TOPIC" \
      --output-topic "$OUTPUT_TOPIC" \
      --compute-pool "$CC_COMPUTE_POOL" \
      --cluster "$CC_CLUSTER_ID" \
      --environment "$CC_ENV_ID" \
      --cloud "$CC_CLOUD" \
      --region "$CC_REGION"

    log "=== UPDATE DONE ==="
    echo ""
    echo "New filter: mdc_id IN (200, 300). Old statement stopped, new one RUNNING."
    echo ""
    echo "To see the difference, start observe-new FIRST (waits for new records),"
    echo "then produce in another terminal:"
    echo ""
    echo "  Terminal 1:  ./test-data/run-live-demo.sh observe-new"
    echo "  Terminal 2:  ./test-data/run-live-demo.sh produce"
    echo ""
    echo "observe-new shows ONLY records arriving after it starts — so you'll see"
    echo "the new filter (mdc 200,300 = 6 records) without old records mixed in."
    echo ""
    echo "Or use observe (from-beginning) to see all records with counts:"
    echo "  Before update: 9 records (mdc 100,200)"
    echo "  After produce: 9 + 6 = 15 records (new ones have mdc 200 or 300)"
    ;;

  # ─── UNSUBSCRIBE: stop filtering ────────────────────────────────────────
  unsubscribe)
    ensure_login
    log "=== UNSUBSCRIBE: vehicle-fixture-001 ==="

    python3 "$DIR/test-data/generate-subscription-msg.py" \
      --mode unsubscribe \
      --vehicle vehicle-fixture-001 \
      --correlation "live-demo-c3" \
      > /tmp/live-sub-unsub.json

    java -jar "$ORCHESTRATOR_JAR" \
      --mode file \
      --subscription /tmp/live-sub-unsub.json \
      --template "$TEMPLATE" \
      --state "$STATE_FILE" \
      --input-topic "$INPUT_TOPIC" \
      --output-topic "$OUTPUT_TOPIC" \
      --compute-pool "$CC_COMPUTE_POOL" \
      --cluster "$CC_CLUSTER_ID" \
      --environment "$CC_ENV_ID" \
      --cloud "$CC_CLOUD" \
      --region "$CC_REGION"

    log "=== UNSUBSCRIBE DONE ==="
    echo "Statement stopped. No more output from new data."
    ;;

  # ─── STATUS: show what exists on CC ──────────────────────────────────────
  status)
    ensure_login
    log "=== STATUS ==="
    echo ""
    echo "── Flink statements ──"
    confluent flink statement list --cloud "$CC_CLOUD" --region "$CC_REGION" --output json 2>/dev/null \
      | python3 -c "
import json,sys
for s in json.load(sys.stdin):
    n = s.get('name','?')
    if n.startswith('kf-') or n.startswith('live-') or n.startswith('kafka-variant'):
        print(f\"  {n:40s} {s.get('status','?'):12s}\")
" 2>/dev/null || echo "  (none or error)"

    echo ""
    echo "── Topics ──"
    for t in "$INPUT_TOPIC" "$OUTPUT_TOPIC" "$SUB_TOPIC" "$ACK_TOPIC"; do
      if topic_exists "$t"; then
        echo "  $t  ✓"
      else
        echo "  $t  ✗ (not created)"
      fi
    done

    echo ""
    echo "── Allocator state ──"
    if [[ -f "$STATE_FILE" ]]; then
      cat "$STATE_FILE"
    else
      echo "  (no state file at $STATE_FILE)"
    fi
    ;;

  # ─── CLEAN: tear down everything ─────────────────────────────────────────
  clean)
    ensure_login
    log "=== CLEAN: tearing down all live demo resources ==="

    # Stop + delete all known statements
    for stmt in $(confluent flink statement list --cloud "$CC_CLOUD" --region "$CC_REGION" --output json 2>/dev/null \
      | python3 -c "import json,sys; [print(s['name']) for s in json.load(sys.stdin) if s['name'].startswith(('kf-','live-','kafka-variant'))]" 2>/dev/null); do
      log "Stopping + deleting $stmt"
      confluent flink statement stop "$stmt" --cloud "$CC_CLOUD" --region "$CC_REGION" 2>/dev/null || true
      confluent flink statement delete "$stmt" --cloud "$CC_CLOUD" --region "$CC_REGION" --force 2>/dev/null || true
    done

    for t in "$INPUT_TOPIC" "$OUTPUT_TOPIC" "$SUB_TOPIC" "$ACK_TOPIC"; do
      if topic_exists "$t"; then
        log "Deleting topic $t"
        confluent kafka topic delete "$t" --cluster "$CC_CLUSTER_ID" --force 2>/dev/null || true
      fi
    done

    # Clean SR subjects
    for subj in "${OUTPUT_TOPIC}-value"; do
      confluent schema-registry subject delete "$subj" --force 2>/dev/null || true
      confluent schema-registry subject delete "$subj" --permanent --force 2>/dev/null || true
    done

    rm -f "$STATE_FILE" /tmp/live-sub*.json
    log "=== CLEAN DONE ==="
    ;;

  # ─── HELP ────────────────────────────────────────────────────────────────
  help|*)
    echo "Live demo — observable Flink SQL filter lifecycle"
    echo ""
    echo "Usage: ./test-data/run-live-demo.sh <step>"
    echo ""
    echo "Steps (run in order):"
    echo "  setup        Create topics + DDL + produce initial data"
    echo "  subscribe    Create filter for vehicle-fixture-001 + mdc 100,200"
    echo "  produce      Send 10 more telemetry records (do AFTER subscribe)"
    echo "  observe      Show ALL records from beginning (with counts + mdc_id summary)"
    echo "  observe-new  Show ONLY new records (start first, then produce in another terminal)"
    echo "  update       Change filter to mdc 200,300 (carry-over offsets)"
    echo "  produce      Send more data (see new filter in action)"
    echo "  observe-new  See only the new records with the updated filter"
    echo "  unsubscribe  Stop filtering"
    echo "  status       Show all statements + topics"
    echo "  clean        Tear down everything"
    echo ""
    echo "Tip: after 'update', use 'observe-new' + 'produce' (two terminals) to see"
    echo "     only the new records from the updated filter, not the old ones mixed in."
    echo ""
    echo "Key differences from verify-phase1.sh:"
    echo "  - Does NOT auto-cleanup — resources persist for observation"
    echo "  - CC Flink reads from EARLIEST by default — setup data is processed on subscribe"
    echo "  - Shows record counts + mdc_id/signal summary (not raw JSON)"
    echo "  - Creates subscription + ACK topics (unused in file mode but visible)"
    ;;
esac
