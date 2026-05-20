#!/usr/bin/env bash
# kafka-to-sql-filter / config / env.example.sh
#
# Copy this file to config/env.sh (gitignored) and adjust per machine / environment.
# Source it before running tests/verify-phase1.sh:
#
#   source kafka-to-sql-filter/config/env.sh
#   ./kafka-to-sql-filter/tests/verify-phase1.sh green
#
# The harness reads these env vars and falls back to the same defaults if unset.
# Centralizing here lets you point at a different env / cluster / pool without
# editing the harness.

# ─── Confluent Cloud infrastructure (existing, shared by both variants) ───────
# Source: HANDOFF.md (original variant validation, 2026-04-22)
export CC_ENV_ID="env-nvv5xz"           # ksilin
export CC_CLUSTER_ID="lkc-6w3rv2"       # aws_fra_basic
export CC_COMPUTE_POOL="lfcp-kknvdm"    # ksilin_tmp_pool
export CC_CLOUD="aws"
export CC_REGION="eu-central-1"

# ─── Topic names (V1 fixture-test names, NOT production) ──────────────────────
# Production names TBD per STAKEHOLDER-QUESTIONS Q11.
export KF_INPUT_TOPIC="kf-input-test"          # raw telemetry JSON, no schema
export KF_OUTPUT_TOPIC="kf-data-test"          # flat envelope sink, json-registry schema
export KF_SUBSCRIPTION_TOPIC="kf-sub-test"     # subscription messages (used in Phase 06+)
export KF_ACK_TOPIC="kf-ack-test"              # ACK messages (used in Phase 07+)

# ─── Statement names ──────────────────────────────────────────────────────────
export KF_SETUP_STATEMENT_NAME="kf-setup-output-table"
export KF_F1_STATEMENT_PREFIX="kafka-variant-f1"

# ─── Optional: Kafka API key for produce/consume (already in CLI keychain) ────
# These are only needed if your CLI session does not have a default kafka
# api-key configured. Otherwise leave unset; CLI uses the active one.
# export CC_KAFKA_API_KEY=""
# export CC_KAFKA_API_SECRET=""
