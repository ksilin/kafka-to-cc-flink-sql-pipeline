# =============================================================================
# Carry-Over Offset Lifecycle for Confluent Cloud Flink Statements
# =============================================================================
#
# This example shows how to replace a running Flink SQL statement (v1) with
# an updated one (v2) using CC Flink's carry-over offset facility, so v2
# resumes from where v1 left off -- no reprocessing, no gap.
#
# LIFECYCLE (3-step apply sequence):
#
#   Step 1 -- Initial deploy:
#     - v1 is active (stopped = false)
#     - v2 resource is commented out or not yet created
#     - `terraform apply` creates v1; it transitions to RUNNING.
#
#   Step 2 -- Create v2 with carry-over:
#     - Uncomment the v2 resource block (or add it).
#     - v2 references v1 via:
#         properties = { "sql.tables.initial-offset-from" = confluent_flink_statement.kf_flt_v1.statement_name }
#     - `terraform apply` creates v2 in PENDING state (it waits for v1 to stop).
#     - v1 is still running at this point.
#
#   Step 3 -- Stop v1, v2 auto-transitions:
#     - Set `stopped = true` on the v1 resource.
#     - `terraform apply` stops v1.
#     - CC automatically transitions v2 from PENDING -> RUNNING using v1's
#       last offsets. No manual offset management needed.
#     - CC waits up to 6 hours for the referenced statement to stop; after
#       that the carry-over fails.
#
# IMPORTANT CONSTRAINTS:
#   - Carry-over works ONLY for stateless statements (no aggregates, no
#     windows, no pattern matching, no upsert sinks).
#   - v1 and v2 must be in the same org, environment, and region.
#   - v1 must be STOPPED, not DELETED. Deleting v1 dangles the carry-over
#     reference and v2 will never transition to RUNNING.
#   - In Terraform terms: never `terraform destroy` the old statement while
#     v2 references it. Use `stopped = true` instead.
#   - Use `lifecycle { prevent_destroy = true }` on production statements
#     to guard against accidental `terraform destroy`.
#
# KEY TF RESOURCE ATTRIBUTES:
#   - `stopped` (bool, optional+computed) -- controls statement lifecycle.
#     Setting false->true stops the statement. Setting true->false resumes
#     it (only for INSERT INTO / CREATE TABLE AS / EXECUTE STATEMENT SET).
#   - `latest_offsets` (map, computed, read-only) -- the last Kafka offsets
#     processed. Populated ONLY when the statement is in STOPPED state.
#   - `latest_offsets_timestamp` (string, computed, read-only) -- RFC3339 UTC
#     timestamp of when offsets were captured.
#   - `properties` (map) -- where "sql.tables.initial-offset-from" is set.
#   - `statement_name` (string, optional+computed) -- if omitted, the
#     provider auto-generates one. We set it explicitly for clarity and so
#     the carry-over reference is deterministic.
#
# REFERENCE:
#   - CC carry-over docs:  https://docs.confluent.io/cloud/current/flink/operate-and-deploy/carry-over-offsets.html
#   - TF resource docs:    https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/confluent_flink_statement
#   - Provider example:    https://github.com/confluentinc/terraform-provider-confluent/tree/master/examples/configurations/flink-carry-over-offset-between-statements
# =============================================================================

terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = ">= 2.72.0"
    }
  }
}

# -----------------------------------------------------------------------------
# Variables -- match project env.example.sh naming
# -----------------------------------------------------------------------------

variable "cc_org_id" {
  type        = string
  description = "Confluent Cloud organization ID"
}

variable "cc_env_id" {
  type        = string
  default     = "env-nvv5xz"
  description = "CC environment ID (project: ksilin)"
}

variable "cc_env_display_name" {
  type        = string
  description = "Environment display name -- maps to Flink catalog"
}

variable "cc_cluster_display_name" {
  type        = string
  description = "Kafka cluster display name -- maps to Flink database (NOT the cluster ID)"
}

variable "cc_compute_pool_id" {
  type        = string
  default     = "lfcp-kknvdm"
  description = "Flink compute pool ID (project: ksilin_tmp_pool)"
}

variable "flink_rest_endpoint" {
  type        = string
  description = "Flink REST endpoint URL for the target region"
}

variable "flink_api_key" {
  type        = string
  description = "Flink API key (owner must have FlinkAdmin role)"
}

variable "flink_api_secret" {
  type        = string
  sensitive   = true
  description = "Flink API secret"
}

variable "flink_principal_id" {
  type        = string
  description = "Service account ID that runs the Flink statements"
}

# -----------------------------------------------------------------------------
# Provider -- Flink-scoped configuration
# -----------------------------------------------------------------------------

provider "confluent" {
  organization_id       = var.cc_org_id
  environment_id        = var.cc_env_id
  flink_compute_pool_id = var.cc_compute_pool_id
  flink_rest_endpoint   = var.flink_rest_endpoint
  flink_api_key         = var.flink_api_key
  flink_api_secret      = var.flink_api_secret
  flink_principal_id    = var.flink_principal_id
}

# =============================================================================
# Locals -- SQL loaded from the project's sql/ directory
# =============================================================================

locals {
  # In production, use file() or templatefile() to load SQL:
  #   v1_sql = file("${path.module}/../sql/01-filter-F1.sql")
  #   v2_sql = file("${path.module}/../sql/01-filter-F1-update.sql")
  #
  # Inline here for self-containment. These match the project's verified
  # F1 and F1-update patterns (mdc_id IN (100,200) -> (200,300)).

  v1_sql = <<-EOT
    INSERT INTO `kf-data-test`
    SELECT
        CAST(ROW(
            JSON_VALUE(CAST(val AS STRING), '$.payload.containerId'),
            JSON_VALUE(CAST(val AS STRING), '$.payload.containerMsgCount'),
            JSON_VALUE(signal_str, '$.containerMsgId'),
            JSON_VALUE(CAST(val AS STRING), '$.payload.correlationId'),
            JSON_VALUE(signal_str, '$.g_id'),
            JSON_VALUE(CAST(val AS STRING), '$.payload.ingest_timestamp'),
            JSON_VALUE(signal_str, '$.mdc_id'),
            JSON_VALUE(signal_str, '$.name'),
            JSON_VALUE(CAST(val AS STRING), '$.payload.orderId'),
            JSON_VALUE(CAST(val AS STRING), '$.payload.orderVersion'),
            JSON_VALUE(signal_str, '$.timestamp'),
            JSON_VALUE(signal_str, '$.unit'),
            JSON_VALUE(signal_str, '$.value')
        ) AS ROW<
            `containerId`       STRING,
            `containerMsgCount` STRING,
            `containerMsgId`    STRING,
            `correlationId`     STRING,
            `g_id`              STRING,
            `ingest_timestamp`  STRING,
            `mdc_id`            STRING,
            `name`              STRING,
            `orderId`           STRING,
            `orderVersion`      STRING,
            `timestamp`         STRING,
            `unit`              STRING,
            `value`             STRING
        >) AS `payload`,
        '' AS payloadUrl,
        JSON_VALUE(CAST(val AS STRING), '$.time') AS `time`,
        CAST(
            JSON_QUERY(CAST(val AS STRING), '$.useCaseIds' RETURNING ARRAY<STRING>)
            AS ARRAY<STRING>
        ) AS useCaseIds,
        JSON_VALUE(CAST(val AS STRING), '$.userId') AS userId,
        JSON_VALUE(CAST(val AS STRING), '$.userMessageId') AS userMessageId,
        JSON_VALUE(CAST(val AS STRING), '$.vehicleId') AS vehicleId
    FROM `kf-input-test`
    CROSS JOIN UNNEST(
        CAST(
            JSON_QUERY(CAST(val AS STRING), 'lax $.payload.signals[*]'
                RETURNING ARRAY<STRING>)
            AS ARRAY<STRING>
        )
    ) AS T(signal_str)
    WHERE JSON_VALUE(CAST(val AS STRING), '$.vehicleId') = 'vehicle-fixture-001'
      AND CAST(JSON_VALUE(signal_str, '$.mdc_id') AS BIGINT) IN (100, 200)
  EOT

  v2_sql = <<-EOT
    INSERT INTO `kf-data-test`
    SELECT
        CAST(ROW(
            JSON_VALUE(CAST(val AS STRING), '$.payload.containerId'),
            JSON_VALUE(CAST(val AS STRING), '$.payload.containerMsgCount'),
            JSON_VALUE(signal_str, '$.containerMsgId'),
            JSON_VALUE(CAST(val AS STRING), '$.payload.correlationId'),
            JSON_VALUE(signal_str, '$.g_id'),
            JSON_VALUE(CAST(val AS STRING), '$.payload.ingest_timestamp'),
            JSON_VALUE(signal_str, '$.mdc_id'),
            JSON_VALUE(signal_str, '$.name'),
            JSON_VALUE(CAST(val AS STRING), '$.payload.orderId'),
            JSON_VALUE(CAST(val AS STRING), '$.payload.orderVersion'),
            JSON_VALUE(signal_str, '$.timestamp'),
            JSON_VALUE(signal_str, '$.unit'),
            JSON_VALUE(signal_str, '$.value')
        ) AS ROW<
            `containerId`       STRING,
            `containerMsgCount` STRING,
            `containerMsgId`    STRING,
            `correlationId`     STRING,
            `g_id`              STRING,
            `ingest_timestamp`  STRING,
            `mdc_id`            STRING,
            `name`              STRING,
            `orderId`           STRING,
            `orderVersion`      STRING,
            `timestamp`         STRING,
            `unit`              STRING,
            `value`             STRING
        >) AS `payload`,
        '' AS payloadUrl,
        JSON_VALUE(CAST(val AS STRING), '$.time') AS `time`,
        CAST(
            JSON_QUERY(CAST(val AS STRING), '$.useCaseIds' RETURNING ARRAY<STRING>)
            AS ARRAY<STRING>
        ) AS useCaseIds,
        JSON_VALUE(CAST(val AS STRING), '$.userId') AS userId,
        JSON_VALUE(CAST(val AS STRING), '$.userMessageId') AS userMessageId,
        JSON_VALUE(CAST(val AS STRING), '$.vehicleId') AS vehicleId
    FROM `kf-input-test`
    CROSS JOIN UNNEST(
        CAST(
            JSON_QUERY(CAST(val AS STRING), 'lax $.payload.signals[*]'
                RETURNING ARRAY<STRING>)
            AS ARRAY<STRING>
        )
    ) AS T(signal_str)
    WHERE JSON_VALUE(CAST(val AS STRING), '$.vehicleId') = 'vehicle-fixture-001'
      AND CAST(JSON_VALUE(signal_str, '$.mdc_id') AS BIGINT) IN (200, 300)
  EOT
}

# =============================================================================
# Statement v1 -- the original filter
# =============================================================================
#
# Step 1: Deploy with stopped = false (or omit -- defaults to running).
# Step 3: After v2 is created, change to stopped = true and re-apply.

resource "confluent_flink_statement" "kf_flt_v1" {
  statement      = local.v1_sql
  statement_name = "kf-flt-v1"

  properties = {
    "sql.current-catalog"  = var.cc_env_display_name
    "sql.current-database" = var.cc_cluster_display_name
  }

  # --- Lifecycle control ---
  # Start with `stopped = false` (statement runs immediately).
  # In Step 3, change this to `true` and `terraform apply`.
  # This triggers CC to stop v1, which unblocks v2's carry-over.
  stopped = false # <-- Change to `true` in Step 3

  # CRITICAL: prevent_destroy guards against `terraform destroy` wiping the
  # statement. Deleting v1 while v2 holds a carry-over reference to it will
  # dangle that reference -- v2 stays PENDING forever.
  # Remove this block only AFTER v2 is confirmed RUNNING and you no longer
  # need v1's offset history.
  lifecycle {
    prevent_destroy = true
  }
}

# =============================================================================
# Statement v2 -- the updated filter with carry-over from v1
# =============================================================================
#
# Step 2: Uncomment this block and `terraform apply`.
#         v2 will be created in PENDING state, waiting for v1 to stop.
#
# Step 3: Set kf_flt_v1.stopped = true above and `terraform apply` again.
#         CC automatically transitions v2 from PENDING -> RUNNING using
#         v1's last committed offsets.
#
# The key property is "sql.tables.initial-offset-from" which references
# v1's statement_name. The TF provider exposes this as the `statement_name`
# attribute on the referenced resource, so you get a deterministic reference
# without hardcoding the name string.

resource "confluent_flink_statement" "kf_flt_v2" {
  statement      = local.v2_sql
  statement_name = "kf-flt-v2"

  properties = {
    "sql.current-catalog"  = var.cc_env_display_name
    "sql.current-database" = var.cc_cluster_display_name

    # THIS IS THE CARRY-OVER PROPERTY.
    # It tells CC Flink: "when v2 starts, use v1's last offsets as v2's
    # starting point." v2 enters PENDING until v1 is STOPPED.
    "sql.tables.initial-offset-from" = confluent_flink_statement.kf_flt_v1.statement_name
  }

  stopped = false

  # depends_on is implicit via the .statement_name reference above, but
  # we make it explicit for clarity: v2 must not be submitted before v1
  # exists (TF needs v1's statement_name to populate the property).
  depends_on = [confluent_flink_statement.kf_flt_v1]

  lifecycle {
    prevent_destroy = true
  }
}

# =============================================================================
# Outputs -- useful for debugging the carry-over lifecycle
# =============================================================================

output "v1_statement_name" {
  value       = confluent_flink_statement.kf_flt_v1.statement_name
  description = "v1 statement name (referenced by v2's carry-over property)"
}

output "v1_latest_offsets" {
  value       = confluent_flink_statement.kf_flt_v1.latest_offsets
  description = "v1's last committed offsets (populated only when stopped)"
}

output "v1_latest_offsets_timestamp" {
  value       = confluent_flink_statement.kf_flt_v1.latest_offsets_timestamp
  description = "UTC timestamp of v1's offset snapshot"
}

output "v2_statement_name" {
  value       = confluent_flink_statement.kf_flt_v2.statement_name
  description = "v2 statement name"
}
