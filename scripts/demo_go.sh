#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

MACHINE_ID="${MACHINE_ID:-MC-0000}"

echo "==> Resolving job IDs from bundle summary (target=$TARGET)"
BRIDGE_JOB_ID="$(get_job_id iothub_to_zerobus_autorun || true)"
ORCHESTRATION_JOB_ID="$(get_job_id iot_orchestration || true)"
PIPELINE_ID="$(get_pipeline_id iot_telemetry_medallion || true)"

if [[ -z "$BRIDGE_JOB_ID" || -z "$ORCHESTRATION_JOB_ID" || -z "$PIPELINE_ID" ]]; then
  echo "Failed to resolve one or more required resource IDs from bundle summary."
  echo "bridge_job_id=$BRIDGE_JOB_ID orchestration_job_id=$ORCHESTRATION_JOB_ID pipeline_id=$PIPELINE_ID"
  exit 1
fi

echo "==> Ensuring continuous bridge is running"
if [[ "$(job_has_active_run "$BRIDGE_JOB_ID")" == "true" ]]; then
  echo "Bridge already running (job_id=$BRIDGE_JOB_ID)."
else
  databricks jobs run-now "$BRIDGE_JOB_ID" --no-wait >/dev/null
  echo "Started continuous bridge (job_id=$BRIDGE_JOB_ID)."
fi

echo "==> Ensuring continuous DLT pipeline is running"
if [[ "$(pipeline_is_running "$PIPELINE_ID")" == "true" ]]; then
  echo "DLT pipeline already running (pipeline_id=$PIPELINE_ID)."
else
  databricks pipelines start-update "$PIPELINE_ID" >/dev/null
  echo "Started DLT pipeline update (pipeline_id=$PIPELINE_ID)."
fi

echo "==> Triggering post-medallion orchestration job"
databricks jobs run-now "$ORCHESTRATION_JOB_ID" --no-wait >/dev/null

echo "==> Live health check for $MACHINE_ID"
sql_query "SELECT machine_id, state, last_event_time, telemetry_lag_ms, ml_lag_ms, prob_fault_next_5m FROM $CATALOG.$SCHEMA.vw_machine_current_status WHERE machine_id = '$MACHINE_ID'"

echo "go phase complete."
