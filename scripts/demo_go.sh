#!/usr/bin/env bash
set -euo pipefail

TARGET="${TARGET:-dev}"
WAREHOUSE_ID="${WAREHOUSE_ID:-148ccb90800933a1}"
CATALOG="${CATALOG:-welch}"
SCHEMA="${SCHEMA:-iot_demo_dev}"
MACHINE_ID="${MACHINE_ID:-MACH_A}"

get_job_id() {
  local resource_key="$1"
  databricks bundle summary -t "$TARGET" | awk -v key="$resource_key" '
    $1 == key ":" { in_key = 1; next }
    in_key && $1 == "URL:" {
      if (match($2, /jobs\/([0-9]+)/, arr)) {
        print arr[1]
        exit
      }
    }
    in_key && NF == 0 { in_key = 0 }
  '
}

cancel_job_runs() {
  local job_id="$1"
  if [[ -n "$job_id" ]]; then
    databricks jobs cancel-all-runs --job-id "$job_id" --all-queued-runs >/dev/null 2>&1 || true
  fi
}

echo "==> Resolving job IDs from bundle summary (target=$TARGET)"
BRIDGE_JOB_ID="$(get_job_id iothub_to_zerobus_autorun || true)"
KEEPALIVE_JOB_ID="$(get_job_id iot_pipeline_keepalive || true)"
ML_JOB_ID="$(get_job_id iot_ml_realtime_scoring || true)"

echo "==> Cancelling overlapping runs"
cancel_job_runs "$BRIDGE_JOB_ID"
cancel_job_runs "$KEEPALIVE_JOB_ID"
cancel_job_runs "$ML_JOB_ID"

echo "==> Running bridge sweep"
databricks bundle run -t "$TARGET" iothub_to_zerobus_autorun

echo "==> Running DLT keepalive"
databricks bundle run -t "$TARGET" iot_pipeline_keepalive

echo "==> Running realtime ML scoring"
databricks bundle run -t "$TARGET" iot_ml_realtime_scoring

echo "==> Live health check for $MACHINE_ID"
databricks api post /api/2.0/sql/statements --json "$(cat <<EOF
{"warehouse_id":"$WAREHOUSE_ID","statement":"SELECT machine_id, state, last_event_time, telemetry_lag_seconds, ml_lag_seconds, prob_fault_next_5m FROM $CATALOG.$SCHEMA.vw_machine_current_status WHERE machine_id = '$MACHINE_ID'"}
EOF
)"

echo "go phase complete."
