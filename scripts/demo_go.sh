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

get_pipeline_id() {
  local resource_key="$1"
  databricks bundle summary -t "$TARGET" | awk -v key="$resource_key" '
    $1 == key ":" { in_key = 1; next }
    in_key && $1 == "URL:" {
      if (match($2, /pipelines\/([0-9a-f-]+)/, arr)) {
        print arr[1]
        exit
      }
    }
    in_key && NF == 0 { in_key = 0 }
  '
}

job_has_active_run() {
  local job_id="$1"
  databricks jobs list-runs --job-id "$job_id" --active-only --limit 1 --output json 2>/dev/null | \
    python3 -c 'import json,sys; d=json.load(sys.stdin); print("true" if (d.get("runs") or []) else "false")'
}

pipeline_is_running() {
  local pipeline_id="$1"
  databricks pipelines get --pipeline-id "$pipeline_id" --output json 2>/dev/null | \
    python3 -c 'import json,sys; d=json.load(sys.stdin); print("true" if d.get("state") == "RUNNING" else "false")'
}

echo "==> Resolving job IDs from bundle summary (target=$TARGET)"
BRIDGE_JOB_ID="$(get_job_id iothub_to_zerobus_autorun || true)"
KEEPALIVE_JOB_ID="$(get_job_id iot_pipeline_keepalive || true)"
ML_JOB_ID="$(get_job_id iot_ml_realtime_scoring || true)"
PIPELINE_ID="$(get_pipeline_id iot_telemetry_medallion || true)"

if [[ -z "$BRIDGE_JOB_ID" || -z "$KEEPALIVE_JOB_ID" || -z "$ML_JOB_ID" || -z "$PIPELINE_ID" ]]; then
  echo "Failed to resolve one or more required resource IDs from bundle summary."
  echo "bridge_job_id=$BRIDGE_JOB_ID keepalive_job_id=$KEEPALIVE_JOB_ID ml_job_id=$ML_JOB_ID pipeline_id=$PIPELINE_ID"
  exit 1
fi

echo "==> Ensuring continuous bridge is running"
if [[ "$(job_has_active_run "$BRIDGE_JOB_ID")" == "true" ]]; then
  echo "Bridge already running (job_id=$BRIDGE_JOB_ID)."
else
  databricks jobs run-now --job-id "$BRIDGE_JOB_ID" >/dev/null
  echo "Started continuous bridge (job_id=$BRIDGE_JOB_ID)."
fi

echo "==> Ensuring continuous DLT pipeline is running"
if [[ "$(pipeline_is_running "$PIPELINE_ID")" == "true" ]]; then
  echo "DLT pipeline already running (pipeline_id=$PIPELINE_ID)."
else
  databricks jobs run-now --job-id "$KEEPALIVE_JOB_ID" >/dev/null
  echo "Triggered pipeline keepalive/start (job_id=$KEEPALIVE_JOB_ID)."
fi

echo "==> Triggering realtime ML scoring job"
databricks jobs run-now --job-id "$ML_JOB_ID" >/dev/null

echo "==> Live health check for $MACHINE_ID"
databricks api post /api/2.0/sql/statements --json "$(cat <<EOF
{"warehouse_id":"$WAREHOUSE_ID","statement":"SELECT machine_id, state, last_event_time, telemetry_lag_seconds, ml_lag_seconds, prob_fault_next_5m FROM $CATALOG.$SCHEMA.vw_machine_current_status WHERE machine_id = '$MACHINE_ID'"}
EOF
)"

echo "go phase complete."
