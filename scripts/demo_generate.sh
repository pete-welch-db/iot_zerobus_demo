#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

IOTHUB_NAME="${IOTHUB_NAME:-iothub-zerobus-demo-welch}"
DEVICES_FILE="${DEVICES_FILE:-edge-python/devices.json}"
# Keep simulator running long enough for live dashboards.
DURATION_SECONDS="${DURATION_SECONDS:-900}"
MESSAGE_RATE_HZ="${MESSAGE_RATE_HZ:-1.0}"
FAULT_PERIOD_SECONDS="${FAULT_PERIOD_SECONDS:-180}"
TARGET_TOTAL_RECORDS="${TARGET_TOTAL_RECORDS:-10000}"

PYTHON_BIN=".venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

echo "==> Starting virtual fleet generation"
echo "==> Validating target payload fields before publish"
"$PYTHON_BIN" edge-python/simulate_fleet_iothub.py \
  --iothub-name "$IOTHUB_NAME" \
  --devices-file "$DEVICES_FILE" \
  --validate-only

echo "==> Ensuring continuous bridge and pipeline are active"
if [[ "$(get_job_id iothub_to_zerobus_autorun || true)" != "" ]]; then
  BRIDGE_JOB_ID="$(get_job_id iothub_to_zerobus_autorun || true)"
  if [[ -n "$BRIDGE_JOB_ID" && "$(job_has_active_run "$BRIDGE_JOB_ID")" != "true" ]]; then
    databricks jobs run-now "$BRIDGE_JOB_ID" >/dev/null
  fi
fi
if [[ "$(get_job_id iot_pipeline_keepalive || true)" != "" ]]; then
  KEEPALIVE_JOB_ID="$(get_job_id iot_pipeline_keepalive || true)"
  if [[ -n "$KEEPALIVE_JOB_ID" ]]; then
    databricks jobs run-now "$KEEPALIVE_JOB_ID" >/dev/null
  fi
fi

echo "==> Running simulator"
"$PYTHON_BIN" edge-python/simulate_fleet_iothub.py \
  --mode stream \
  --iothub-name "$IOTHUB_NAME" \
  --devices-file "$DEVICES_FILE" \
  --duration-seconds "$DURATION_SECONDS" \
  --message-rate-hz "$MESSAGE_RATE_HZ" \
  --fault-period-seconds "$FAULT_PERIOD_SECONDS" \
  --target-total-records "$TARGET_TOTAL_RECORDS"

echo "==> Triggering DLT + ML refresh after generation"
databricks bundle run -t "$TARGET" iot_pipeline_keepalive
databricks bundle run -t "$TARGET" iot_ml_realtime_scoring --no-wait

echo "==> Fleet freshness check (last 5 minutes)"
sql_query "SELECT machine_id, state, telemetry_lag_ms, ml_lag_ms, prob_fault_next_5m, last_event_time FROM $CATALOG.$SCHEMA.vw_machine_current_status WHERE last_event_time >= current_timestamp() - INTERVAL 5 MINUTES ORDER BY telemetry_lag_ms ASC, last_event_time DESC LIMIT 50"

echo "generate phase complete."
