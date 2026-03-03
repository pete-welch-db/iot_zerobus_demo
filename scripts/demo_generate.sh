#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

IOTHUB_NAME="${IOTHUB_NAME:-iothub-zerobus-demo-welch}"
DEVICES_FILE="${DEVICES_FILE:-edge-python/devices.json}"
DURATION_SECONDS="${DURATION_SECONDS:-180}"
MESSAGE_RATE_HZ="${MESSAGE_RATE_HZ:-1.0}"
FAULT_PERIOD_SECONDS="${FAULT_PERIOD_SECONDS:-180}"

PYTHON_BIN=".venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

echo "==> Starting virtual fleet generation"
"$PYTHON_BIN" edge-python/simulate_fleet_iothub.py \
  --iothub-name "$IOTHUB_NAME" \
  --devices-file "$DEVICES_FILE" \
  --duration-seconds "$DURATION_SECONDS" \
  --message-rate-hz "$MESSAGE_RATE_HZ" \
  --fault-period-seconds "$FAULT_PERIOD_SECONDS"

echo "==> Triggering bridge + DLT refresh after generation"
databricks bundle run -t "$TARGET" iothub_to_zerobus_autorun
databricks bundle run -t "$TARGET" iot_pipeline_keepalive
databricks bundle run -t "$TARGET" iot_ml_realtime_scoring

echo "==> Fleet freshness check (last 15 minutes)"
sql_query "SELECT machine_id, state, telemetry_lag_seconds, ml_lag_seconds, prob_fault_next_5m, last_event_time FROM $CATALOG.$SCHEMA.vw_machine_current_status WHERE last_event_time >= current_timestamp() - INTERVAL 15 MINUTES ORDER BY telemetry_lag_seconds ASC, last_event_time DESC LIMIT 50"

echo "generate phase complete."
