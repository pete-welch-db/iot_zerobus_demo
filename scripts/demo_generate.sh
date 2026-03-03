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
BRIDGE_REFRESH_SECONDS="${BRIDGE_REFRESH_SECONDS:-60}"

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

echo "==> Running simulator and bridging during generation"
"$PYTHON_BIN" edge-python/simulate_fleet_iothub.py \
  --iothub-name "$IOTHUB_NAME" \
  --devices-file "$DEVICES_FILE" \
  --duration-seconds "$DURATION_SECONDS" \
  --message-rate-hz "$MESSAGE_RATE_HZ" \
  --fault-period-seconds "$FAULT_PERIOD_SECONDS" &
SIM_PID=$!

while kill -0 "$SIM_PID" 2>/dev/null; do
  echo "==> Bridge sweep (available-now)"
  if ! databricks bundle run -t "$TARGET" iothub_to_zerobus_autorun; then
    echo "Warning: bridge sweep failed; continuing."
  fi
  sleep "$BRIDGE_REFRESH_SECONDS"
done
wait "$SIM_PID"

echo "==> Triggering bridge + DLT refresh after generation"
databricks bundle run -t "$TARGET" iothub_to_zerobus_autorun
databricks bundle run -t "$TARGET" iot_pipeline_keepalive
databricks bundle run -t "$TARGET" iot_ml_realtime_scoring --no-wait

echo "==> Fleet freshness check (last 5 minutes)"
sql_query "SELECT machine_id, state, telemetry_lag_seconds, ml_lag_seconds, prob_fault_next_5m, last_event_time FROM $CATALOG.$SCHEMA.vw_machine_current_status WHERE last_event_time >= current_timestamp() - INTERVAL 5 MINUTES ORDER BY telemetry_lag_seconds ASC, last_event_time DESC LIMIT 50"

echo "generate phase complete."
