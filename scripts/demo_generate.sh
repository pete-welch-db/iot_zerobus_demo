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
WAVE_MODE="${WAVE_MODE:-wave}"
WAVE_RAMP_SECONDS="${WAVE_RAMP_SECONDS:-600}"
PHASE_STAGGER_SECONDS="${PHASE_STAGGER_SECONDS:-2.5}"
DEGRADING_DEVICE_FRACTION="${DEGRADING_DEVICE_FRACTION:-0.30}"
RISKY_DEVICE_FRACTION="${RISKY_DEVICE_FRACTION:-0.20}"

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
    databricks jobs run-now "$BRIDGE_JOB_ID" --no-wait >/dev/null
  fi
fi
PIPELINE_ID="$(get_pipeline_id iot_telemetry_medallion || true)"
if [[ -n "$PIPELINE_ID" && "$(pipeline_is_running "$PIPELINE_ID")" != "true" ]]; then
  databricks pipelines start-update "$PIPELINE_ID" >/dev/null
fi

echo "==> Running simulator"
"$PYTHON_BIN" edge-python/simulate_fleet_iothub.py \
  --mode stream \
  --iothub-name "$IOTHUB_NAME" \
  --devices-file "$DEVICES_FILE" \
  --duration-seconds "$DURATION_SECONDS" \
  --message-rate-hz "$MESSAGE_RATE_HZ" \
  --fault-period-seconds "$FAULT_PERIOD_SECONDS" \
  --target-total-records "$TARGET_TOTAL_RECORDS" \
  --wave-mode "$WAVE_MODE" \
  --wave-ramp-seconds "$WAVE_RAMP_SECONDS" \
  --phase-stagger-seconds "$PHASE_STAGGER_SECONDS" \
  --degrading-device-fraction "$DEGRADING_DEVICE_FRACTION" \
  --risky-device-fraction "$RISKY_DEVICE_FRACTION"

echo "==> Triggering post-medallion orchestration"
databricks bundle run -t "$TARGET" iot_zerobus_orchestration --no-wait

echo "==> Fleet freshness check (last 5 minutes)"
sql_query "SELECT machine_id, state, telemetry_lag_ms, ml_lag_ms, prob_fault_next_5m, last_event_time FROM $CATALOG.$SCHEMA.vw_machine_current_status WHERE last_event_time >= current_timestamp() - INTERVAL 5 MINUTES ORDER BY telemetry_lag_ms ASC, last_event_time DESC LIMIT 50"

echo "==> Risk mix check (last 10 minutes)"
sql_query "SELECT CASE WHEN prob_fault_next_5m >= 0.8 THEN 'CRITICAL' WHEN prob_fault_next_5m >= 0.5 THEN 'WATCH' ELSE 'NORMAL' END AS risk_band, COUNT(*) AS machine_count FROM $CATALOG.$SCHEMA.vw_machine_current_status WHERE last_event_time >= current_timestamp() - INTERVAL 10 MINUTES GROUP BY 1 ORDER BY 1"

echo "==> Fault diversity check (last 10 minutes)"
sql_query "SELECT COALESCE(fault_code,'NONE') AS fault_code, COUNT(*) AS records FROM $CATALOG.$SCHEMA.silver_machine_telemetry WHERE event_time >= current_timestamp() - INTERVAL 10 MINUTES GROUP BY 1 ORDER BY records DESC LIMIT 10"

if [[ -f "lakebase.env" ]]; then
  echo "==> Lakebase mirror freshness check"
  # shellcheck disable=SC1091
  source "lakebase.env"
  PYTHON_BIN=".venv/bin/python"
  if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="python3"
  fi
  "$PYTHON_BIN" - <<'PY'
import os
import psycopg2

required = ["LAKEBASE_DB_HOST", "LAKEBASE_DB_PORT", "LAKEBASE_DB_NAME", "LAKEBASE_DB_USER", "LAKEBASE_DB_PASSWORD"]
missing = [k for k in required if not os.getenv(k)]
if missing:
    print(f"Skipping Lakebase check, missing keys: {missing}")
    raise SystemExit(0)

conn = psycopg2.connect(
    host=os.environ["LAKEBASE_DB_HOST"],
    port=int(os.environ.get("LAKEBASE_DB_PORT", "5432")),
    dbname=os.environ["LAKEBASE_DB_NAME"],
    user=os.environ["LAKEBASE_DB_USER"],
    password=os.environ["LAKEBASE_DB_PASSWORD"],
    sslmode="require",
)
try:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS rows, MAX(updated_at) AS latest_update
            FROM machine_current_status
            """
        )
        print("LAKEBASE_STATUS:", cur.fetchone())
finally:
    conn.close()
PY
fi

echo "generate phase complete."
