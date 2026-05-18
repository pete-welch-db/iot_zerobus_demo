#!/usr/bin/env python3
"""Query per-hop latency breakdown from the SQL Warehouse."""
import json
import subprocess
import sys

WAREHOUSE_ID = "148ccb90800933a1"

QUERIES = {
    "Hop 1+2: Device to Bronze (vw_pipeline_latency, last 20min)": """
        SELECT
            count(*) as sample_count,
            ROUND(PERCENTILE(hop1_device_to_iothub_ms, 0.50), 0) AS p50_device_to_iothub_ms,
            ROUND(PERCENTILE(hop1_device_to_iothub_ms, 0.95), 0) AS p95_device_to_iothub_ms,
            ROUND(MIN(hop1_device_to_iothub_ms), 0) AS min_device_to_iothub_ms,
            ROUND(MAX(hop1_device_to_iothub_ms), 0) AS max_device_to_iothub_ms,
            ROUND(AVG(hop1_device_to_iothub_ms), 0) AS avg_device_to_iothub_ms,
            ROUND(PERCENTILE(hop2_iothub_to_zerobus_ms, 0.50), 0) AS p50_iothub_to_bronze_ms,
            ROUND(PERCENTILE(hop2_iothub_to_zerobus_ms, 0.95), 0) AS p95_iothub_to_bronze_ms,
            ROUND(MIN(hop2_iothub_to_zerobus_ms), 0) AS min_iothub_to_bronze_ms,
            ROUND(MAX(hop2_iothub_to_zerobus_ms), 0) AS max_iothub_to_bronze_ms,
            ROUND(AVG(hop2_iothub_to_zerobus_ms), 0) AS avg_iothub_to_bronze_ms,
            ROUND(PERCENTILE(total_device_to_zerobus_ms, 0.50), 0) AS p50_device_to_bronze_ms,
            ROUND(PERCENTILE(total_device_to_zerobus_ms, 0.95), 0) AS p95_device_to_bronze_ms,
            ROUND(MIN(total_device_to_zerobus_ms), 0) AS min_device_to_bronze_ms,
            ROUND(MAX(total_device_to_zerobus_ms), 0) AS max_device_to_bronze_ms,
            ROUND(AVG(total_device_to_zerobus_ms), 0) AS avg_device_to_bronze_ms
        FROM welch.iot_demo.vw_pipeline_latency
        WHERE device_ts > current_timestamp() - INTERVAL 20 MINUTES
    """,
    "Gold table telemetry lag (current snapshot)": """
        SELECT
            count(*) as machine_count,
            ROUND(PERCENTILE(telemetry_lag_ms, 0.50), 0) AS p50_gold_lag_ms,
            ROUND(PERCENTILE(telemetry_lag_ms, 0.95), 0) AS p95_gold_lag_ms,
            ROUND(MIN(telemetry_lag_ms), 0) AS min_gold_lag_ms,
            ROUND(MAX(telemetry_lag_ms), 0) AS max_gold_lag_ms,
            ROUND(AVG(telemetry_lag_ms), 0) AS avg_gold_lag_ms
        FROM welch.iot_demo.gold_machine_latest_status
    """,
    "Bronze record count (last 20 min)": """
        SELECT count(*) as recent_bronze_records,
               min(ingest_ts) as earliest,
               max(ingest_ts) as latest
        FROM welch.iot_demo.bronze_iot_telemetry
        WHERE ingest_ts > current_timestamp() - INTERVAL 20 MINUTES
    """,
    "Gold vs Lakebase sync delta": """
        SELECT
            g.machine_id,
            g.last_event_time as gold_event_time,
            l.last_event_time as lakebase_event_time,
            ROUND(
              (unix_millis(l.last_event_time) - unix_millis(g.last_event_time)),
              0
            ) AS sync_delta_ms
        FROM welch.iot_demo.gold_machine_latest_status g
        JOIN welch.iot_demo.machine_current_status l
          ON g.machine_id = l.machine_id
        WHERE g.machine_id IN ('MC-0001', 'MC-0010', 'MC-0050')
    """,
}


def run_query(label, sql):
    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql.strip(),
        "wait_timeout": "50s",
    })
    result = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", payload],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        print(f"\n{'=' * 60}")
        print(f"  {label}")
        print(f"{'=' * 60}")
        print(f"  CLI error (rc={result.returncode}): {result.stderr[:300]}")
        return
    resp = json.loads(result.stdout)
    state = resp.get("status", {}).get("state", "?")
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"{'=' * 60}")
    if state == "SUCCEEDED":
        cols = [c["name"] for c in resp["manifest"]["schema"]["columns"]]
        rows = resp["result"]["data_array"]
        for row in rows:
            for c, v in zip(cols, row):
                print(f"  {c:>35s} : {v}")
            if len(rows) > 1:
                print("  ---")
    else:
        err = resp.get("status", {}).get("error", {}).get("message", "?")
        print(f"  ERROR: {state} - {err[:300]}")


if __name__ == "__main__":
    for label, sql in QUERIES.items():
        run_query(label, sql)
