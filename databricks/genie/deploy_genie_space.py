"""
Create or update the Manufacturing Command Center Genie space.

Uses the serialized_space API (version 2) for atomic updates:
- Rich text instructions with KPI definitions and query behavior rules
- Tables and metric views with descriptions
- De-duplicated sample questions
- Example SQL queries
- Join specs, SQL snippets, and benchmark questions

Usage (local):
  python deploy_genie_space.py \
    --catalog welch --schema iot_demo \
    --warehouse-id 148ccb90800933a1 \
    --genie-space-id 01f1337e71ab1bdd8cb044b8576e5073

Usage (Databricks notebook):
  Runs via spark/dbutils context automatically.
"""

import argparse
import json
import os
import uuid
from typing import Optional

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy enhanced Genie Space for IoT demo.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--space-name", default="Manufacturing Command Center")
    parser.add_argument(
        "--space-description",
        default=(
            "IoT manufacturing operations assistant. Ask about machine health, "
            "OEE, fault risk predictions, anomaly detection, downtime analysis, "
            "sensor trends, pipeline latency, and service requests across a 100+ machine fleet."
        ),
    )
    parser.add_argument("--genie-space-id", default="")
    return parser.parse_args()


_ID_COUNTER = 0

def _id() -> str:
    """Generate a monotonically increasing hex ID so all arrays are pre-sorted."""
    global _ID_COUNTER
    _ID_COUNTER += 1
    return f"{_ID_COUNTER:032x}"


def _get_auth(args: argparse.Namespace):
    """Resolve workspace URL and auth token from environment or notebook context."""
    host = os.getenv("DATABRICKS_HOST", "")
    token = os.getenv("DATABRICKS_TOKEN", "")

    if not host or not token:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            host = f"https://{w.config.host}" if not w.config.host.startswith("http") else w.config.host
            auth_headers = w.config.authenticate()
            token = (auth_headers or {}).get("Authorization", "").replace("Bearer ", "")
        except Exception:
            pass

    if not host or not token:
        try:
            host = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        except Exception:
            pass

    if not host or not token:
        raise RuntimeError("Cannot resolve workspace URL and auth token")

    if not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/"), token


def build_serialized_space(catalog: str, schema: str) -> dict:
    """Build the complete serialized_space version-2 payload."""
    fqn = f"{catalog}.{schema}"

    # ─── Text Instructions ────────────────────────────────────────────
    instructions_text = f"""\
You are an operations analytics assistant for an IoT manufacturing predictive maintenance system.
You have access to curated tables and metric views in catalog '{catalog}', schema '{schema}'.

═══ DATA MODEL ═══

PRIMARY TABLES (use these first):
• vw_machine_current_status — Latest snapshot per machine: sensor readings, OEE, multi-horizon fault predictions (5m/1h/24h/7d), anomaly scores, pipeline freshness.
• vw_machine_health — 5-minute windowed health aggregates: OEE components, downtime breakdown, anomaly score, fault predictions. Use for trending.
• vw_machine_telemetry_live — Rolling 24-hour event-level telemetry. Use for time-series and trend queries.
• silver_machine_telemetry — Full event-level streaming telemetry with device-to-hub and hub-to-bridge latency. Use for deep latency analysis.
• dim_machine — Maps machine_id to line_name. Always join for user-friendly names.

METRIC VIEWS (prefer for KPI aggregations):
• mv_machine_oee — OEE, availability, performance, quality by machine and time window.
• mv_machine_downtime — Run, stopped, fault seconds and downtime percentage.
• mv_machine_risk — Anomaly score, multi-horizon fault risk, high-risk window counts.
• mv_machine_telemetry — Sensor averages (temp, vibration, throughput, load, power, pressure, flow).
• mv_machine_freshness — Telemetry and ML lag metrics.
• mv_machine_current — Current-state sensor readings, OEE, fault risk.

═══ KPI DEFINITIONS ═══

OEE (Overall Equipment Effectiveness):
  oee_pct = availability_pct × performance_pct × quality_pct / 10000
  Use oee_pct directly from vw_machine_health or vw_machine_current_status.

Availability = time_in_run_s / (time_in_run_s + time_in_stopped_s + time_in_fault_s) × 100
Performance = avg_throughput_cpm / 100 (target 100 CPM), capped at 100%
Quality = (1 − fault_rate) × 100
Downtime = time_in_stopped_s + time_in_fault_s (seconds per 5-minute window)

═══ FAULT RISK LEVELS ═══

prob_fault_next_5m: probability of FAULT within 5 minutes
  ≥ 0.8 → CRITICAL (immediate attention)
  ≥ 0.5 → WATCH (elevated risk)
  < 0.5 → NORMAL

Additional horizons: prob_fault_next_1h, prob_fault_next_24h, prob_fault_next_7d

Anomaly score: anomaly_score ≥ 0.7 → anomaly detected (unless user specifies different threshold)

═══ SENSOR THRESHOLDS ═══

vibration_mm_s: normal < 8, warning 8–9.5, fault ≥ 9.5
temp_c: normal < 75°C, warning 75–85°C, fault ≥ 85°C
throughput_cpm: normal 60–120, degraded < 30, stopped = 0
rpm: normal 1200–2800, overspeed > 2800, stopped = 0
current_amps: normal 3–10A, warning 10–12A, fault ≥ 12A
humidity_pct: normal 30–60%, elevated > 70%
power_kw, voltage_v, pressure_bar, flow_rate_lpm: continuous measures

═══ PIPELINE LATENCY ═══

telemetry_lag_ms: end-to-end time from device event to availability in the view (milliseconds)
ml_lag_ms: time from event to ML scoring completion
device_to_hub_ms: latency from device to Azure IoT Hub (silver_machine_telemetry only)
hub_to_bridge_ms: latency from IoT Hub to Zerobus bridge (silver_machine_telemetry only)

═══ SERVICE REQUESTS ═══

Service requests (maintenance work orders) are managed in the Streamlit app and stored in Lakebase (PostgreSQL).
They are NOT queryable from SQL here. If asked about service requests, explain that they are tracked
in the Manufacturing Command Center application and include fields: machine_id, priority (CRITICAL/HIGH/MEDIUM/LOW),
request_type (PREVENTIVE/CORRECTIVE/INSPECTION), status (OPEN/IN_PROGRESS/RESOLVED/CLOSED), and description.
Direct the user to the Streamlit app for service request details.

═══ FAULT CODES ═══

OVERTEMP, VIBRATION, OVERCURRENT, BEARING_WEAR, MANUAL_FAULT, F_OVERHEAT, F_VIBRATION, NONE

═══ QUERY RULES ═══

1. Prefer metric views (mv_machine_*) for aggregations and KPI questions.
2. Always include machine_id in results. LEFT JOIN dim_machine for line_name.
3. For "current" or "right now" questions, filter to latest rows by last_event_time or MAX(window_end).
4. Never reference bronze_iot_telemetry unless explicitly asked.
5. Round percentages to 1 decimal. Round sensor values to 2 decimals. Round lag to 0 decimals.
6. When any machine has prob_fault_next_5m > 0.8, always flag it as CRITICAL.
7. Use INTERVAL syntax for time ranges: INTERVAL 1 HOUR, INTERVAL 6 HOURS, INTERVAL 1 DAY.
8. Machine IDs follow pattern MC-NNNN (e.g., MC-0000, MC-0001, ..., MC-0099).

═══ CLARIFICATION TRIGGERS ═══

If user asks about machine performance without specifying a time range, ask:
  "What time range? For example: last hour, last 6 hours, or today."

If user asks about a specific machine without specifying which metric, ask:
  "Which metrics? For example: OEE, vibration, temperature, fault risk, or full status."

═══ SUMMARY FORMAT ═══

• Always state the time range covered.
• Use bullet points for multi-part summaries.
• Cite the table/view used.
• Flag any machine with prob_fault_next_5m > 0.8 prominently.
• For fleet summaries, include counts: running, stopped, faulted."""

    # ─── Sample Questions ─────────────────────────────────────────────
    sample_questions = [
        "Show me the current status of all machines with their OEE and fault risk",
        "Which machines are currently in FAULT state and what are their fault codes?",
        "Give me a fleet-wide operations summary: machines running, stopped, faulted, average OEE",
        "Which production line has the worst performance right now?",
        "Which machines are most likely to fault in the next 5 minutes?",
        "Show all machines with critical fault risk (above 80%) across any time horizon",
        "Compare 5-minute, 1-hour, and 24-hour fault predictions for MC-0000",
        "Which machines have anomaly scores above 0.7 and what are their sensor readings?",
        "Show OEE breakdown (availability, performance, quality) for all machines",
        "How much total downtime has each production line accumulated today?",
        "Which machines have the lowest OEE and what is driving the gap?",
        "Show the OEE trend over the last 6 hours for the worst-performing machine",
        "Show vibration and temperature trends for MC-0000 over the last hour",
        "Which machines have vibration above warning threshold (8 mm/s)?",
        "Compare motor current draw across all running machines",
        "Show power consumption trend by production line for the last 4 hours",
        "How fresh is the telemetry data for each machine?",
        "Show end-to-end latency: device-to-hub and hub-to-bridge for all machines",
        "Which machines have the highest telemetry lag right now?",
        "What is the average and maximum pipeline latency across the fleet?",
    ]

    # ─── Example SQL ──────────────────────────────────────────────────
    example_sqls = [
        {
            "question": "Example: Which machines are most likely to fault in the next 5 minutes?",
            "sql": f"""\
SELECT s.machine_id, d.line_name,
  ROUND(s.prob_fault_next_5m, 3) AS fault_risk_5m,
  ROUND(s.prob_fault_next_1h, 3) AS fault_risk_1h,
  ROUND(s.anomaly_score, 3) AS anomaly_score,
  s.state, s.fault_code,
  ROUND(s.telemetry_lag_ms, 0) AS lag_ms
FROM {fqn}.vw_machine_current_status s
LEFT JOIN {fqn}.dim_machine d ON s.machine_id = d.machine_id
ORDER BY s.prob_fault_next_5m DESC
LIMIT 10""",
        },
        {
            "question": "Example: Show downtime breakdown by production line today",
            "sql": f"""\
SELECT d.line_name,
  COUNT(DISTINCT h.machine_id) AS machines,
  ROUND(SUM(h.time_in_fault_s), 0) AS fault_downtime_s,
  ROUND(SUM(h.time_in_stopped_s), 0) AS planned_stop_s,
  ROUND(SUM(h.time_in_stopped_s + h.time_in_fault_s), 0) AS total_downtime_s,
  ROUND(AVG(h.oee_pct), 1) AS avg_oee_pct
FROM {fqn}.vw_machine_health h
LEFT JOIN {fqn}.dim_machine d ON h.machine_id = d.machine_id
WHERE h.window_end >= current_timestamp() - INTERVAL 1 DAY
GROUP BY d.line_name
ORDER BY total_downtime_s DESC""",
        },
        {
            "question": "Example: Show OEE breakdown for all machines",
            "sql": f"""\
SELECT s.machine_id, d.line_name,
  ROUND(s.oee_pct, 1) AS oee_pct,
  ROUND(s.availability_pct, 1) AS availability_pct,
  ROUND(s.performance_pct, 1) AS performance_pct,
  ROUND(s.quality_pct, 1) AS quality_pct,
  s.state
FROM {fqn}.vw_machine_current_status s
LEFT JOIN {fqn}.dim_machine d ON s.machine_id = d.machine_id
ORDER BY s.oee_pct ASC""",
        },
        {
            "question": "Example: Show vibration and temperature trends for a machine",
            "sql": f"""\
SELECT event_time, machine_id,
  ROUND(vibration_mm_s, 2) AS vibration_mm_s,
  ROUND(temp_c, 2) AS temp_c,
  ROUND(current_amps, 2) AS current_amps,
  rpm, state
FROM {fqn}.vw_machine_telemetry_live
WHERE machine_id = 'MC-0000'
  AND event_time >= current_timestamp() - INTERVAL 1 HOUR
ORDER BY event_time DESC""",
        },
        {
            "question": "Example: Which machines have anomaly score above threshold?",
            "sql": f"""\
SELECT s.machine_id, d.line_name,
  ROUND(s.anomaly_score, 3) AS anomaly_score,
  ROUND(s.prob_fault_next_5m, 3) AS fault_risk_5m,
  s.state, s.fault_code,
  ROUND(s.vibration_mm_s, 2) AS vibration,
  ROUND(s.temp_c, 1) AS temp_c
FROM {fqn}.vw_machine_current_status s
LEFT JOIN {fqn}.dim_machine d ON s.machine_id = d.machine_id
WHERE s.anomaly_score >= 0.7
ORDER BY s.anomaly_score DESC""",
        },
        {
            "question": "Example: How fresh is the telemetry data?",
            "sql": f"""\
SELECT s.machine_id, d.line_name,
  ROUND(s.telemetry_lag_ms, 0) AS telemetry_lag_ms,
  ROUND(s.ml_lag_ms, 0) AS ml_lag_ms,
  s.last_event_time, s.last_ml_score_time, s.state
FROM {fqn}.vw_machine_current_status s
LEFT JOIN {fqn}.dim_machine d ON s.machine_id = d.machine_id
ORDER BY s.telemetry_lag_ms DESC""",
        },
        {
            "question": "Example: Fleet-wide current status of all machines",
            "sql": f"""\
SELECT s.machine_id, d.line_name, s.state,
  ROUND(s.temp_c, 1) AS temp_c,
  ROUND(s.vibration_mm_s, 2) AS vibration,
  s.rpm,
  ROUND(s.current_amps, 1) AS amps,
  ROUND(s.oee_pct, 1) AS oee_pct,
  ROUND(s.prob_fault_next_5m, 2) AS risk_5m,
  CASE
    WHEN s.prob_fault_next_5m >= 0.8 THEN 'CRITICAL'
    WHEN s.prob_fault_next_5m >= 0.5 THEN 'WATCH'
    ELSE 'NORMAL'
  END AS risk_band
FROM {fqn}.vw_machine_current_status s
LEFT JOIN {fqn}.dim_machine d ON s.machine_id = d.machine_id
ORDER BY s.prob_fault_next_5m DESC""",
        },
        {
            "question": "Example: Compare multi-horizon fault predictions for a machine",
            "sql": f"""\
SELECT machine_id,
  ROUND(prob_fault_next_5m, 3) AS risk_5m,
  ROUND(prob_fault_next_1h, 3) AS risk_1h,
  ROUND(prob_fault_next_24h, 3) AS risk_24h,
  ROUND(prob_fault_next_7d, 3) AS risk_7d,
  ROUND(anomaly_score, 3) AS anomaly,
  state, fault_code
FROM {fqn}.vw_machine_current_status
WHERE machine_id = 'MC-0000'""",
        },
        {
            "question": "Example: Fleet operations summary",
            "sql": f"""\
SELECT
  COUNT(*) AS total_machines,
  SUM(CASE WHEN state = 'RUN' THEN 1 ELSE 0 END) AS running,
  SUM(CASE WHEN state = 'STOPPED' THEN 1 ELSE 0 END) AS stopped,
  SUM(CASE WHEN state = 'FAULT' THEN 1 ELSE 0 END) AS faulted,
  ROUND(AVG(oee_pct), 1) AS avg_oee_pct,
  ROUND(AVG(prob_fault_next_5m), 3) AS avg_fault_risk,
  SUM(CASE WHEN prob_fault_next_5m >= 0.8 THEN 1 ELSE 0 END) AS critical_machines,
  SUM(CASE WHEN anomaly_score >= 0.7 THEN 1 ELSE 0 END) AS anomaly_machines
FROM {fqn}.vw_machine_current_status""",
        },
        {
            "question": "Example: End-to-end pipeline latency analysis",
            "sql": f"""\
SELECT machine_id,
  ROUND(AVG(device_to_hub_ms), 0) AS avg_device_to_hub_ms,
  ROUND(AVG(hub_to_bridge_ms), 0) AS avg_hub_to_bridge_ms,
  ROUND(AVG(device_to_hub_ms + hub_to_bridge_ms), 0) AS avg_total_latency_ms,
  ROUND(MAX(device_to_hub_ms), 0) AS max_device_to_hub_ms,
  ROUND(MAX(hub_to_bridge_ms), 0) AS max_hub_to_bridge_ms,
  COUNT(*) AS event_count
FROM {fqn}.silver_machine_telemetry
WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR
  AND device_to_hub_ms IS NOT NULL
GROUP BY machine_id
ORDER BY avg_total_latency_ms DESC
LIMIT 20""",
        },
    ]

    # ─── SQL Snippets ─────────────────────────────────────────────────
    sql_snippets = {
        "filters": [
            {"id": _id(), "sql": ["prob_fault_next_5m >= 0.5"], "display_name": "high risk machines"},
            {"id": _id(), "sql": ["prob_fault_next_5m >= 0.8"], "display_name": "critical risk machines"},
            {"id": _id(), "sql": ["anomaly_score >= 0.7"], "display_name": "anomaly detected"},
            {"id": _id(), "sql": ["state = 'RUN'"], "display_name": "running machines"},
            {"id": _id(), "sql": ["state = 'FAULT'"], "display_name": "faulted machines"},
            {"id": _id(), "sql": ["event_time >= current_timestamp() - INTERVAL 1 HOUR"], "display_name": "last hour"},
            {"id": _id(), "sql": ["event_time >= current_timestamp() - INTERVAL 1 DAY"], "display_name": "today"},
        ],
        "measures": [
            {"id": _id(), "alias": "total_downtime_s", "sql": ["SUM(time_in_stopped_s + time_in_fault_s)"]},
            {"id": _id(), "alias": "avg_oee", "sql": ["ROUND(AVG(oee_pct), 1)"]},
            {"id": _id(), "alias": "fleet_risk", "sql": ["ROUND(AVG(prob_fault_next_5m), 3)"]},
            {"id": _id(), "alias": "machine_count", "sql": ["COUNT(DISTINCT machine_id)"]},
        ],
        "expressions": [
            {
                "id": _id(),
                "alias": "risk_band",
                "sql": [
                    "CASE WHEN prob_fault_next_5m >= 0.8 THEN 'CRITICAL' "
                    "WHEN prob_fault_next_5m >= 0.5 THEN 'WATCH' "
                    "ELSE 'NORMAL' END"
                ],
            },
            {
                "id": _id(),
                "alias": "vibration_status",
                "sql": [
                    "CASE WHEN vibration_mm_s >= 9.5 THEN 'FAULT' "
                    "WHEN vibration_mm_s >= 8 THEN 'WARNING' "
                    "ELSE 'NORMAL' END"
                ],
            },
        ],
    }

    # ─── Benchmark Questions ──────────────────────────────────────────
    benchmarks = {
        "questions": [
            {
                "id": _id(),
                "question": ["How many machines are currently running?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT COUNT(*) AS running_machines FROM {fqn}.vw_machine_current_status WHERE state = 'RUN'"
                ]}],
            },
            {
                "id": _id(),
                "question": ["What is the average OEE across the fleet?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT ROUND(AVG(oee_pct), 1) AS avg_oee_pct FROM {fqn}.vw_machine_current_status"
                ]}],
            },
            {
                "id": _id(),
                "question": ["Which machine has the highest fault risk right now?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT machine_id, ROUND(prob_fault_next_5m, 3) AS fault_risk "
                    f"FROM {fqn}.vw_machine_current_status ORDER BY prob_fault_next_5m DESC LIMIT 1"
                ]}],
            },
            {
                "id": _id(),
                "question": ["Show machines with vibration above 8 mm/s"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT machine_id, ROUND(vibration_mm_s, 2) AS vibration, state "
                    f"FROM {fqn}.vw_machine_current_status WHERE vibration_mm_s >= 8 "
                    f"ORDER BY vibration_mm_s DESC"
                ]}],
            },
            {
                "id": _id(),
                "question": ["What is the average telemetry lag in milliseconds?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT ROUND(AVG(telemetry_lag_ms), 0) AS avg_lag_ms, "
                    f"ROUND(MAX(telemetry_lag_ms), 0) AS max_lag_ms "
                    f"FROM {fqn}.vw_machine_current_status"
                ]}],
            },
        ]
    }

    # ─── Assemble serialized_space ────────────────────────────────────
    tables = sorted([
        {
            "identifier": f"{fqn}.vw_machine_current_status",
            "description": [
                "Latest snapshot per machine with current sensor readings, OEE, "
                "multi-horizon fault predictions (5m/1h/24h/7d), anomaly scores, "
                "and pipeline freshness metrics (telemetry_lag_ms, ml_lag_ms)."
            ],
        },
        {
            "identifier": f"{fqn}.vw_machine_health",
            "description": [
                "5-minute windowed health aggregates per machine: OEE components "
                "(availability, performance, quality), downtime breakdown "
                "(time_in_run_s, time_in_stopped_s, time_in_fault_s), anomaly score, "
                "and multi-horizon fault predictions. Use for trending and historical."
            ],
        },
        {
            "identifier": f"{fqn}.vw_machine_telemetry_live",
            "description": [
                "Rolling 24-hour window of event-level telemetry: vibration, temperature, "
                "throughput, RPM, current, humidity, load, power, voltage, pressure, flow. "
                "Use for recent time-series trend queries."
            ],
        },
        {
            "identifier": f"{fqn}.silver_machine_telemetry",
            "description": [
                "Full event-level streaming telemetry with all sensor readings plus "
                "device_to_hub_ms and hub_to_bridge_ms latency columns. Use for deep "
                "latency analysis and historical trend queries beyond the 24-hour live window."
            ],
        },
        {
            "identifier": f"{fqn}.dim_machine",
            "description": [
                "Machine dimension table mapping machine_id to line_name. "
                "Always LEFT JOIN to fact tables for user-friendly production line names."
            ],
        },
    ], key=lambda t: t["identifier"])

    metric_views = sorted([
        {
            "identifier": f"{fqn}.mv_machine_oee",
            "description": [
                "OEE, availability, performance, and quality metrics aggregated "
                "by machine and time window. Prefer for OEE KPI questions."
            ],
        },
        {
            "identifier": f"{fqn}.mv_machine_downtime",
            "description": [
                "Downtime breakdown: run seconds, stopped seconds, fault seconds, "
                "total downtime seconds, and downtime percentage by machine and time."
            ],
        },
        {
            "identifier": f"{fqn}.mv_machine_risk",
            "description": [
                "Risk metrics: average anomaly score, multi-horizon fault risk "
                "(5m/1h/24h/7d), high-risk window counts, and anomaly window counts."
            ],
        },
        {
            "identifier": f"{fqn}.mv_machine_telemetry",
            "description": [
                "Sensor telemetry aggregations: average temperature, vibration, "
                "throughput, load, power, pressure, and flow by machine and time."
            ],
        },
        {
            "identifier": f"{fqn}.mv_machine_freshness",
            "description": [
                "Pipeline freshness metrics: average and max telemetry lag and "
                "ML lag in both seconds and milliseconds. Use for data freshness questions."
            ],
        },
        {
            "identifier": f"{fqn}.mv_machine_current",
            "description": [
                "Current machine state metrics: real-time sensor readings, OEE, "
                "and multi-horizon fault risk. Prefer for current-state KPI summaries."
            ],
        },
    ], key=lambda m: m["identifier"])

    return {
        "version": 2,
        "config": {
            "sample_questions": [
                {"id": _id(), "question": [q]}
                for q in sample_questions
            ],
        },
        "data_sources": {
            "tables": tables,
            "metric_views": metric_views,
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": _id(),
                    "content": instructions_text.split("\n"),
                },
            ],
            "example_question_sqls": [
                {
                    "id": _id(),
                    "question": [eq["question"]],
                    "sql": eq["sql"].split("\n"),
                }
                for eq in example_sqls
            ],
            "sql_snippets": sql_snippets,
        },
        "benchmarks": benchmarks,
    }


def deploy(
    base_url: str,
    token: str,
    space_id: str,
    title: str,
    description: str,
    warehouse_id: str,
    serialized_space: dict,
) -> None:
    """PATCH the Genie space with the complete serialized_space payload."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "title": title,
        "description": description,
        "warehouse_id": warehouse_id,
        "serialized_space": json.dumps(serialized_space, indent=2),
    }
    url = f"{base_url}/api/2.0/genie/spaces/{space_id}"
    print(f"PATCHing Genie space {space_id} ...")
    resp = requests.patch(url, headers=headers, json=payload, timeout=120)
    if not resp.ok:
        raise RuntimeError(f"PATCH failed ({resp.status_code}): {resp.text[:3000]}")
    result = resp.json()
    print(f"Genie space updated: {result.get('space_id', space_id)}")
    print(f"Title: {result.get('title', title)}")
    print(f"URL: {base_url}/genie/rooms/{space_id}")


def main() -> None:
    args = parse_args()
    base_url, token = _get_auth(args)

    space_id = args.genie_space_id.strip()
    if space_id in {"__AUTO__", "AUTO", "NONE", ""}:
        raise ValueError(
            "A --genie-space-id is required for PATCH updates. "
            "Provide the existing space ID to update."
        )

    serialized = build_serialized_space(args.catalog, args.schema)

    sq_count = len(serialized["config"]["sample_questions"])
    eq_count = len(serialized["instructions"]["example_question_sqls"])
    tbl_count = len(serialized["data_sources"]["tables"])
    mv_count = len(serialized["data_sources"]["metric_views"])
    bm_count = len(serialized["benchmarks"]["questions"])
    snip_count = sum(
        len(v) for v in serialized["instructions"]["sql_snippets"].values()
    )

    print(f"Built serialized_space v2:")
    print(f"  Tables: {tbl_count}  |  Metric Views: {mv_count}")
    print(f"  Sample Questions: {sq_count}  |  Example SQL: {eq_count}")
    print(f"  SQL Snippets: {snip_count}  |  Benchmarks: {bm_count}")
    print(f"  Text Instructions: 1 (comprehensive)")

    deploy(
        base_url=base_url,
        token=token,
        space_id=space_id,
        title=args.space_name,
        description=args.space_description,
        warehouse_id=args.warehouse_id,
        serialized_space=serialized,
    )


if __name__ == "__main__":
    main()
