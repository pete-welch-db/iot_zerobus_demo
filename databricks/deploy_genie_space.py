"""
Create or update the Manufacturing Command Center Genie space.

Uses Databricks Genie Data Rooms REST API:
- create/update space
- add/update sample questions
- add canonical instruction text
"""

import argparse
import re
from typing import Dict, Iterable, List, Optional

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update Genie Space for IoT demo.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--space-name", default="Manufacturing Command Center")
    parser.add_argument("--space-description", default="Genie assistant for IoT operations and predictive maintenance.")
    parser.add_argument("--genie-space-id", default="")
    return parser.parse_args()


def normalize_title(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def is_target_space(candidate_title: str, target_title: str) -> bool:
    return normalize_title(candidate_title) == normalize_title(target_title)


def iter_spaces_via_sdk() -> Iterable[Dict[str, str]]:
    from databricks.sdk import WorkspaceClient

    workspace = WorkspaceClient()
    next_page_token = None
    seen_tokens = set()

    while True:
        kwargs = {"page_size": 100}
        if next_page_token:
            kwargs["page_token"] = next_page_token
        response = workspace.genie.list_spaces(**kwargs)
        for space in (response.spaces or []):
            yield {"space_id": space.space_id, "title": space.title or ""}
        next_page_token = getattr(response, "next_page_token", None)
        if not next_page_token or next_page_token in seen_tokens:
            break
        seen_tokens.add(next_page_token)


def iter_spaces_via_rest(base_url: str, headers: Dict[str, str]) -> Iterable[Dict[str, str]]:
    next_page_token = None
    seen_tokens = set()

    while True:
        params = {"page_size": 100}
        if next_page_token:
            params["page_token"] = next_page_token
        response = requests.get(f"{base_url}/api/2.0/data-rooms", headers=headers, params=params, timeout=60)
        if not response.ok:
            break
        payload = response.json()
        spaces = payload.get("spaces") or payload.get("data_rooms") or payload.get("dataRooms") or []
        for space in spaces:
            yield {
                "space_id": space.get("space_id", space.get("id")),
                "title": space.get("display_name", space.get("title", space.get("name", ""))),
            }
        next_page_token = payload.get("next_page_token") or payload.get("nextPageToken")
        if not next_page_token or next_page_token in seen_tokens:
            break
        seen_tokens.add(next_page_token)


def fetch_existing_questions(base_url: str, headers: Dict[str, str], space_id: str) -> List[str]:
    response = requests.get(
        f"{base_url}/api/2.0/data-rooms/{space_id}/curated-questions",
        headers=headers,
        timeout=60,
    )
    if not response.ok:
        return []
    payload = response.json()
    questions = payload.get("curated_questions") or payload.get("questions") or payload.get("items") or []
    return [q.get("question_text", q.get("text", "")).strip() for q in questions if q.get("question_text", q.get("text", "")).strip()]


def fetch_existing_instruction_titles(base_url: str, headers: Dict[str, str], space_id: str) -> List[str]:
    response = requests.get(
        f"{base_url}/api/2.0/data-rooms/{space_id}/instructions",
        headers=headers,
        timeout=60,
    )
    if not response.ok:
        return []
    payload = response.json()
    instructions = payload.get("instructions") or payload.get("items") or []
    return [i.get("title", "").strip() for i in instructions if i.get("title", "").strip()]


def main() -> None:
    args = parse_args()
    catalog = args.catalog
    schema = args.schema
    warehouse_id = args.warehouse_id
    target_space_name = args.space_name
    space_description = args.space_description
    space_id_override = args.genie_space_id.strip()
    if space_id_override in {"__AUTO__", "AUTO", "NONE"}:
        space_id_override = ""

    host = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    base_url = f"https://{host}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    table_identifiers = [
        f"{catalog}.{schema}.mv_machine_telemetry",
        f"{catalog}.{schema}.mv_machine_oee",
        f"{catalog}.{schema}.mv_machine_downtime",
        f"{catalog}.{schema}.mv_machine_risk",
        f"{catalog}.{schema}.mv_machine_freshness",
        f"{catalog}.{schema}.mv_machine_current",
        f"{catalog}.{schema}.vw_machine_telemetry_live",
        f"{catalog}.{schema}.vw_machine_health",
        f"{catalog}.{schema}.vw_machine_current_status",
        f"{catalog}.{schema}.dim_machine",
    ]

    sample_questions = [
        "Which machine is most likely to fault in the next 5 minutes?",
        "Show machines at risk of faulting right now",
        "Any machines about to break down?",
        "Show downtime for each machine in the last 60 minutes.",
        "How much downtime has each line had today?",
        "Which machines currently have anomaly score above 0.7?",
        "Show anomalies across all lines",
        "What is the latest OEE for MACH_A?",
        "Show overall equipment effectiveness for all machines",
        "How efficient are the machines?",
        "Compare availability, performance, and quality for all machines right now.",
        "Show trend of vibration and temperature for the last hour for MACH_A.",
        "Show RPM and motor current trends for the last hour",
        "Which line has the highest average fault risk today?",
        "Which machine has the highest ML lag right now?",
        "How fresh is the telemetry data?",
        "Show me the current status of all machines",
        "Which machines are currently in FAULT state?",
        "What is the average motor current across all running machines?",
        "Show humidity trends for the last 2 hours",
    ]

    genie_instructions = f"""You are an operations analytics assistant for an IoT-based manufacturing predictive maintenance system.
You have access to curated tables and metric views in catalog '{catalog}' and schema '{schema}' only.

KPI Definitions (use these exact formulas):
- OEE (Overall Equipment Effectiveness) = availability_pct * performance_pct * quality_pct / 10000, expressed as a percentage. Use oee_pct from vw_machine_health or vw_machine_current_status.
- Availability = time_in_run_s / (time_in_run_s + time_in_stopped_s + time_in_fault_s), expressed as a percentage.
- Performance = avg_throughput_cpm / target_throughput_cpm (target is 100 CPM), capped at 100%.
- Quality = 1 - fault_rate, expressed as a percentage.
- Downtime = time_in_stopped_s + time_in_fault_s (in seconds within each 5-minute window).
- Anomaly Score = anomaly_score from ML model output, threshold >= 0.7 indicates anomaly unless user specifies otherwise.
- Fault Risk = prob_fault_next_5m, the probability a machine will enter FAULT state within the next 5 minutes. Values above 0.5 are high risk, above 0.8 are critical.
- Telemetry Freshness = telemetry_lag_seconds, the time since the last telemetry reading was received.
- ML Freshness = ml_lag_seconds, the time since the last ML scoring run completed.

Telemetry fields:
- vibration_mm_s: vibration in mm/s (normal < 8, warning 8-9.5, fault >= 9.5)
- temp_c: temperature in Celsius (normal < 75, warning 75-85, fault >= 85)
- throughput_cpm: parts per minute (normal 60-120, degraded < 30, stopped = 0)
- rpm: motor rotational speed (normal 1200-2800, overspeed > 2800, stopped = 0)
- current_amps: motor current draw in amps (normal 3-10, warning 10-12, fault >= 12)
- humidity_pct: ambient humidity percentage (normal 30-60, elevated > 70)

Fault codes: OVERTEMP, VIBRATION, OVERCURRENT, BEARING_WEAR, MANUAL_FAULT, F_OVERHEAT, F_VIBRATION

Query behavior:
- Prefer Unity Catalog metric views (mv_machine_*) for KPI aggregations when possible.
- For "current" or "right now" questions, use the latest rows by last_event_time or window_end.
- Always include machine_id in results. Use dim_machine.line_name for user-friendly names when available.
- Never reference raw or bronze tables unless explicitly requested.
- Round percentages to 1 decimal place. Round sensor values to 2 decimal places.
- When a machine shows prob_fault_next_5m > 0.8, highlight it as CRITICAL RISK in the summary.

When users ask about machine performance without specifying a time range, ask: "What time range should I analyze? For example: last hour, last 6 hours, or today."
When users ask about a specific machine without specifying which metric, ask: "Which metrics are you interested in? For example: OEE, vibration, temperature, fault risk, or current status."

Instructions you must follow when providing summaries:
- Include the time range covered in the results.
- Use bullet points to structure multi-part summaries.
- Cite the table or view name used in the analysis.
- If any machine has prob_fault_next_5m > 0.8, always mention it prominently.
"""

    example_sql_queries = [
        {
            "question": "Which machine is most likely to fault in the next 5 minutes?",
            "sql": f"""SELECT s.machine_id, d.line_name, s.prob_fault_next_5m, s.anomaly_score, s.state, s.telemetry_lag_seconds
FROM {catalog}.{schema}.vw_machine_current_status s
LEFT JOIN {catalog}.{schema}.dim_machine d ON s.machine_id = d.machine_id
ORDER BY s.prob_fault_next_5m DESC
LIMIT 5""",
        },
        {
            "question": "Show downtime for each machine in the last 60 minutes",
            "sql": f"""SELECT h.machine_id, d.line_name,
  ROUND(SUM(h.time_in_stopped_s + h.time_in_fault_s), 0) AS total_downtime_s,
  ROUND(SUM(h.time_in_fault_s), 0) AS fault_downtime_s,
  ROUND(SUM(h.time_in_stopped_s), 0) AS planned_downtime_s
FROM {catalog}.{schema}.vw_machine_health h
LEFT JOIN {catalog}.{schema}.dim_machine d ON h.machine_id = d.machine_id
WHERE h.window_end >= current_timestamp() - INTERVAL 60 MINUTES
GROUP BY h.machine_id, d.line_name
ORDER BY total_downtime_s DESC""",
        },
        {
            "question": "What is the OEE for all machines?",
            "sql": f"""SELECT s.machine_id, d.line_name,
  ROUND(s.oee_pct, 1) AS oee_pct,
  ROUND(s.availability_pct, 1) AS availability_pct,
  ROUND(s.performance_pct, 1) AS performance_pct,
  ROUND(s.quality_pct, 1) AS quality_pct
FROM {catalog}.{schema}.vw_machine_current_status s
LEFT JOIN {catalog}.{schema}.dim_machine d ON s.machine_id = d.machine_id
ORDER BY s.oee_pct ASC""",
        },
        {
            "question": "Show vibration and temperature trends for a machine",
            "sql": f"""SELECT event_time, machine_id,
  ROUND(vibration_mm_s, 2) AS vibration_mm_s,
  ROUND(temp_c, 2) AS temp_c,
  ROUND(current_amps, 2) AS current_amps,
  rpm, state
FROM {catalog}.{schema}.vw_machine_telemetry_live
WHERE machine_id = 'MACH_A'
ORDER BY event_time DESC
LIMIT 200""",
        },
        {
            "question": "Which machines have anomaly score above threshold?",
            "sql": f"""SELECT s.machine_id, d.line_name,
  ROUND(s.anomaly_score, 3) AS anomaly_score,
  ROUND(s.prob_fault_next_5m, 3) AS fault_risk,
  s.state
FROM {catalog}.{schema}.vw_machine_current_status s
LEFT JOIN {catalog}.{schema}.dim_machine d ON s.machine_id = d.machine_id
WHERE s.anomaly_score >= 0.7
ORDER BY s.anomaly_score DESC""",
        },
        {
            "question": "How fresh is the data?",
            "sql": f"""SELECT machine_id,
  ROUND(telemetry_lag_seconds, 0) AS telemetry_lag_s,
  ROUND(ml_lag_seconds, 0) AS ml_lag_s,
  last_event_time, last_ml_score_time
FROM {catalog}.{schema}.vw_machine_current_status
ORDER BY telemetry_lag_seconds DESC""",
        },
        {
            "question": "Show current status of all machines",
            "sql": f"""SELECT s.machine_id, d.line_name, s.state,
  ROUND(s.temp_c, 1) AS temp_c,
  ROUND(s.vibration_mm_s, 2) AS vibration_mm_s,
  s.rpm, ROUND(s.current_amps, 1) AS current_amps,
  ROUND(s.humidity_pct, 0) AS humidity_pct,
  ROUND(s.oee_pct, 1) AS oee_pct,
  ROUND(s.prob_fault_next_5m, 2) AS fault_risk
FROM {catalog}.{schema}.vw_machine_current_status s
LEFT JOIN {catalog}.{schema}.dim_machine d ON s.machine_id = d.machine_id
ORDER BY s.machine_id""",
        },
        {
            "question": "What is the average motor current across running machines?",
            "sql": f"""SELECT
  COUNT(*) AS running_machines,
  ROUND(AVG(current_amps), 2) AS avg_current_amps,
  ROUND(MAX(current_amps), 2) AS max_current_amps,
  ROUND(AVG(rpm), 0) AS avg_rpm
FROM {catalog}.{schema}.vw_machine_current_status
WHERE state = 'RUN'""",
        },
    ]

    space_id: Optional[str] = None
    if space_id_override:
        space_id = space_id_override
        print(f"Using provided Genie space ID: {space_id}")
    else:
        try:
            for space in iter_spaces_via_sdk():
                if is_target_space(space.get("title", ""), target_space_name):
                    space_id = space.get("space_id")
                    print(f"Found existing Genie space via SDK: {space_id}")
                    break
        except Exception as sdk_error:
            print(f"SDK list failed: {sdk_error}")

        if not space_id:
            for space in iter_spaces_via_rest(base_url, headers):
                if is_target_space(space.get("title", ""), target_space_name):
                    space_id = space.get("space_id")
                    print(f"Found existing Genie space via REST: {space_id}")
                    break

    if space_id:
        payload = {
            "id": space_id,
            "display_name": target_space_name,
            "description": space_description,
            "warehouse_id": warehouse_id,
            "table_identifiers": table_identifiers,
            "run_as_type": "VIEWER",
        }
        response = requests.patch(
            f"{base_url}/api/2.0/data-rooms/{space_id}",
            headers=headers,
            json=payload,
            timeout=60,
        )
        if not response.ok:
            raise RuntimeError(f"Failed to update Genie space {space_id}: {response.text[:2000]}")
        print(f"Updated Genie space: {space_id}")
    else:
        payload = {
            "display_name": target_space_name,
            "warehouse_id": warehouse_id,
            "table_identifiers": table_identifiers,
            "description": space_description,
            "run_as_type": "VIEWER",
        }
        response = requests.post(
            f"{base_url}/api/2.0/data-rooms/",
            headers=headers,
            json=payload,
            timeout=60,
        )
        if not response.ok:
            raise RuntimeError(f"Failed to create Genie space: {response.text[:2000]}")
        result = response.json()
        space_id = result.get("space_id", result.get("id"))
        if not space_id:
            raise RuntimeError(f"Create succeeded but no space_id returned: {result}")
        print(f"Created Genie space: {space_id}")

    existing_questions = set(fetch_existing_questions(base_url, headers, space_id))
    questions_to_add = [q for q in sample_questions if q not in existing_questions]
    if questions_to_add:
        actions = [
            {
                "action_type": "CREATE",
                "curated_question": {
                    "data_room_id": space_id,
                    "question_text": question,
                    "question_type": "SAMPLE_QUESTION",
                },
            }
            for question in questions_to_add
        ]
        response = requests.post(
            f"{base_url}/api/2.0/data-rooms/{space_id}/curated-questions/batch-actions",
            headers=headers,
            json={"actions": actions},
            timeout=60,
        )
        if not response.ok:
            raise RuntimeError(f"Failed to add sample questions: {response.text[:1000]}")
        print(f"Added {len(questions_to_add)} sample questions.")
    else:
        print("No new sample questions to add.")

    instruction_title = "Manufacturing Command Center Context and KPI Definitions"
    existing_instruction_titles = set(fetch_existing_instruction_titles(base_url, headers, space_id))
    if instruction_title not in existing_instruction_titles:
        response = requests.post(
            f"{base_url}/api/2.0/data-rooms/{space_id}/instructions",
            headers=headers,
            json={
                "title": instruction_title,
                "content": genie_instructions,
                "instruction_type": "TEXT_INSTRUCTION",
            },
            timeout=60,
        )
        if not response.ok:
            raise RuntimeError(f"Failed to add instruction: {response.text[:1000]}")
        print("Added Genie instruction.")
    else:
        print("Instruction already exists; skipping.")

    for eq in example_sql_queries:
        eq_title = f"Example: {eq['question']}"
        if eq_title not in existing_instruction_titles:
            response = requests.post(
                f"{base_url}/api/2.0/data-rooms/{space_id}/instructions",
                headers=headers,
                json={
                    "title": eq_title,
                    "content": eq["sql"],
                    "instruction_type": "EXAMPLE_SQL",
                },
                timeout=60,
            )
            if response.ok:
                print(f"Added example SQL: {eq_title}")
            else:
                print(f"Failed to add example SQL '{eq_title}': {response.text[:500]}")

    print(f"GENIE_SPACE_ID={space_id}")
    print(f"GENIE_SPACE_URL=https://{host}/genie/rooms/{space_id}")


if __name__ == "__main__":
    main()
