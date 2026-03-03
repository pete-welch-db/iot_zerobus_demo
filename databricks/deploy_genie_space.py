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
        "Show downtime for each machine in the last 60 minutes.",
        "Which machines currently have anomaly score above 0.7?",
        "What is the latest OEE for MACH_A?",
        "Show OEE by machine for the last 6 windows using metric view measures.",
        "Compare availability, performance, and quality for all machines right now.",
        "Show trend of vibration and temperature for the last hour for MACH_A.",
        "Which line has the highest average fault risk today?",
        "Which machine has the highest ML lag right now?",
    ]

    genie_instructions = f"""You are an operations analytics assistant for an IoT manufacturing demo.
You have access to curated objects in catalog '{catalog}' and schema '{schema}' only.

Definitions and behavior:
- "risk" or "likely to fail" means prob_fault_next_5m.
- "anomaly" means anomaly_score, with threshold >= 0.7 unless user specifies otherwise.
- "downtime" means time_in_stopped_s + time_in_fault_s.
- "OEE" means oee_pct from vw_machine_health or vw_machine_current_status.
- Prefer Unity Catalog metric views (mv_machine_*) for KPI aggregations when possible.
- For "current" questions, default to latest last_event_time/window_end rows.
- Include machine_id in answers, and use dim_machine.line_name when relevant.
- For freshness questions, use telemetry_lag_seconds and ml_lag_seconds.
- Never reference raw/bronze tables unless explicitly requested.
"""

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

    print(f"GENIE_SPACE_ID={space_id}")
    print(f"GENIE_SPACE_URL=https://{host}/genie/rooms/{space_id}")


if __name__ == "__main__":
    main()
