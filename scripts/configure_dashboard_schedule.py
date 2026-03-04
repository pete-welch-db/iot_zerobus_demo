import argparse
import json
import subprocess
import sys
from typing import Dict, Optional


def run_cli_json(cmd):
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed ({' '.join(cmd)}): {result.stderr.strip()}")
    return json.loads(result.stdout or "{}")


def find_dashboard_id(display_name: str) -> Optional[str]:
    page_token = None
    for _ in range(20):
        endpoint = "/api/2.0/lakeview/dashboards"
        if page_token:
            endpoint = f"{endpoint}?page_token={page_token}"
        payload = run_cli_json(["databricks", "api", "get", endpoint])
        dashboards = payload.get("dashboards", [])
        for d in dashboards:
            if d.get("display_name") == display_name:
                return d.get("dashboard_id")
        page_token = payload.get("next_page_token")
        if not page_token:
            break
    return None


def try_schedule_update(dashboard_id: str, interval_seconds: int) -> bool:
    body: Dict = {
        "schedule": {
            "pause_status": "UNPAUSED",
            "timezone_id": "UTC",
            "interval_seconds": interval_seconds,
        }
    }
    attempts = [
        f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedule",
        f"/api/2.0/lakeview/dashboards/{dashboard_id}/schedules/default",
    ]
    for endpoint in attempts:
        cmd = ["databricks", "api", "put", endpoint, "--json", json.dumps(body)]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        if result.returncode == 0:
            return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Configure Lakeview dashboard schedule interval.")
    parser.add_argument("--dashboard-name", default="Manufacturing Command Center v2")
    parser.add_argument("--dashboard-id", default="")
    parser.add_argument("--interval-seconds", type=int, required=True)
    args = parser.parse_args()

    dashboard_id = args.dashboard_id.strip() or find_dashboard_id(args.dashboard_name)
    if not dashboard_id:
        raise RuntimeError(f"Could not find dashboard: {args.dashboard_name}")
    if not try_schedule_update(dashboard_id, args.interval_seconds):
        raise RuntimeError(
            f"Failed to update schedule for dashboard {dashboard_id}. "
            "Lakeview API endpoint may require manual update in the UI."
        )
    print(f"Dashboard schedule updated: dashboard_id={dashboard_id} interval_seconds={args.interval_seconds}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
