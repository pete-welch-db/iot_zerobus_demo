#!/usr/bin/env bash
# Shared helper functions for IoT Zerobus demo scripts.

TARGET="${TARGET:-dev}"
WAREHOUSE_ID="${WAREHOUSE_ID:-148ccb90800933a1}"
CATALOG="${CATALOG:-welch}"
SCHEMA="${SCHEMA:-iot_demo_dev}"

get_job_id() {
  local resource_key="$1"
  databricks bundle summary -t "$TARGET" -o json | python3 -c '
import json, re, sys
resource_key = sys.argv[1]
data = json.load(sys.stdin)
job = ((data.get("resources") or {}).get("jobs") or {}).get(resource_key) or {}
job_id = str(job.get("id") or "")
if job_id:
    print(job_id)
    raise SystemExit(0)
url = str(job.get("url") or "")
m = re.search(r"/jobs/(\d+)", url)
if m:
    print(m.group(1))
' "$resource_key"
}

get_pipeline_id() {
  local resource_key="$1"
  databricks bundle summary -t "$TARGET" -o json | python3 -c '
import json, re, sys
resource_key = sys.argv[1]
data = json.load(sys.stdin)
pipeline = ((data.get("resources") or {}).get("pipelines") or {}).get(resource_key) or {}
pipeline_id = str(pipeline.get("id") or "")
if pipeline_id:
    print(pipeline_id)
    raise SystemExit(0)
url = str(pipeline.get("url") or "")
m = re.search(r"/pipelines/([0-9a-f-]+)", url)
if m:
    print(m.group(1))
' "$resource_key"
}

job_has_active_run() {
  local job_id="$1"
  databricks jobs list-runs --job-id "$job_id" --active-only --limit 1 --output json 2>/dev/null | \
    python3 -c 'import json,sys; d=json.load(sys.stdin); runs=d if isinstance(d,list) else (d.get("runs") or []); print("true" if runs else "false")'
}

pipeline_is_running() {
  local pipeline_id="$1"
  databricks pipelines get "$pipeline_id" --output json 2>/dev/null | \
    python3 -c 'import json,sys
raw=sys.stdin.read().strip()
if not raw:
    print("false")
    raise SystemExit(0)
try:
    d=json.loads(raw)
except Exception:
    print("false")
    raise SystemExit(0)
print("true" if isinstance(d,dict) and d.get("state")=="RUNNING" else "false")'
}

cancel_job_resource() {
  local key="$1"
  local job_id
  job_id="$(get_job_id "$key" || true)"
  if [[ -z "$job_id" ]]; then
    echo "Skipping $key (job id not found)"
    return
  fi
  echo "Cancelling all runs for $key ($job_id)"
  databricks jobs cancel-all-runs --job-id "$job_id" --all-queued-runs >/dev/null || true
}

stop_pipeline_resource() {
  local key="$1"
  local pipeline_id
  pipeline_id="$(get_pipeline_id "$key" || true)"
  if [[ -z "$pipeline_id" ]]; then
    echo "Skipping pipeline $key (pipeline id not found)"
    return
  fi
  echo "Stopping pipeline $key ($pipeline_id)"
  databricks pipelines stop "$pipeline_id" >/dev/null || true
}

sql_query() {
  local statement="$1"
  databricks api post /api/2.0/sql/statements --json "$(cat <<EOF
{"warehouse_id":"$WAREHOUSE_ID","statement":"$statement"}
EOF
)"
}
