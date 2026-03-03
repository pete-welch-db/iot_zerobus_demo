#!/usr/bin/env bash
# Shared helper functions for IoT Zerobus demo scripts.

TARGET="${TARGET:-dev}"
WAREHOUSE_ID="${WAREHOUSE_ID:-148ccb90800933a1}"
CATALOG="${CATALOG:-welch}"
SCHEMA="${SCHEMA:-iot_demo_dev}"

get_job_id() {
  local resource_key="$1"
  databricks bundle summary -t "$TARGET" | awk -v key="$resource_key" '
    $1 == key ":" { in_key = 1; next }
    in_key && $1 == "URL:" {
      if (match($2, /jobs\/([0-9]+)/, arr)) {
        print arr[1]
        exit
      }
    }
    in_key && NF == 0 { in_key = 0 }
  '
}

get_pipeline_id() {
  local resource_key="$1"
  databricks bundle summary -t "$TARGET" | awk -v key="$resource_key" '
    $1 == key ":" { in_key = 1; next }
    in_key && $1 == "URL:" {
      if (match($2, /pipelines\/([0-9a-f-]+)/, arr)) {
        print arr[1]
        exit
      }
    }
    in_key && NF == 0 { in_key = 0 }
  '
}

job_has_active_run() {
  local job_id="$1"
  databricks jobs list-runs --job-id "$job_id" --active-only --limit 1 --output json 2>/dev/null | \
    python3 -c 'import json,sys; d=json.load(sys.stdin); print("true" if (d.get("runs") or []) else "false")'
}

pipeline_is_running() {
  local pipeline_id="$1"
  databricks pipelines get --pipeline-id "$pipeline_id" --output json 2>/dev/null | \
    python3 -c 'import json,sys; d=json.load(sys.stdin); print("true" if d.get("state") == "RUNNING" else "false")'
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
  databricks pipelines stop --pipeline-id "$pipeline_id" >/dev/null || true
}

sql_query() {
  local statement="$1"
  databricks api post /api/2.0/sql/statements --json "$(cat <<EOF
{"warehouse_id":"$WAREHOUSE_ID","statement":"$statement"}
EOF
)"
}
