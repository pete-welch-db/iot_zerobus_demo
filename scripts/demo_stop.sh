#!/usr/bin/env bash
set -euo pipefail

TARGET="${TARGET:-dev}"

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

cancel_resource() {
  local key="$1"
  local job_id
  job_id="$(get_job_id "$key" || true)"
  if [[ -n "$job_id" ]]; then
    echo "Cancelling all runs for $key ($job_id)"
    databricks jobs cancel-all-runs --job-id "$job_id" --all-queued-runs >/dev/null
  else
    echo "Skipping $key (job id not found)"
  fi
}

cancel_resource iothub_to_zerobus_autorun
cancel_resource iot_pipeline_keepalive
cancel_resource iot_ml_realtime_scoring
cancel_resource iot_demo_realtime_workflow

echo "stop phase complete."
