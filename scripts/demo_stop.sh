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

# Order matters: stop ingest stream first, then DLT, then any batch workflows.
cancel_job_resource iothub_to_zerobus_autorun
stop_pipeline_resource iot_telemetry_medallion
cancel_job_resource iot_pipeline_keepalive
cancel_job_resource iot_ml_realtime_scoring
cancel_job_resource iot_demo_realtime_workflow

echo "stop phase complete."
