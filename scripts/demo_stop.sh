#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

cancel_job_resource iothub_to_zerobus_autorun
stop_pipeline_resource iot_telemetry_medallion
cancel_job_resource iot_pipeline_keepalive
cancel_job_resource iot_ml_realtime_scoring
cancel_job_resource iot_demo_realtime_workflow

echo "stop phase complete."
