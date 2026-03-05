#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

cancel_job_resource iothub_to_zerobus_autorun
stop_pipeline_resource iot_telemetry_medallion
cancel_job_resource iot_zerobus_orchestration

echo "stop phase complete."
