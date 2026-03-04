#!/usr/bin/env bash
set -euo pipefail

IOTHUB_NAME="${IOTHUB_NAME:-iothub-zerobus-demo-welch}"
VIRTUAL_COUNT="${VIRTUAL_COUNT:-100}"
PHYSICAL_DEVICE_ID="${PHYSICAL_DEVICE_ID:-iotdev-0000}"
PHYSICAL_MACHINE_ID="${PHYSICAL_MACHINE_ID:-MC-0000}"
DEVICES_FILE="${DEVICES_FILE:-edge-python/devices.json}"
PHYSICAL_FILE="${PHYSICAL_FILE:-edge-python/arduino_device.json}"
DELETE_ALL_EXISTING="${DELETE_ALL_EXISTING:-true}"

PYTHON_BIN=".venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

echo "==> Resetting IoT Hub identities and reprovisioning fleet"
CMD=(
  "$PYTHON_BIN" edge-python/reset_and_reprovision_iothub.py
  --iothub-name "$IOTHUB_NAME"
  --virtual-count "$VIRTUAL_COUNT"
  --physical-device-id "$PHYSICAL_DEVICE_ID"
  --physical-machine-id "$PHYSICAL_MACHINE_ID"
  --output-virtual-devices-file "$DEVICES_FILE"
  --output-physical-device-file "$PHYSICAL_FILE"
)
if [[ "$DELETE_ALL_EXISTING" == "true" ]]; then
  CMD+=(--delete-all-existing)
fi
"${CMD[@]}"

echo "==> Virtual fleet manifest: $DEVICES_FILE"
echo "==> Physical device metadata: $PHYSICAL_FILE"
echo "reset devices phase complete."
