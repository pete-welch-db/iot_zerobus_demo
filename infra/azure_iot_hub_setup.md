# Azure IoT Hub Setup (Databricks Zerobus Demo)

This guide provisions the minimum Azure prerequisites for the demo flow:

Primary path:

Arduino Uno WiFi Rev2 -> Azure IoT Hub -> built-in Event Hubs-compatible endpoint -> Databricks

Fallback path:

Arduino (serial) -> Mac Python sender -> Azure IoT Hub -> built-in Event Hubs-compatible endpoint -> Databricks

## 1) Create IoT Hub

1. In Azure Portal, create an IoT Hub in the same or nearest region to your Databricks workspace.
2. Suggested naming:
   - Resource Group: `rg-iot-zerobus-demo`
  - IoT Hub: `iothub-zerobus-demo-welch`
3. Use Standard tier appropriate for expected message volume.

## 2) Register Device

1. In IoT Hub, navigate to `Devices`.
2. Create device:
   - Device ID: `arduino-panel`
   - Authentication type: symmetric key.
3. Copy primary key (used to generate SAS token for edge sender).

## 3) Record Built-in Event Hubs-Compatible Endpoint

From IoT Hub `Built-in endpoints`, collect:

- Event Hubs-compatible endpoint:
  - Format: `sb://<hub>-namespace.servicebus.windows.net/`
- Event Hubs-compatible path:
  - Usually the IoT Hub name.
- Shared access policy key for read access:
  - Prefer a least-privilege policy for consumer access.

Also create/confirm consumer group:

- `zerobus-lakeflow`

## 4) Connection String Template for Zerobus/Lakeflow

Use the built-in endpoint, not a standalone Event Hubs namespace:

```text
Endpoint=sb://<event-hubs-compatible-endpoint>/;SharedAccessKeyName=<policy-name>;SharedAccessKey=<policy-key>;EntityPath=<event-hubs-compatible-path>
```

## 5) Generate Device SAS Token for MQTT Sender

Both paths publish to IoT Hub over MQTT with device SAS authentication.

Resource URI format for a device:

```text
iothub-zerobus-demo-welch.azure-devices.net/devices/arduino-panel
```

Generate token with helper script:

```bash
cd edge-python
python generate_sas_token.py \
  --resource-uri "iothub-zerobus-demo-welch.azure-devices.net/devices/arduino-panel" \
  --device-key "<device-primary-key>" \
  --ttl-seconds 28800
```

Use the generated token in either:

- `edge-python/sender.py` as `SAS_TOKEN` environment variable.
- `arduino/machine_panel.ino` as the `SAS_TOKEN` constant for direct device publish.
- Keep generated SAS tokens and device keys out of source control.

## 6) Uno WiFi Rev2 Direct-MQTT Settings

For direct publish from Uno WiFi Rev2, set these in firmware:

```text
WIFI_SSID=<phone_hotspot_name>
WIFI_PASSWORD=<phone_hotspot_password>
IOT_HUB_HOST=iothub-zerobus-demo-welch.azure-devices.net
DEVICE_ID=arduino-panel
SAS_TOKEN=SharedAccessSignature sr=...&sig=...&se=...
MQTT_PORT=8883
MQTT_TOPIC=devices/arduino-panel/messages/events/
MQTT_USERNAME=<iothub-host>/<device-id>/?api-version=2021-04-12
```

Notes:

- For demos, use a SAS token TTL long enough for the session (for example 4-8 hours) to avoid expiring mid-demo.
- When the token expires, regenerate and flash updated firmware or move temporarily to fallback Python mode.
- If you later generate SAS tokens on-device, add NTP time sync before signing tokens.

## 7) Required Sender Environment Variables (Fallback Python Mode)

```text
IOT_HUB_NAME=<iothub-name-without-domain>
DEVICE_ID=arduino-panel
SAS_TOKEN=<SharedAccessSignature ...>
SERIAL_PORT=/dev/cu.usbmodem101
BAUD_RATE=115200
MACHINE_ID=MACH_A
```

Optional:

```text
TLS_INSECURE=true   # demo-only if local cert chain issues occur
```

## 8) Security and Operational Guardrails

- Use short-lived SAS tokens and rotate regularly.
- Keep hotspot credentials, SAS tokens, and keys out of source control.
- Prefer Azure Key Vault or secret manager for shared/demo environments.
- Keep Databricks and IoT Hub regions close to minimize end-to-end latency.
- For simulator manifests, use local-only files (for example `devices.local.json`) and never commit keys.
