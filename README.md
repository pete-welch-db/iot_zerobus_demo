# iot_zerobus_demo

Arduino-to-Databricks predictive maintenance and OEE demo using Azure IoT Hub, Zerobus/Lakeflow ingestion, Delta medallion tables, ML scoring, and SQL/Genie analytics.

## Architecture

1. Arduino Uno WiFi Rev2 (primary mode):
   - Connects to hotspot/WiFi using WiFiNINA.
   - Publishes telemetry JSON directly to Azure IoT Hub over MQTT/TLS.
   - Still emits serial CSV for local debug and fallback support.
2. Arduino serial fallback mode:
   - Same Arduino sketch emits serial CSV.
   - Mac Python sender republishes to Azure IoT Hub over MQTT.
3. Azure IoT Hub:
   - Receives device telemetry (`arduino-panel`).
   - Exposes built-in Event Hubs-compatible endpoint.
4. Databricks:
   - Zerobus/Lakeflow connector ingests raw messages.
   - DLT pipeline builds Bronze/Silver/Gold Delta tables.
   - ML scripts score anomaly and fault probability.
   - SQL views power dashboards and Genie.

## Edge Operating Modes

### Mode A (Primary): Direct Uno WiFi -> IoT Hub

- Board: Arduino Uno WiFi Rev2
- Firmware: `arduino/machine_panel.ino`
- Libraries: `WiFiNINA`, `PubSubClient`
- Uses MQTT/TLS on `8883` with IoT Hub device SAS authentication.
- Telemetry JSON contract matches downstream Databricks expectations.

### Mode B (Fallback): Uno Serial -> Python -> IoT Hub

- Keep the same Arduino firmware serial output enabled.
- Run `edge-python/sender.py` to parse CSV and publish same JSON contract.
- Use this when hotspot quality drops or when rapidly rotating SAS tokens.

## Repository Layout

- `requirements.md`: functional and deployment requirements.
- `arduino/machine_panel.ino`: Arduino firmware for telemetry, direct IoT Hub MQTT publish, and serial fallback.
- `edge-python/requirements.txt`: edge dependencies.
- `edge-python/sender.py`: serial -> IoT Hub sender fallback.
- `edge-python/generate_sas_token.py`: helper for device SAS tokens.
- `infra/azure_iot_hub_setup.md`: Azure setup steps and endpoint guidance.
- `databricks/lakeflow_zerobus_config.json`: Zerobus connector configuration template.
- `databricks/dlt_pipeline.py`: Bronze/Silver/Gold DLT pipeline.
- `databricks/ml_anomaly_notebook.py`: anomaly scoring training/output script.
- `databricks/ml_state_prediction_notebook.py`: fault prediction training/output script.
- `databricks/sql_views.sql`: curated SQL semantic layer.
- `databricks/manufacturing_command_center_dashboard.sql`: AI/BI dashboard query pack.
- `databricks/genie_manufacturing_command_center.md`: Genie Space instruction and prompt pack.
- `databricks/dashboard_notes.md`: dashboard implementation guidance.
- `databricks/genie_space_notes.md`: Genie scope and instruction guidance.
- `databricks.yml`: Databricks Asset Bundle root configuration.
- `resources/pipelines.yml`: DLT pipeline resource.
- `resources/jobs.yml`: pipeline refresh + ML scoring jobs.

## Hardware Wiring (Arduino)

- Pots:
  - `A0` vibration
  - `A1` temperature
  - `A2` throughput
- Buttons (active-low, `INPUT_PULLUP`):
  - `D2` toggle `RUN`/`STOPPED`
  - `D3` toggle `FAULT` on/off
- Optional LEDs:
  - `D10` RUN
  - `D11` FAULT

Serial CSV format every ~1 second:

`vibration,temp,throughput,state,faultCode`

## Edge Setup (Primary Direct Mode)

1. In Arduino IDE, install libraries:
   - WiFiNINA
   - PubSubClient
2. Update config constants in `arduino/machine_panel.ino`:
   - `WIFI_SSID`, `WIFI_PASSWORD`
   - `IOT_HUB_HOST`
   - `DEVICE_ID` (default `arduino-panel`)
   - `SAS_TOKEN`
3. Upload `arduino/machine_panel.ino` to the board.
4. Open Serial Monitor at `115200` to verify:
   - WiFi connection and IP
   - MQTT connect success
   - CSV and publish behavior

## Edge Setup (Fallback Python Mode)

1. Keep Arduino sketch running (serial output is always enabled).
2. Install Python dependencies:

```bash
cd edge-python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Generate SAS token:

```bash
python generate_sas_token.py \
  --resource-uri "iothub-zerobus-demo-welch.azure-devices.net/devices/arduino-panel" \
  --device-key "<device-primary-key>" \
  --ttl-seconds 28800
```

4. Export environment variables and run sender:

```bash
export SERIAL_PORT="/dev/cu.usbmodem101"
export BAUD_RATE="115200"
export IOT_HUB_NAME="iothub-zerobus-demo-welch"
export DEVICE_ID="arduino-panel"
export SAS_TOKEN="<paste generated SharedAccessSignature token>"
export MACHINE_ID="MACH_A"
python sender.py
```

Both modes should publish the same payload fields:
`machine_id`, `vibration_mm_s`, `temp_c`, `throughput_cpm`, `state`, `fault_code`, `ts`.

## Scale Testing and Synthetic Training Data

For demo-scale simulation and repeatable fault training datasets, use:

- `edge-python/simulate_fleet_iothub.py`: publish live telemetry for many virtual devices to Azure IoT Hub.
- `edge-python/generate_fault_training_data.py`: generate large synthetic datasets with threshold-driven fault behavior.

### 1) Simulate many devices to IoT Hub

Create a device manifest from `edge-python/devices.example.json` with real IoT Hub device IDs/keys.

```bash
cd edge-python
python simulate_fleet_iothub.py \
  --iothub-name "iothub-zerobus-demo-welch" \
  --devices-file "devices.example.json" \
  --duration-seconds 600 \
  --message-rate-hz 1.0 \
  --fault-period-seconds 180
```

### 1a) Auto-provision IoT Hub devices for simulator

Use Azure CLI to create or reuse device identities and emit a simulator manifest.

```bash
cd edge-python
python autoprovision_iothub_devices.py \
  --iothub-name "iothub-zerobus-demo-welch" \
  --count 250 \
  --device-prefix "sim-device" \
  --machine-prefix "MACH" \
  --padding 4 \
  --output-file "devices.json"
```

Then run:

```bash
python simulate_fleet_iothub.py \
  --iothub-name "iothub-zerobus-demo-welch" \
  --devices-file "devices.json" \
  --duration-seconds 600
```

### 2) Generate synthetic training data (fault ramps)

```bash
cd edge-python
python generate_fault_training_data.py \
  --num-devices 250 \
  --samples-per-device 8000 \
  --sample-interval-seconds 5 \
  --output-jsonl "../data/synthetic_fault_training.jsonl" \
  --output-csv "../data/synthetic_fault_training.csv"
```

The generated rows include `threshold_crossed` and `label_fault_next_5m` to support supervised predictive-maintenance experiments.

## Azure Prerequisites

Follow `infra/azure_iot_hub_setup.md` to:

- Create IoT Hub.
- Register device `arduino-panel`.
- Create consumer group `zerobus-lakeflow`.
- Capture built-in Event Hubs-compatible endpoint/path for Zerobus/Lakeflow.

## Databricks Prerequisites

- Databricks CLI authenticated to target workspace.
- Unity Catalog enabled.
- Serverless enabled for DLT/Jobs/SQL as applicable.
- Zerobus/Lakeflow connection configured to write raw source table:
  - `${catalog}.${schema}.raw_iothub_messages`
- Workspace URL:
  - `https://adb-984752964297111.11.azuredatabricks.net/`
- SQL warehouse endpoint:
  - `/sql/1.0/warehouses/148ccb90800933a1`

### Zerobus Setup Secrets (one-time)

Before running the automated Zerobus setup job, create secret scope and keys:

- Scope: `iot_zerobus_demo` (or update bundle vars)
- Keys:
  - `zerobus_sp_client_id`
  - `zerobus_sp_client_secret`

Example:

```bash
databricks secrets create-scope iot_zerobus_demo
databricks secrets put-secret iot_zerobus_demo zerobus_sp_client_id
databricks secrets put-secret iot_zerobus_demo zerobus_sp_client_secret
```

## Deploy via Databricks Asset Bundles (DABs)

From repo root:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

1) Run one-click end-to-end demo workflow (recommended):

```bash
databricks bundle run -t dev iot_demo_realtime_workflow
```

2) Start continuous near-real-time ML scoring (optional):

```bash
databricks bundle run -t dev iot_ml_realtime_scoring
```

### Demo control commands (`go`, `generate`, `stop`)

Use scripted commands for deterministic demo execution:

```bash
# Physical device only (MACH_A)
TARGET=dev MACHINE_ID=MACH_A scripts/demo_go.sh

# Add virtual fleet
TARGET=dev scripts/demo_generate.sh

# Stop continuous ingest/DLT and queued runs
TARGET=dev scripts/demo_stop.sh
```

3) Always-on ingest + medallion behavior (recommended for live demo):

- `iothub_to_zerobus_autorun_${bundle.target}` is now a long-running continuous bridge job (`--run-mode continuous`) from IoT Hub into raw input table storage.
- `iot_telemetry_medallion_${bundle.target}` is configured with `continuous: true` and processes new data while active.
- `iot_pipeline_keepalive_${bundle.target}` is used by `scripts/demo_go.sh` to start/ensure the DLT pipeline when not running.
- `iot_ml_realtime_scoring_${bundle.target}` remains optional near-live scoring to refresh anomaly/fault outputs.

Use `go` to ensure both continuous services are running before the talk track:

```bash
TARGET=dev MACHINE_ID=MACH_A scripts/demo_go.sh
```

Troubleshooting:
- If you suspect duplicate ingest, run `scripts/demo_stop.sh` once, then `scripts/demo_go.sh` once (do not manually start multiple bridge runs).
- Keep the bridge checkpoint path stable to avoid replay/duplication surprises during a demo.
- For intentional backfill, run the bridge in one-shot mode (`--run-mode available-now --starting-offsets earliest`) outside the live talk track.

The end-to-end workflow includes preflight checks, Zerobus setup, IoT Hub bridge, medallion refresh, batch+realtime ML scoring, semantic view refresh, UC metric view refresh, Genie refresh, and output validation.

## Expected Tables and Views

Tables:

- Bronze: `bronze_iot_raw`
- Silver: `silver_machine_telemetry`
- Gold: `gold_machine_health_5m`
- ML outputs: `ml_anomaly_scores`, `ml_fault_predictions`

Views:

- `vw_machine_telemetry_live`
- `vw_machine_health`
- `dim_machine`
- `vw_machine_current_status`

UC metric views:

- `mv_machine_telemetry`
- `mv_machine_oee`
- `mv_machine_downtime`
- `mv_machine_risk`
- `mv_machine_freshness`
- `mv_machine_current`

## Manufacturing Command Center Dashboard + Genie

1. Build Databricks AI/BI dashboard named `Manufacturing Command Center` using queries in:
   - `databricks/manufacturing_command_center_dashboard.sql`
2. Create/update Genie Space named `Manufacturing Command Center` via:
   - `databricks/deploy_genie_space.py`
   - `databricks/genie_manufacturing_command_center.md` (context reference)
3. Include only curated views in Genie:
   - `vw_machine_telemetry_live`
   - `vw_machine_health`
   - `vw_machine_current_status`
   - `dim_machine`

## Verification Checklist

1. Direct mode connectivity
   - Uno joins hotspot/WiFi and prints local IP.
   - MQTT connects to IoT Hub and publishes on `devices/arduino-panel/messages/events/`.
2. Fallback mode (if needed)
   - Python sender logs valid JSON with `machine_id`, metrics, `state`, `fault_code`, and UTC `ts`.
3. Bronze ingest
   - `bronze_iot_raw` receives records and transport metadata.
4. Silver quality
   - `silver_machine_telemetry` has typed fields and valid states.
5. Gold KPIs
   - `gold_machine_health_5m` updates with availability/performance/quality/OEE.
6. ML outputs
   - `ml_anomaly_scores` and `ml_fault_predictions` populated.
7. SQL semantic layer
   - `vw_machine_health` shows anomaly + predicted fault columns.
8. Business story interaction
   - Pot changes affect trends.
   - RUN/STOP and FAULT button actions change state and throughput as expected.

## Demo-Day Switching Checklist

1. Start in direct Uno WiFi mode.
2. If direct publish drops, keep Arduino running and start fallback Python sender.
3. Reuse the same `DEVICE_ID` and payload schema to avoid downstream changes.
4. Continue dashboard/Genie demo without changing Databricks pipeline assets.

### Demo-day quick recovery (`MACH_A`)

- If `MACH_A` shows `STOPPED` and should be running:
  1. Press the RUN/STOP button once on the Arduino.
  2. Run `TARGET=dev MACHINE_ID=MACH_A scripts/demo_go.sh`.
  3. Refresh dashboard and verify `state = RUN` in `vw_machine_current_status`.
- If you need to prove fault response:
  - Raise temperature/vibration pots above threshold.
  - Confirm `FAULT` appears in telemetry and `prob_fault_next_5m` rises after realtime ML refresh.

## Demo SLO Targets

- Telemetry visible in dashboard: `< 30-60s` after bridge run.
- Fleet average `telemetry_lag_seconds`: `< 60s`.
- Fleet average `ml_lag_seconds`: `< 90s` after realtime scoring.

## Notes

- Azure provisioning remains prerequisite/manual; Databricks runtime assets are DAB-managed.
- Keep hotspot credentials and SAS tokens out of source control.
