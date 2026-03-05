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
   - Receives device telemetry (`iotdev-0000` physical + `iotdev-0001..0100` virtual).
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
- `databricks/ingestion/lakeflow_zerobus_config.json`: Zerobus connector configuration template.
- `databricks/pipelines/dlt_pipeline.py`: Bronze/Silver/Gold DLT pipeline.
- `databricks/ml/ml_anomaly_notebook.py`: anomaly scoring training/output script.
- `databricks/ml/ml_state_prediction_notebook.py`: fault prediction training/output script.
- `databricks/semantic/sql_views.sql`: curated SQL semantic layer.
- `databricks/dashboard/manufacturing_command_center_dashboard.sql`: AI/BI dashboard query pack.
- `databricks/genie/genie_manufacturing_command_center.md`: Genie Space instruction and prompt pack.
- `databricks/dashboard/dashboard_notes.md`: dashboard implementation guidance.
- `databricks/genie/genie_space_notes.md`: Genie scope and instruction guidance.
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
2. Copy the credentials template and fill your values:

```bash
cp arduino/secrets.example.h arduino/secrets.h
```

3. Edit `arduino/secrets.h`:
   - `WIFI_SSID`, `WIFI_PASSWORD`
   - `IOT_HUB_HOST`
   - `DEVICE_ID` (default `iotdev-0000`)
   - `MACHINE_ID` (default `MC-0000`)
   - `SAS_TOKEN`
4. Upload `arduino/machine_panel.ino` to the board.
5. Open Serial Monitor at `115200` to verify:
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
  --resource-uri "iothub-zerobus-demo-welch.azure-devices.net/devices/iotdev-0000" \
  --device-key "<device-primary-key>" \
  --ttl-seconds 28800
```

4. Export environment variables and run sender:

```bash
export SERIAL_PORT="/dev/cu.usbmodem101"
export BAUD_RATE="115200"
export IOT_HUB_NAME="iothub-zerobus-demo-welch"
export DEVICE_ID="iotdev-0000"
export SAS_TOKEN="<paste generated SharedAccessSignature token>"
export MACHINE_ID="MC-0000"
python sender.py
```

Both modes should publish the same payload fields:
`machine_id`, `vibration_mm_s`, `temp_c`, `throughput_cpm`, `state`, `fault_code`, `ts`.

## Scale Testing and Synthetic Training Data

For demo-scale simulation and repeatable fault training datasets, use:

- `edge-python/simulate_fleet_iothub.py --mode stream`: publish live telemetry for many virtual devices to Azure IoT Hub.
- `edge-python/simulate_fleet_iothub.py --mode export-training`: generate threshold-driven training data from the same scenario engine.
- `edge-python/generate_fault_training_data.py`: temporary compatibility wrapper (deprecated).

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

Use Azure CLI to reset and reprovision all demo identities (1 physical + 100 virtual):

```bash
TARGET=dev IOTHUB_NAME="iothub-zerobus-demo-welch" scripts/demo_reset_devices.sh
```

This writes:
- `edge-python/devices.json` for virtual fleet simulation (`iotdev-0001..0100` -> `MC-0001..0100`)
- `edge-python/arduino_device.json` for the physical device (`iotdev-0000` -> `MC-0000`)

If you only need virtual create/reuse without deleting all identities:

```bash
cd edge-python
python autoprovision_iothub_devices.py \
  --iothub-name "iothub-zerobus-demo-welch" \
  --count 100 \
  --device-prefix "iotdev" \
  --machine-prefix "MC" \
  --padding 4 \
  --output-file "devices.json"
```

### 1b) Regenerate Arduino `secrets.h` with deterministic device identity

```bash
cd edge-python
python generate_arduino_secrets.py \
  --iothub-host "iothub-zerobus-demo-welch.azure-devices.net" \
  --device-id "iotdev-0000" \
  --machine-id "MC-0000" \
  --device-key "<physical-device-primary-key>" \
  --wifi-ssid "<wifi-ssid>" \
  --wifi-password "<wifi-password>" \
  --ttl-seconds 28800 \
  --output-file "../arduino/secrets.h"
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
python simulate_fleet_iothub.py \
  --mode export-training \
  --num-devices 100 \
  --target-total-records 10000 \
  --sample-interval-seconds 5 \
  --output-jsonl "../data/synthetic_fault_training.jsonl" \
  --output-csv "../data/synthetic_fault_training.csv"
```

The generated rows include `threshold_crossed` and `label_fault_next_5m` to support supervised predictive-maintenance experiments.

## Azure Prerequisites

Follow `infra/azure_iot_hub_setup.md` to:

- Create IoT Hub.
- Register device `iotdev-0000`.
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

### Slack alerting status

Slack alerting is currently disabled and archived under `z_archive/slack/`.

## Deploy via Databricks Asset Bundles (DABs)

From repo root:

```bash
databricks bundle validate -t dev

# Optional: provision autoscaling Lakebase and emit metadata contract
scripts/provision_lakebase_autoscaling.sh

# Deploy with cadence mode (demo=1 minute, steady=5 minutes)
TARGET=dev MODE=demo scripts/deploy_with_cadence.sh
```

### Databricks App (Streamlit)

This repo includes a Databricks App resource (`resources/apps.yml`) and Streamlit code under `app/`.

App capabilities:
- Narrative landing page (IoT manufacturing story + citations)
- Embedded AI/BI dashboard
- Flow-break risk command center over UC views
- Native Genie chat surface
- Lakebase live operational table reads

Deploy with bundle:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

The deployed app resource is named `iot-flowbreak-app-dev` for the `dev` target.

Runtime configuration is controlled by app environment variables (for example):
- `APP_CATALOG` / `APP_SCHEMA`
- `DATABRICKS_WAREHOUSE_ID`
- `APP_DASHBOARD_URL`
- `APP_GENIE_SPACE_ID`
- `LAKEBASE_DB_HOST` / `LAKEBASE_DB_PORT` / `LAKEBASE_DB_NAME` / `LAKEBASE_DB_USER` / `LAKEBASE_DB_PASSWORD`

Demo runbook:
- `docs/app_demo_flow.md`

`scripts/deploy_with_cadence.sh` handles cadence deployment for ingest and post-medallion jobs.

1) Start post-medallion refresh chain (ML + semantic/metric views + Lakebase mirror + dashboard refresh task):

```bash
databricks bundle run -t dev iot_ml_realtime_scoring
```

### Demo control commands (`go`, `generate`, `stop`)

Use scripted commands for deterministic demo execution:

```bash
# Physical device only (MC-0000)
TARGET=dev MACHINE_ID=MC-0000 scripts/demo_go.sh

# Add virtual fleet (~10k records by default)
TARGET=dev scripts/demo_generate.sh

# Stop continuous ingest/DLT and queued runs
TARGET=dev scripts/demo_stop.sh
```

2) Always-on ingest + medallion behavior (recommended for live demo):

- `iothub_to_zerobus_autorun_${bundle.target}` is now a long-running continuous bridge job (`--run-mode continuous`) from IoT Hub into raw input table storage.
- `iot_telemetry_medallion_${bundle.target}` is configured with `continuous: true` and processes new data while active.
- `iot_ml_realtime_scoring_${bundle.target}` is an on-demand post-medallion chain and no longer runs on a separate cron schedule.

Use `go` to ensure both continuous services are running before the talk track:

```bash
TARGET=dev MACHINE_ID=MC-0000 scripts/demo_go.sh
```

Troubleshooting:
- If you suspect duplicate ingest, run `scripts/demo_stop.sh` once, then `scripts/demo_go.sh` once (do not manually start multiple bridge runs).
- Keep the bridge checkpoint path stable to avoid replay/duplication surprises during a demo.
- For intentional backfill, run the bridge in one-shot mode (`--run-mode available-now --starting-offsets earliest`) outside the live talk track.

The post-medallion job includes medallion sync, batch+realtime ML scoring, semantic view refresh, UC metric view refresh, Lakebase mirror + parity validation, and dashboard refresh checks.

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
   - `databricks/dashboard/manufacturing_command_center_dashboard.sql`
2. Create/update Genie Space named `Manufacturing Command Center` via:
   - `databricks/genie/deploy_genie_space.py`
   - `databricks/genie/genie_manufacturing_command_center.md` (context reference)
3. Include only curated views in Genie:
   - `vw_machine_telemetry_live`
   - `vw_machine_health`
   - `vw_machine_current_status`
   - `dim_machine`

## Verification Checklist

1. Direct mode connectivity
   - Uno joins hotspot/WiFi and prints local IP.
   - MQTT connects to IoT Hub and publishes on `devices/iotdev-0000/messages/events/`.
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
   - Risk mix is non-flat for demo windows (expect WATCH/CRITICAL rows, not all NORMAL):
     ```sql
     SELECT
       CASE
         WHEN prob_fault_next_5m >= 0.8 THEN 'CRITICAL'
         WHEN prob_fault_next_5m >= 0.5 THEN 'WATCH'
         ELSE 'NORMAL'
       END AS risk_band,
       COUNT(*) AS machine_count
     FROM welch.iot_demo_dev.vw_machine_current_status
     WHERE last_event_time >= current_timestamp() - INTERVAL 10 MINUTES
     GROUP BY 1
     ORDER BY 1;
     ```
   - Fault code diversity exists in recent telemetry:
     ```sql
     SELECT COALESCE(fault_code, 'NONE') AS fault_code, COUNT(*) AS records
     FROM welch.iot_demo_dev.silver_machine_features
     WHERE ts >= current_timestamp() - INTERVAL 10 MINUTES
     GROUP BY 1
     ORDER BY records DESC
     LIMIT 10;
     ```
7. SQL semantic layer
   - `vw_machine_health` shows anomaly + predicted fault columns.
8. Business story interaction
   - Pot changes affect trends.
   - RUN/STOP and FAULT button actions change state and throughput as expected.
9. Slack alerting
   - Disabled by design in the current code path.

## Demo-Day Switching Checklist

1. Start in direct Uno WiFi mode.
2. If direct publish drops, keep Arduino running and start fallback Python sender.
3. Reuse the same `DEVICE_ID` and payload schema to avoid downstream changes.
4. Continue dashboard/Genie demo without changing Databricks pipeline assets.

### Demo-day quick recovery (`MC-0000`)

- If `MC-0000` shows `STOPPED` and should be running:
  1. Press the RUN/STOP button once on the Arduino.
  2. Run `TARGET=dev MACHINE_ID=MC-0000 scripts/demo_go.sh`.
  3. Refresh dashboard and verify `state = RUN` in `vw_machine_current_status`.
- If you need to prove fault response:
  - Raise temperature/vibration pots above threshold.
  - Confirm `FAULT` appears in telemetry and `prob_fault_next_5m` rises after realtime ML refresh.

## Demo SLO Targets

- Telemetry visible in dashboard: `< 30-60s` after bridge run.
- Fleet average `telemetry_lag_ms`: `< 60000`.
- Fleet average `ml_lag_ms`: `< 90000` after realtime scoring.

## Notes

- Azure provisioning remains prerequisite/manual; Databricks runtime assets are DAB-managed.
- Keep hotspot credentials and SAS tokens out of source control.
