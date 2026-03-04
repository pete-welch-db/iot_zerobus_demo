# Arduino to Databricks IoT Demo Requirements

## Business Context

This demo represents a manufacturing predictive-maintenance scenario where an Arduino-based control panel simulates machine behavior, streams telemetry to Azure IoT Hub, and lands in Databricks for analytics, ML scoring, and OEE-style KPI reporting.

## End-to-End Flow

Arduino WiFi board -> USB serial on Mac -> Python sender -> Azure IoT Hub (built-in Event Hubs-compatible endpoint) -> Zerobus/Lakeflow on Databricks -> Delta Bronze/Silver/Gold -> SQL dashboards + Genie.

## Telemetry Contract

### Serial CSV

`vibration,temp,throughput,state,faultCode`

- `vibration`: float in engineering units (mm/s).
- `temp`: float in Celsius.
- `throughput`: integer components per minute.
- `state`: one of `RUN`, `STOPPED`, `FAULT`.
- `faultCode`: `NONE` when clear, fault identifier when active.

### IoT Hub JSON

```json
{
  "machine_id": "MC-0000",
  "vibration_mm_s": 4.2,
  "temp_c": 57.5,
  "throughput_cpm": 82,
  "state": "RUN",
  "fault_code": null,
  "ts": "2026-03-03T20:15:00Z"
}
```

## Lakehouse Model

- Bronze: raw payload + transport metadata.
- Silver: normalized telemetry with typed schema and quality checks.
- Gold: windowed machine health aggregates, OEE-style KPIs, and ML outputs.

## KPI and ML Targets

- Availability proxy: run-time fraction.
- Performance proxy: actual throughput / target throughput.
- Quality proxy: anomaly/fault-adjusted quality percentage.
- OEE proxy: availability x performance x quality.
- ML outputs:
  - `anomaly_score` (0-1)
  - `is_anomaly` (bool)
  - `prob_fault_next_5m` (0-1)

## Deployment Requirement

All Databricks assets must be deployable through Databricks Asset Bundles (DABs) with serverless-first configuration and Unity Catalog three-level namespaces.
