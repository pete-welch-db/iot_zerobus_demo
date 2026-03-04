# Demo Day Talk Track

This runbook is for an on-site conference room demo with a physical Arduino (`MC-0000`) and optional virtual scale-out (`MC-####`).

## Pre-demo Setup (5-10 min)

1. Turn on phone hotspot.
2. Plug Arduino in via USB and confirm serial logs show WiFi + MQTT connected.
3. Open Databricks dashboard (`Manufacturing Command Center`) and set machine filter to `MC-0000`.
4. In terminal from repo root, ensure scripts are executable:
   - `scripts/demo_go.sh`
   - `scripts/demo_generate.sh`
   - `scripts/demo_stop.sh`

## Operator Commands

- `go`: starts one-shot real-time pipeline refresh for physical-device storytelling.
- `generate`: runs fleet simulator and pushes virtual-device telemetry, then refreshes bridge + DLT + ML.
- `stop`: cancels active and queued demo job runs.

## Command Reference

```bash
# Physical-device-first phase
TARGET=dev MACHINE_ID=MC-0000 scripts/demo_go.sh

# Scale-out phase (3 minutes default)
TARGET=dev scripts/demo_generate.sh

# Optional: custom scale burst
TARGET=dev DURATION_SECONDS=240 MESSAGE_RATE_HZ=1.2 scripts/demo_generate.sh

# Tear down active runs
TARGET=dev scripts/demo_stop.sh
```

## Talk Track

### 1) Opening (1-2 min)

- "We are streaming live telemetry from a physical edge device over hotspot into Azure IoT and into Databricks through Zerobus."
- "The value here is low-latency operational awareness with immediate ML-assisted risk."

### 2) `go` Phase: Physical Device Only (3-5 min)

1. Say: "Go."
2. Run: `TARGET=dev MACHINE_ID=MC-0000 scripts/demo_go.sh`.
3. Keep dashboard filtered to `MC-0000`.
4. Narrate:
   - temperature/vibration/throughput update in near real time,
   - telemetry lag and ML lag counters stay low,
   - OEE and risk react to state transitions.

### 3) Fault Moment (2-3 min)

1. Increase pots to push temperature/vibration above threshold.
2. Explain expected behavior:
   - feed shows `FAULT` state and fault code indicators,
   - risk (`prob_fault_next_5m`) rises,
   - anomaly and fault widgets move upward as scoring refreshes.

### 4) `generate` Phase: Scale-out (3-5 min)

1. Say: "Generate."
2. Run: `TARGET=dev scripts/demo_generate.sh`.
3. Refresh dashboard and clear machine filter to fleet view.
4. Narrate:
   - additional virtual devices appear,
   - health + latency table shows per-device freshness,
   - fleet ranking highlights highest fault risk first.

### 5) Close (1-2 min)

- "Zerobus gives us a fast ingestion path from edge signals to governed analytics and ML."
- "We can start with one physical asset and then scale to many devices without changing architecture."

## Rehearsal Acceptance Criteria (SLO Targets)

- Telemetry visible in dashboard: `< 30-60s` after event ingestion run.
- `telemetry_lag_ms` fleet average: `< 60000` in steady state.
- `ml_lag_ms` fleet average: `< 90000` after realtime ML scoring run.
- `go` phase confirms `MC-0000` freshness and ML score update.
- `generate` phase adds `MC-####` devices and updates risk ranking.

## Quick Recovery

- If `MC-0000` appears `STOPPED` unexpectedly:
  - press RUN/STOP button once on Arduino,
  - rerun `TARGET=dev MACHINE_ID=MC-0000 scripts/demo_go.sh`,
  - confirm latest state in `vw_machine_current_status`.
- If jobs are queued:
  - run `TARGET=dev scripts/demo_stop.sh`,
  - rerun `demo_go.sh`.
