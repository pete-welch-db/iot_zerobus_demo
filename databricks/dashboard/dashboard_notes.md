# AI/BI Dashboard Notes

Use Databricks SQL Serverless and build visuals on top of `vw_machine_telemetry_live` and `vw_machine_health`.

## Dashboard Sections

1. Live telemetry (last 60 minutes)
   - Line chart: `temp_c` by `event_time` and `machine_id`.
   - Line chart: `vibration_mm_s` by `event_time` and `machine_id`.
   - Area or bar chart: `throughput_cpm` by `event_time`.

2. Current machine health
   - KPI cards:
     - `availability_pct`
     - `performance_pct`
     - `quality_pct`
     - `oee_pct`
   - Risk cards:
     - `anomaly_score`
     - `prob_fault_next_5m`

3. Downtime and loss
   - Stacked bars:
     - `time_in_run_s`
     - `time_in_stopped_s`
     - `time_in_fault_s`
   - Table:
     - machine, window, OEE, anomaly, predicted fault.

4. Ranking
   - Top machines by `prob_fault_next_5m` descending.
   - Top machines by `anomaly_score` descending.

## KPI Definitions (Demo Approximation)

- Availability proxy:
  - `time_in_run_s / (time_in_run_s + time_in_stopped_s + time_in_fault_s)`
- Performance proxy:
  - `avg_throughput_cpm / target_throughput_cpm` (capped at 100%)
- Quality proxy:
  - `100% - fault_rate`
- OEE proxy:
  - `availability * performance * quality`

## Recommended Filters

- Time range (`window_end` or `event_time`)
- Machine (`machine_id`)
- State (`state` on telemetry view)
