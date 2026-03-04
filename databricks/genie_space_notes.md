# Genie Space Notes

Create a Genie Space backed by SQL Serverless and include only curated objects:

- `vw_machine_telemetry_live`
- `vw_machine_health`
- `dim_machine`

Mark these views as trusted and hide raw Bronze tables from Genie scope.

## Domain Vocabulary

- machine
- line
- run / stopped / fault
- downtime
- anomaly
- predictive maintenance
- availability
- performance
- quality
- OEE

## Guidance for Genie Instructions

Use concise business-language instructions:

1. Interpret "machine risk" as `prob_fault_next_5m`.
2. Interpret "anomaly" using `anomaly_score` and `is_anomaly`.
3. For "downtime", use `time_in_stopped_s + time_in_fault_s`.
4. For OEE, use `oee_pct` from `vw_machine_health`.
5. Prefer latest available `window_end` unless user asks for a specific range.

## Example Prompts

- Which machine is most likely to fault in the next 10 minutes?
- Show the last hour of downtime and performance loss for MC-0000.
- What is the current OEE trend for Packaging Line A?
