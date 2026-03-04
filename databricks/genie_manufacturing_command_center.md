# Genie Space: Manufacturing Command Center

Use this as the canonical setup/instruction pack when creating the Genie Space.
Automated deployment script: `databricks/deploy_genie_space.py`.

## Space Name
`Manufacturing Command Center`

## Warehouse
Use SQL warehouse: `148ccb90800933a1`

## Included Objects (Trusted)
- `mv_machine_telemetry`
- `mv_machine_oee`
- `mv_machine_downtime`
- `mv_machine_risk`
- `mv_machine_freshness`
- `mv_machine_current`
- `vw_machine_telemetry_live`
- `vw_machine_health`
- `vw_machine_current_status`
- `dim_machine`

Do not include Bronze/raw tables in this Genie scope.

## Genie Instructions
1. Interpret "risk" or "likely to fail" as `prob_fault_next_5m`.
2. Interpret "anomaly" using `anomaly_score` and threshold `>= 0.7`.
3. Interpret "downtime" as `time_in_stopped_s + time_in_fault_s`.
4. Interpret "OEE" as `oee_pct` from `vw_machine_health` or `vw_machine_current_status`.
5. For "current" questions, default to latest `window_end` or `last_event_time`.
6. For machine line names, use `dim_machine.line_name`.
7. Always return machine IDs in results alongside business labels.

## Validation Prompt Pack
- Which machine is most likely to fault in the next 5 minutes?
- Show downtime for each machine in the last 60 minutes.
- Which machines currently have anomaly score above 0.7?
- What is the latest OEE for MC-0000?
- Compare availability, performance, and quality for all machines right now.
- Show trend of vibration and temperature for the last hour for MC-0000.
- Which line has the highest average fault risk today?

## Expected Behavior Checks
- Results align with dashboard metrics for latest windows.
- Time filters are respected when user specifies intervals.
- Calculations for downtime and OEE use definitions above.
