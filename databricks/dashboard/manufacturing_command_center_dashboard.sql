-- Manufacturing Command Center dashboard query pack (multi-horizon + filter-friendly).
-- Set catalog/schema before running:
-- USE CATALOG welch;
-- USE SCHEMA iot_demo;
--
-- Parameter guidance:
--   {{start_ts}} and {{end_ts}} should represent the selected time window.

-- 1) Fleet KPI cards (UC metric views + semantic state)
SELECT
  AVG(s.oee_pct) AS fleet_oee_pct,
  AVG(s.anomaly_score) AS avg_anomaly_score,
  AVG(s.prob_fault_next_5m) AS avg_fault_risk_5m,
  AVG(s.prob_fault_next_24h) AS avg_fault_risk_24h,
  AVG(s.prob_fault_next_7d) AS avg_fault_risk_7d,
  SUM(CASE WHEN s.prob_fault_next_5m >= 0.5 THEN 1 ELSE 0 END) AS watch_machines_5m,
  SUM(CASE WHEN s.prob_fault_next_24h >= 0.5 THEN 1 ELSE 0 END) AS watch_machines_24h,
  SUM(CASE WHEN s.prob_fault_next_7d >= 0.5 THEN 1 ELSE 0 END) AS watch_machines_7d,
  AVG(s.telemetry_lag_ms) AS avg_telemetry_lag_ms,
  AVG(s.ml_lag_ms) AS avg_ml_lag_ms
FROM vw_machine_current_status s;

-- 2) Live telemetry trends (date/time filter ready)
SELECT
  event_time,
  DATE_TRUNC('HOUR', event_time) AS event_hour,
  machine_id,
  state,
  temp_c,
  vibration_mm_s,
  rpm,
  current_amps,
  humidity_pct
FROM vw_machine_telemetry_live
WHERE event_time BETWEEN {{start_ts}} AND {{end_ts}}
ORDER BY event_time DESC;

-- 3) OEE and downtime by window
SELECT
  machine_id,
  window_end,
  DATE_TRUNC('HOUR', window_end) AS window_hour,
  oee_pct,
  availability_pct,
  performance_pct,
  quality_pct,
  time_in_run_s,
  time_in_stopped_s,
  time_in_fault_s
FROM vw_machine_health
WHERE window_end BETWEEN {{start_ts}} AND {{end_ts}}
ORDER BY window_end DESC, machine_id;

-- 4) Risk ranking with maintenance planning horizons
SELECT
  machine_id,
  state,
  oee_pct,
  anomaly_score,
  prob_fault_next_5m,
  prob_fault_next_1h,
  prob_fault_next_24h,
  prob_fault_next_7d,
  last_event_time
FROM vw_machine_current_status
ORDER BY prob_fault_next_7d DESC, prob_fault_next_24h DESC, prob_fault_next_1h DESC, prob_fault_next_5m DESC;

-- 5) Freshness from UC metric view
SELECT
  `Machine` AS machine_id,
  MEASURE(`Avg Telemetry Lag Ms`) AS avg_telemetry_lag_ms,
  MEASURE(`Avg ML Lag Ms`) AS avg_ml_lag_ms,
  MEASURE(`Max Telemetry Lag Ms`) AS max_telemetry_lag_ms,
  MEASURE(`Max ML Lag Ms`) AS max_ml_lag_ms
FROM mv_machine_freshness
GROUP BY ALL
ORDER BY max_ml_lag_ms DESC;

-- 6) OEE metric rollup from UC metric view
SELECT
  `Window Hour` AS window_hour,
  MEASURE(`OEE Pct`) AS oee_pct,
  MEASURE(`Availability Pct`) AS availability_pct,
  MEASURE(`Performance Pct`) AS performance_pct,
  MEASURE(`Quality Pct`) AS quality_pct
FROM mv_machine_oee
GROUP BY ALL
ORDER BY window_hour DESC;

-- 7) Risk metric rollup from UC metric view
SELECT
  `Window Date` AS window_date,
  MEASURE(`Avg Fault Risk 5m`) AS avg_fault_risk_5m,
  MEASURE(`Avg Fault Risk 1h`) AS avg_fault_risk_1h,
  MEASURE(`Avg Fault Risk 24h`) AS avg_fault_risk_24h,
  MEASURE(`Avg Fault Risk 7d`) AS avg_fault_risk_7d,
  MEASURE(`High Risk Windows`) AS high_risk_windows
FROM mv_machine_risk
GROUP BY ALL
ORDER BY window_date DESC;

-- 8) Machine drill-down table
SELECT
  s.machine_id,
  d.line_name,
  s.state,
  s.last_event_time,
  s.telemetry_lag_ms,
  s.ml_lag_ms,
  s.temp_c,
  s.vibration_mm_s,
  s.rpm,
  s.current_amps,
  s.humidity_pct,
  s.throughput_cpm,
  s.load_pct,
  s.power_kw,
  s.power_factor,
  s.voltage_v,
  s.pressure_bar,
  s.flow_rate_lpm,
  s.oee_pct,
  s.anomaly_score,
  s.prob_fault_next_5m,
  s.prob_fault_next_1h,
  s.prob_fault_next_24h,
  s.prob_fault_next_7d,
  CASE
    WHEN s.prob_fault_next_5m >= 0.8 THEN 'CRITICAL_NOW'
    WHEN s.prob_fault_next_24h >= 0.6 THEN 'PLAN_24H'
    WHEN s.prob_fault_next_7d >= 0.5 THEN 'PLAN_7D'
    ELSE 'NORMAL'
  END AS maintenance_band
FROM vw_machine_current_status s
LEFT JOIN dim_machine d ON s.machine_id = d.machine_id
ORDER BY s.prob_fault_next_7d DESC, s.prob_fault_next_24h DESC, s.prob_fault_next_5m DESC, s.last_event_time DESC;

-- 9) Latency status monitor table
SELECT
  machine_id,
  telemetry_lag_ms,
  ml_lag_ms,
  CASE
    WHEN telemetry_lag_ms < 10000 THEN '✓ <10s'
    WHEN telemetry_lag_ms < 30000 THEN '⚠ 10-30s'
    ELSE '✗ >30s'
  END AS latency_status
FROM vw_machine_current_status
ORDER BY telemetry_lag_ms DESC;
