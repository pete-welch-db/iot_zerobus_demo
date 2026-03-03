-- Manufacturing Command Center dashboard query pack.
-- Set catalog/schema before running:
-- USE CATALOG welch;
-- USE SCHEMA iot_demo_dev;

-- 1) Live Operations: temperature trend
SELECT
  event_time,
  machine_id,
  temp_c
FROM vw_machine_telemetry_live
ORDER BY event_time DESC;

-- 2) Live Operations: vibration trend
SELECT
  event_time,
  machine_id,
  vibration_mm_s
FROM vw_machine_telemetry_live
ORDER BY event_time DESC;

-- 3) Live Operations: throughput trend
SELECT
  event_time,
  machine_id,
  throughput_cpm
FROM vw_machine_telemetry_live
ORDER BY event_time DESC;

-- 4) Health and risk KPI snapshot
SELECT
  machine_id,
  oee_pct,
  availability_pct,
  performance_pct,
  quality_pct,
  anomaly_score,
  prob_fault_next_5m,
  window_end
FROM vw_machine_health
QUALIFY row_number() OVER (PARTITION BY machine_id ORDER BY window_end DESC) = 1;

-- 5) Loss analysis by machine
SELECT
  machine_id,
  window_end,
  time_in_run_s,
  time_in_stopped_s,
  time_in_fault_s
FROM vw_machine_health
ORDER BY window_end DESC, machine_id;

-- 6) At-risk machine ranking
SELECT
  machine_id,
  prob_fault_next_5m,
  anomaly_score,
  oee_pct,
  window_end
FROM vw_machine_health
QUALIFY row_number() OVER (PARTITION BY machine_id ORDER BY window_end DESC) = 1
ORDER BY prob_fault_next_5m DESC, anomaly_score DESC;

-- 7) Current status table
SELECT
  s.machine_id,
  d.line_name,
  s.last_event_time,
  s.state,
  s.vibration_mm_s,
  s.temp_c,
  s.throughput_cpm,
  s.oee_pct,
  s.anomaly_score,
  s.prob_fault_next_5m,
  s.last_ml_score_time
FROM vw_machine_current_status s
LEFT JOIN dim_machine d
  ON s.machine_id = d.machine_id
ORDER BY s.last_event_time DESC;

-- 8) Fleet KPI cards
SELECT
  avg(oee_pct) AS fleet_oee_pct,
  avg(availability_pct) AS fleet_availability_pct,
  avg(performance_pct) AS fleet_performance_pct,
  avg(quality_pct) AS fleet_quality_pct,
  sum(CASE WHEN anomaly_score >= 0.7 THEN 1 ELSE 0 END) AS active_anomaly_machines,
  sum(CASE WHEN prob_fault_next_5m >= 0.5 THEN 1 ELSE 0 END) AS high_risk_machines,
  avg(telemetry_lag_seconds) AS avg_telemetry_lag_seconds,
  avg(ml_lag_seconds) AS avg_ml_lag_seconds
FROM vw_machine_current_status;

-- 9) ML/telemetry freshness monitor
SELECT
  machine_id,
  last_event_time,
  last_ml_score_time,
  telemetry_lag_seconds,
  ml_lag_seconds,
  anomaly_inference_type,
  fault_inference_type
FROM vw_machine_current_status
ORDER BY ml_lag_seconds DESC, telemetry_lag_seconds DESC;
