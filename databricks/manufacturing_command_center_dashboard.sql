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
  AVG(oee_pct) AS fleet_oee_pct,
  AVG(availability_pct) AS fleet_availability_pct,
  AVG(performance_pct) AS fleet_performance_pct,
  AVG(quality_pct) AS fleet_quality_pct,
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

-- 10) Device health + latency table (bottom widget)
SELECT
  s.machine_id,
  d.line_name,
  s.state,
  s.last_event_time,
  s.telemetry_lag_seconds,
  s.ml_lag_seconds,
  s.temp_c,
  s.vibration_mm_s,
  s.throughput_cpm,
  s.rpm,
  s.load_pct,
  s.power_kw,
  s.current_a,
  s.pressure_bar,
  s.anomaly_score,
  s.prob_fault_next_5m,
  CASE
    WHEN s.prob_fault_next_5m >= 0.7 THEN 'HIGH'
    WHEN s.prob_fault_next_5m >= 0.4 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS fault_band
FROM vw_machine_current_status s
LEFT JOIN dim_machine d
  ON s.machine_id = d.machine_id
ORDER BY s.telemetry_lag_seconds ASC, s.ml_lag_seconds ASC, s.last_event_time DESC;

-- 11) Fleet SLO benchmark status (backed by semantic view + UC metric view)
SELECT
  machine_count,
  pct_within_telemetry_slo,
  pct_within_ml_slo,
  max_telemetry_lag_seconds,
  max_ml_lag_seconds
FROM vw_machine_slo_status;

-- 12) Critical machines breaching SLOs
SELECT
  machine_id,
  state,
  telemetry_lag_seconds,
  ml_lag_seconds,
  prob_fault_next_5m,
  anomaly_score
FROM vw_machine_current_status
WHERE telemetry_lag_seconds > 60 OR ml_lag_seconds > 90
ORDER BY ml_lag_seconds DESC, telemetry_lag_seconds DESC, prob_fault_next_5m DESC;

-- 13) Metric-view query examples for validated KPI definitions
SELECT
  MAX(`Pct Within Telemetry SLO`) AS mv_pct_within_telemetry_slo,
  MAX(`Pct Within ML SLO`) AS mv_pct_within_ml_slo,
  MAX(`Max Telemetry Lag Seconds`) AS mv_max_telemetry_lag_seconds,
  MAX(`Max ML Lag Seconds`) AS mv_max_ml_lag_seconds
FROM mv_machine_slo;
