-- Configure catalog/schema for your workspace before execution.
-- Example:
-- USE CATALOG welch;
-- CREATE SCHEMA IF NOT EXISTS iot_demo_dev;
-- USE SCHEMA iot_demo_dev;

CREATE OR REPLACE VIEW vw_machine_telemetry_live AS
SELECT
  event_time,
  machine_id,
  vibration_mm_s,
  temp_c,
  throughput_cpm,
  state,
  fault_code,
  iothub_device_id
FROM silver_machine_telemetry
WHERE event_time >= current_timestamp() - INTERVAL 2 HOURS;

CREATE OR REPLACE VIEW vw_machine_health AS
WITH anomaly_latest AS (
  SELECT machine_id, event_time, anomaly_score, is_anomaly, scored_at, inference_type, model_run_id
  FROM (
    SELECT
      machine_id,
      event_time,
      anomaly_score,
      is_anomaly,
      scored_at,
      inference_type,
      model_run_id,
      row_number() OVER (PARTITION BY machine_id ORDER BY scored_at DESC, event_time DESC) AS rn
    FROM ml_anomaly_scores
  )
  WHERE rn = 1
),
fault_pred_latest AS (
  SELECT machine_id, event_time, prob_fault_next_5m, predicted_fault_next_5m, scored_at, inference_type, model_run_id
  FROM (
    SELECT
      machine_id,
      event_time,
      prob_fault_next_5m,
      predicted_fault_next_5m,
      scored_at,
      inference_type,
      model_run_id,
      row_number() OVER (PARTITION BY machine_id ORDER BY scored_at DESC, event_time DESC) AS rn
    FROM ml_fault_predictions
  )
  WHERE rn = 1
)
SELECT
  g.machine_id,
  g.window_start,
  g.window_end,
  g.avg_vibration_mm_s,
  g.avg_temp_c,
  g.avg_throughput_cpm,
  g.time_in_run_s,
  g.time_in_stopped_s,
  g.time_in_fault_s,
  g.availability_pct,
  g.performance_pct,
  g.quality_pct,
  g.oee_pct,
  COALESCE(a.anomaly_score, g.anomaly_score) AS anomaly_score,
  COALESCE(a.is_anomaly, g.is_anomaly) AS is_anomaly,
  COALESCE(f.prob_fault_next_5m, g.prob_fault_next_5m) AS prob_fault_next_5m,
  COALESCE(f.predicted_fault_next_5m, COALESCE(f.prob_fault_next_5m, g.prob_fault_next_5m) >= 0.5) AS predicted_fault_next_5m,
  a.inference_type AS anomaly_inference_type,
  f.inference_type AS fault_inference_type,
  a.model_run_id AS anomaly_model_run_id,
  f.model_run_id AS fault_model_run_id,
  a.scored_at AS anomaly_scored_at,
  f.scored_at AS fault_scored_at,
  greatest(a.scored_at, f.scored_at) AS last_ml_score_time
FROM gold_machine_health_5m g
LEFT JOIN anomaly_latest a
  ON g.machine_id = a.machine_id
LEFT JOIN fault_pred_latest f
  ON g.machine_id = f.machine_id;

CREATE OR REPLACE VIEW dim_machine AS
SELECT DISTINCT
  machine_id,
  CASE
    WHEN machine_id = 'MC-0000' THEN 'Physical Line 0000'
    WHEN machine_id RLIKE '^MC-[0-9]+$'
      THEN CONCAT('Virtual Line ', regexp_extract(machine_id, '^MC-([0-9]+)$', 1))
    ELSE 'Unknown Line'
  END AS line_name
FROM silver_machine_telemetry;

CREATE OR REPLACE VIEW vw_machine_current_status AS
WITH latest_telemetry AS (
  SELECT *
  FROM (
    SELECT
      t.*,
      row_number() OVER (PARTITION BY machine_id ORDER BY event_time DESC) AS rn
    FROM silver_machine_telemetry t
  )
  WHERE rn = 1
),
latest_health AS (
  SELECT *
  FROM (
    SELECT
      h.*,
      row_number() OVER (PARTITION BY machine_id ORDER BY window_end DESC) AS rn
    FROM vw_machine_health h
  )
  WHERE rn = 1
)
SELECT
  t.machine_id,
  t.event_time AS last_event_time,
  t.state,
  t.vibration_mm_s,
  t.temp_c,
  t.throughput_cpm,
  h.oee_pct,
  h.availability_pct,
  h.performance_pct,
  h.quality_pct,
  h.anomaly_score,
  h.prob_fault_next_5m,
  h.anomaly_inference_type,
  h.fault_inference_type,
  h.anomaly_model_run_id,
  h.fault_model_run_id,
  h.anomaly_scored_at,
  h.fault_scored_at,
  h.last_ml_score_time,
  CAST(unix_timestamp(current_timestamp()) - unix_timestamp(t.event_time) AS INT) AS telemetry_lag_seconds,
  CAST((unix_timestamp(current_timestamp()) - unix_timestamp(t.event_time)) * 1000 AS BIGINT) AS telemetry_lag_ms,
  CAST(
    unix_timestamp(current_timestamp()) - unix_timestamp(COALESCE(h.last_ml_score_time, t.event_time))
    AS INT
  ) AS ml_lag_seconds,
  CAST(
    (unix_timestamp(current_timestamp()) - unix_timestamp(COALESCE(h.last_ml_score_time, t.event_time))) * 1000
    AS BIGINT
  ) AS ml_lag_ms
FROM latest_telemetry t
LEFT JOIN latest_health h
  ON t.machine_id = h.machine_id;
