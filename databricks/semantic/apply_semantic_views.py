"""
Create or refresh curated semantic views for dashboard and Genie.

Uses CREATE VIEW IF NOT EXISTS + ALTER VIEW AS to update query definitions
without wiping Unity Catalog comments on views and columns.
"""

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create semantic views for IoT demo.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--telemetry-live-window-hours",
        type=int,
        default=24,
        help="Rolling window used by vw_machine_telemetry_live.",
    )
    return parser.parse_args()


def _upsert_view(full_name: str, select_sql: str) -> None:
    """Create view if new, otherwise alter its query — preserving UC comments."""
    spark.sql(f"CREATE VIEW IF NOT EXISTS {full_name} AS {select_sql}")
    spark.sql(f"ALTER VIEW {full_name} AS {select_sql}")


def main() -> None:
    args = parse_args()
    catalog = args.catalog
    schema = args.schema
    telemetry_live_window_hours = max(1, int(args.telemetry_live_window_hours))
    fault_pred_cols = {
        c.lower() for c in spark.table(f"{catalog}.{schema}.ml_fault_predictions").columns
    }

    def _fault_col_or_default(column_name: str, fallback_sql: str) -> str:
        if column_name.lower() in fault_pred_cols:
            return column_name
        return f"{fallback_sql} AS {column_name}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    _upsert_view(
        f"{catalog}.{schema}.vw_machine_telemetry_live",
        f"""
        SELECT
          event_time,
          machine_id,
          vibration_mm_s,
          temp_c,
          throughput_cpm,
          rpm,
          current_amps,
          humidity_pct,
          load_pct,
          power_kw,
          power_factor,
          voltage_v,
          pressure_bar,
          flow_rate_lpm,
          state,
          fault_code,
          iothub_device_id
        FROM {catalog}.{schema}.silver_machine_telemetry
        WHERE event_time >= current_timestamp() - INTERVAL {telemetry_live_window_hours} HOURS
        """,
    )

    fault_horizon_cols = ",\n              ".join(
        [
            _fault_col_or_default("prob_fault_next_1h", "CAST(NULL AS DOUBLE)"),
            _fault_col_or_default("predicted_fault_next_1h", "CAST(NULL AS BOOLEAN)"),
            _fault_col_or_default("prob_fault_next_24h", "CAST(NULL AS DOUBLE)"),
            _fault_col_or_default("predicted_fault_next_24h", "CAST(NULL AS BOOLEAN)"),
            _fault_col_or_default("prob_fault_next_7d", "CAST(NULL AS DOUBLE)"),
            _fault_col_or_default("predicted_fault_next_7d", "CAST(NULL AS BOOLEAN)"),
        ]
    )

    _upsert_view(
        f"{catalog}.{schema}.vw_machine_health",
        f"""
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
            FROM {catalog}.{schema}.ml_anomaly_scores
          )
          WHERE rn = 1
        ),
        fault_pred_latest AS (
          SELECT
            machine_id,
            event_time,
            prob_fault_next_5m,
            predicted_fault_next_5m,
            prob_fault_next_1h,
            predicted_fault_next_1h,
            prob_fault_next_24h,
            predicted_fault_next_24h,
            prob_fault_next_7d,
            predicted_fault_next_7d,
            scored_at,
            inference_type,
            model_run_id
          FROM (
            SELECT
              machine_id,
              event_time,
              prob_fault_next_5m,
              predicted_fault_next_5m,
              {fault_horizon_cols},
              scored_at,
              inference_type,
              model_run_id,
              row_number() OVER (PARTITION BY machine_id ORDER BY scored_at DESC, event_time DESC) AS rn
            FROM {catalog}.{schema}.ml_fault_predictions
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
          g.avg_rpm,
          g.avg_current_amps,
          g.avg_humidity_pct,
          g.event_count,
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
          COALESCE(f.predicted_fault_next_5m, COALESCE(f.prob_fault_next_5m, g.prob_fault_next_5m) >= 0.5)
            AS predicted_fault_next_5m,
          f.prob_fault_next_1h AS prob_fault_next_1h,
          f.predicted_fault_next_1h AS predicted_fault_next_1h,
          f.prob_fault_next_24h AS prob_fault_next_24h,
          f.predicted_fault_next_24h AS predicted_fault_next_24h,
          f.prob_fault_next_7d AS prob_fault_next_7d,
          f.predicted_fault_next_7d AS predicted_fault_next_7d,
          a.inference_type AS anomaly_inference_type,
          f.inference_type AS fault_inference_type,
          a.model_run_id AS anomaly_model_run_id,
          f.model_run_id AS fault_model_run_id,
          a.scored_at AS anomaly_scored_at,
          f.scored_at AS fault_scored_at,
          greatest(a.scored_at, f.scored_at) AS last_ml_score_time
        FROM {catalog}.{schema}.gold_machine_health_5m g
        LEFT JOIN anomaly_latest a
          ON g.machine_id = a.machine_id
        LEFT JOIN fault_pred_latest f
          ON g.machine_id = f.machine_id
        """
    )

    _upsert_view(
        f"{catalog}.{schema}.dim_machine",
        f"""
        SELECT DISTINCT
          machine_id,
          CASE
            WHEN machine_id = 'MC-0000' THEN 'Physical Line 0000'
            WHEN machine_id RLIKE '^MC-[0-9]+$'
              THEN CONCAT('Virtual Line ', regexp_extract(machine_id, '^MC-([0-9]+)$', 1))
            ELSE 'Unknown Line'
          END AS line_name
        FROM {catalog}.{schema}.silver_machine_telemetry
        """,
    )

    _upsert_view(
        f"{catalog}.{schema}.vw_pipeline_latency",
        f"""
        WITH parsed AS (
          SELECT
            b.machine_id,
            to_timestamp(b.ts)                    AS device_ts,
            to_timestamp(b.iothub_enqueued_time)  AS iothub_ts,
            to_timestamp(b.ingest_ts)             AS zerobus_ts,
            b.state
          FROM {catalog}.{schema}.bronze_iot_telemetry b
          WHERE b.ts IS NOT NULL
            AND b.iothub_enqueued_time IS NOT NULL
            AND b.ingest_ts IS NOT NULL
        )
        SELECT
          p.machine_id,
          d.line_name,
          p.state,
          p.device_ts,
          p.iothub_ts,
          p.zerobus_ts,
          ROUND(unix_millis(p.iothub_ts) - unix_millis(p.device_ts))    AS hop1_device_to_iothub_ms,
          ROUND(unix_millis(p.zerobus_ts) - unix_millis(p.iothub_ts))   AS hop2_iothub_to_zerobus_ms,
          ROUND(unix_millis(p.zerobus_ts) - unix_millis(p.device_ts))   AS total_device_to_zerobus_ms
        FROM parsed p
        LEFT JOIN {catalog}.{schema}.dim_machine d
          ON p.machine_id = d.machine_id
        """,
    )

    _upsert_view(
        f"{catalog}.{schema}.vw_machine_current_status",
        f"""
        WITH latest_telemetry AS (
          SELECT *
          FROM (
            SELECT
              t.*,
              row_number() OVER (PARTITION BY machine_id ORDER BY last_event_time DESC) AS rn
            FROM {catalog}.{schema}.gold_machine_current_status t
          )
          WHERE rn = 1
        ),
        latest_health AS (
          SELECT *
          FROM (
            SELECT
              h.*,
              row_number() OVER (PARTITION BY machine_id ORDER BY window_end DESC) AS rn
            FROM {catalog}.{schema}.vw_machine_health h
          )
          WHERE rn = 1
        )
        SELECT
          t.machine_id,
          t.last_event_time,
          t.state,
          t.vibration_mm_s,
          t.temp_c,
          t.throughput_cpm,
          t.rpm,
          t.current_amps,
          t.humidity_pct,
          t.load_pct,
          t.power_kw,
          t.power_factor,
          t.voltage_v,
          t.pressure_bar,
          t.flow_rate_lpm,
          h.oee_pct,
          h.availability_pct,
          h.performance_pct,
          h.quality_pct,
          h.anomaly_score,
          h.prob_fault_next_5m,
          h.prob_fault_next_1h,
          h.prob_fault_next_24h,
          h.prob_fault_next_7d,
          h.predicted_fault_next_5m,
          h.predicted_fault_next_1h,
          h.predicted_fault_next_24h,
          h.predicted_fault_next_7d,
          h.anomaly_inference_type,
          h.fault_inference_type,
          h.anomaly_model_run_id,
          h.fault_model_run_id,
          h.anomaly_scored_at,
          h.fault_scored_at,
          h.last_ml_score_time,
          CAST(
            COALESCE(
              t.telemetry_lag_seconds,
              unix_timestamp(current_timestamp()) - unix_timestamp(t.last_event_time)
            ) AS INT
          ) AS telemetry_lag_seconds,
          CAST(
            COALESCE(
              t.telemetry_lag_ms,
              (unix_timestamp(current_timestamp()) - unix_timestamp(t.last_event_time)) * 1000
            ) AS BIGINT
          ) AS telemetry_lag_ms,
          CAST(
            unix_timestamp(current_timestamp()) - unix_timestamp(COALESCE(h.last_ml_score_time, t.last_event_time))
            AS INT
          ) AS ml_lag_seconds,
          CAST(
            (unix_timestamp(current_timestamp()) - unix_timestamp(COALESCE(h.last_ml_score_time, t.last_event_time))) * 1000
            AS BIGINT
          ) AS ml_lag_ms,
          d.line_name
        FROM latest_telemetry t
        LEFT JOIN latest_health h
          ON t.machine_id = h.machine_id
        LEFT JOIN {catalog}.{schema}.dim_machine d
          ON t.machine_id = d.machine_id
        """
    )

    print(f"Semantic views refreshed in {catalog}.{schema}.")


if __name__ == "__main__":
    main()
