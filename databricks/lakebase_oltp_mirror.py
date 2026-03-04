"""
Mirror near-real-time OLAP semantic outputs into Lakebase OLTP tables.
"""

import argparse
from typing import Any, Dict, List

import psycopg2
from psycopg2.extras import execute_values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upsert current IoT semantic status into Lakebase.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--jdbc-url", default="")
    parser.add_argument("--db-host", default="")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", default="iot_demo")
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--user-secret-key", required=True)
    parser.add_argument("--password-secret-key", required=True)
    parser.add_argument("--instance-id", default="")
    return parser.parse_args()


def _resolve_jdbc(args: argparse.Namespace) -> str:
    if args.jdbc_url:
        return args.jdbc_url
    if not args.db_host:
        raise ValueError("Either --jdbc-url or --db-host must be provided.")
    return f"jdbc:postgresql://{args.db_host}:{args.db_port}/{args.db_name}"


def _jdbc_to_pg_dsn(jdbc_url: str, user: str, password: str) -> str:
    if not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError(f"Unsupported JDBC URL format: {jdbc_url}")
    dsn = jdbc_url.replace("jdbc:", "", 1)
    return f"{dsn}?sslmode=require&user={user}&password={password}"


def _collect_rows(view_name: str) -> List[Dict[str, Any]]:
    rows = spark.table(view_name).select(
        "machine_id",
        "state",
        "last_event_time",
        "telemetry_lag_ms",
        "ml_lag_ms",
        "temp_c",
        "vibration_mm_s",
        "throughput_cpm",
        "rpm",
        "current_amps",
        "humidity_pct",
        "load_pct",
        "power_kw",
        "power_factor",
        "voltage_v",
        "pressure_bar",
        "flow_rate_lpm",
        "oee_pct",
        "anomaly_score",
        "prob_fault_next_5m",
    )
    return [r.asDict(recursive=True) for r in rows.collect()]


def _ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS machine_current_status (
              machine_id TEXT PRIMARY KEY,
              state TEXT,
              last_event_time TIMESTAMPTZ,
              telemetry_lag_ms BIGINT,
              ml_lag_ms BIGINT,
              temp_c DOUBLE PRECISION,
              vibration_mm_s DOUBLE PRECISION,
              throughput_cpm INTEGER,
              rpm INTEGER,
              current_amps DOUBLE PRECISION,
              humidity_pct DOUBLE PRECISION,
              load_pct DOUBLE PRECISION,
              power_kw DOUBLE PRECISION,
              power_factor DOUBLE PRECISION,
              voltage_v DOUBLE PRECISION,
              pressure_bar DOUBLE PRECISION,
              flow_rate_lpm DOUBLE PRECISION,
              oee_pct DOUBLE PRECISION,
              anomaly_score DOUBLE PRECISION,
              prob_fault_next_5m DOUBLE PRECISION,
              source_watermark TIMESTAMPTZ,
              updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mirror_metadata (
              instance_id TEXT PRIMARY KEY,
              last_run_at TIMESTAMPTZ,
              row_count BIGINT,
              source_watermark TIMESTAMPTZ
            )
            """
        )
    conn.commit()


def _upsert_rows(conn, rows: List[Dict[str, Any]], instance_id: str) -> None:
    if not rows:
        return
    source_watermark = max(row["last_event_time"] for row in rows if row["last_event_time"] is not None)
    payload = [
        (
            row["machine_id"],
            row["state"],
            row["last_event_time"],
            row["telemetry_lag_ms"],
            row["ml_lag_ms"],
            row["temp_c"],
            row["vibration_mm_s"],
            row["throughput_cpm"],
            row["rpm"],
            row["current_amps"],
            row["humidity_pct"],
            row["load_pct"],
            row["power_kw"],
            row["power_factor"],
            row["voltage_v"],
            row["pressure_bar"],
            row["flow_rate_lpm"],
            row["oee_pct"],
            row["anomaly_score"],
            row["prob_fault_next_5m"],
            source_watermark,
        )
        for row in rows
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO machine_current_status (
              machine_id, state, last_event_time, telemetry_lag_ms, ml_lag_ms,
              temp_c, vibration_mm_s, throughput_cpm, rpm, current_amps, humidity_pct,
              load_pct, power_kw, power_factor, voltage_v, pressure_bar, flow_rate_lpm,
              oee_pct, anomaly_score, prob_fault_next_5m, source_watermark
            ) VALUES %s
            ON CONFLICT (machine_id) DO UPDATE SET
              state = EXCLUDED.state,
              last_event_time = EXCLUDED.last_event_time,
              telemetry_lag_ms = EXCLUDED.telemetry_lag_ms,
              ml_lag_ms = EXCLUDED.ml_lag_ms,
              temp_c = EXCLUDED.temp_c,
              vibration_mm_s = EXCLUDED.vibration_mm_s,
              throughput_cpm = EXCLUDED.throughput_cpm,
              rpm = EXCLUDED.rpm,
              current_amps = EXCLUDED.current_amps,
              humidity_pct = EXCLUDED.humidity_pct,
              load_pct = EXCLUDED.load_pct,
              power_kw = EXCLUDED.power_kw,
              power_factor = EXCLUDED.power_factor,
              voltage_v = EXCLUDED.voltage_v,
              pressure_bar = EXCLUDED.pressure_bar,
              flow_rate_lpm = EXCLUDED.flow_rate_lpm,
              oee_pct = EXCLUDED.oee_pct,
              anomaly_score = EXCLUDED.anomaly_score,
              prob_fault_next_5m = EXCLUDED.prob_fault_next_5m,
              source_watermark = EXCLUDED.source_watermark,
              updated_at = NOW()
            """,
            payload,
        )
        cur.execute(
            """
            INSERT INTO mirror_metadata (instance_id, last_run_at, row_count, source_watermark)
            VALUES (%s, NOW(), %s, %s)
            ON CONFLICT (instance_id) DO UPDATE SET
              last_run_at = EXCLUDED.last_run_at,
              row_count = EXCLUDED.row_count,
              source_watermark = EXCLUDED.source_watermark
            """,
            (instance_id or "lakebase-default", len(rows), source_watermark),
        )
    conn.commit()


def main() -> None:
    args = parse_args()
    user = dbutils.secrets.get(scope=args.secret_scope, key=args.user_secret_key)
    password = dbutils.secrets.get(scope=args.secret_scope, key=args.password_secret_key)
    jdbc_url = _resolve_jdbc(args)
    dsn = _jdbc_to_pg_dsn(jdbc_url, user, password)
    view_name = f"{args.catalog}.{args.schema}.vw_machine_current_status"
    rows = _collect_rows(view_name)
    conn = psycopg2.connect(dsn)
    try:
        _ensure_tables(conn)
        _upsert_rows(conn, rows, args.instance_id)
    finally:
        conn.close()
    print(f"Lakebase mirror upsert complete. rows={len(rows)} view={view_name}")


if __name__ == "__main__":
    main()
