from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from psycopg2 import sql as psql
from databricks import sql as dbsql

from config import AppConfig

APP_TZ = ZoneInfo("America/Detroit")


def _localize_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns from UTC to America/Detroit for display."""
    for col in df.columns:
        if df[col].empty:
            continue
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize("UTC").dt.tz_convert(APP_TZ)
            else:
                df[col] = df[col].dt.tz_convert(APP_TZ)
        elif df[col].dtype == "object":
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, datetime):
                df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(APP_TZ)
    return df


@dataclass
class DataClients:
    config: AppConfig

    def query_sql(self, statement: str) -> pd.DataFrame:
        with dbsql.connect(
            server_hostname=self.config.workspace_host.replace("https://", ""),
            http_path=self.config.sql_http_path,
            access_token=self.config.token,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(statement)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        return _localize_timestamps(pd.DataFrame(rows, columns=cols))

    def query_metric_summary(self) -> pd.DataFrame:
        c = self.config.catalog
        s = self.config.schema
        return self.query_sql(
            f"""
            SELECT
              machine_id,
              state,
              oee_pct,
              anomaly_score,
              prob_fault_next_5m,
              telemetry_lag_ms,
              ml_lag_ms,
              last_event_time
            FROM {c}.{s}.vw_machine_current_status
            ORDER BY prob_fault_next_5m DESC, anomaly_score DESC
            LIMIT 200
            """
        )

    def query_flow_break_signals(self) -> pd.DataFrame:
        c = self.config.catalog
        s = self.config.schema
        return self.query_sql(
            f"""
            SELECT
              machine_id,
              state,
              throughput_cpm,
              vibration_mm_s,
              temp_c,
              current_amps,
              humidity_pct,
              load_pct,
              anomaly_score,
              prob_fault_next_5m,
              telemetry_lag_ms,
              last_event_time
            FROM {c}.{s}.vw_machine_current_status
            ORDER BY prob_fault_next_5m DESC, telemetry_lag_ms DESC
            LIMIT 200
            """
        )

    def query_latency_stats(
        self,
        machine_ids: list[str] | None = None,
        states: list[str] | None = None,
        line_names: list[str] | None = None,
        minutes: int = 10,
    ) -> pd.DataFrame:
        """Hop-by-hop latency stats from vw_pipeline_latency, filterable by device/state/line."""
        c = self.config.catalog
        s = self.config.schema
        filters = [f"device_ts >= current_timestamp() - INTERVAL {minutes} MINUTES"]
        if machine_ids:
            ids_str = ",".join(f"'{m}'" for m in machine_ids)
            filters.append(f"machine_id IN ({ids_str})")
        if states:
            st_str = ",".join(f"'{v}'" for v in states)
            filters.append(f"state IN ({st_str})")
        if line_names:
            ln_str = ",".join(f"'{v}'" for v in line_names)
            filters.append(f"line_name IN ({ln_str})")
        where = " AND ".join(filters)
        return self.query_sql(
            f"""
            SELECT
              machine_id,
              ROUND(AVG(hop1_device_to_iothub_ms), 0)    AS avg_d2h_ms,
              ROUND(AVG(hop2_iothub_to_zerobus_ms), 0)   AS avg_h2z_ms,
              ROUND(AVG(total_device_to_zerobus_ms), 0)   AS avg_total_ms,
              ROUND(MAX(hop1_device_to_iothub_ms), 0)     AS max_d2h_ms,
              ROUND(MAX(hop2_iothub_to_zerobus_ms), 0)    AS max_h2z_ms,
              ROUND(MAX(total_device_to_zerobus_ms), 0)   AS max_total_ms,
              ROUND(PERCENTILE(hop1_device_to_iothub_ms, 0.50), 0)  AS p50_d2h_ms,
              ROUND(PERCENTILE(hop1_device_to_iothub_ms, 0.95), 0)  AS p95_d2h_ms,
              ROUND(PERCENTILE(hop2_iothub_to_zerobus_ms, 0.50), 0) AS p50_h2z_ms,
              ROUND(PERCENTILE(hop2_iothub_to_zerobus_ms, 0.95), 0) AS p95_h2z_ms,
              ROUND(PERCENTILE(total_device_to_zerobus_ms, 0.50), 0) AS p50_total_ms,
              ROUND(PERCENTILE(total_device_to_zerobus_ms, 0.95), 0) AS p95_total_ms,
              COUNT(*) AS sample_count
            FROM {c}.{s}.vw_pipeline_latency
            WHERE {where}
            GROUP BY machine_id
            ORDER BY machine_id
            """
        )

    def query_pipeline_freshness(self) -> dict:
        """Return latest timestamps from each pipeline layer for freshness display."""
        c = self.config.catalog
        s = self.config.schema
        result: dict = {}
        try:
            df = self.query_sql(
                f"""
                SELECT
                  MAX(last_event_time)        AS latest_event,
                  MAX(telemetry_lag_ms)        AS max_lag_ms,
                  AVG(telemetry_lag_ms)        AS avg_lag_ms,
                  COUNT(DISTINCT machine_id)   AS machine_count,
                  COUNT(*)                     AS row_count
                FROM {c}.{s}.vw_machine_current_status
                """
            )
            if not df.empty and df.iloc[0]["latest_event"] is not None:
                row = df.iloc[0]
                latest = pd.Timestamp(row["latest_event"])
                if latest.tzinfo is None:
                    latest = latest.tz_localize("UTC")
                now = pd.Timestamp.now(tz="UTC")
                age_seconds = max(0, (now - latest).total_seconds())
                result["sql_latest_event"] = latest
                result["sql_age_seconds"] = age_seconds
                result["sql_max_lag_ms"] = row["max_lag_ms"]
                result["sql_avg_lag_ms"] = row["avg_lag_ms"]
                result["sql_machine_count"] = int(row["machine_count"])
                result["sql_row_count"] = int(row["row_count"])
        except Exception as exc:
            result["sql_error"] = str(exc)

        if self.lakebase_available():
            try:
                conn = psycopg2.connect(
                    host=self.config.lakebase_host,
                    port=self.config.lakebase_port,
                    dbname=self.config.lakebase_db,
                    user=self.config.lakebase_user,
                    password=self.config.lakebase_password,
                    sslmode="require",
                    connect_timeout=10,
                )
                try:
                    with conn.cursor() as cur:
                        ref = self._resolve_table(cur, "machine_current_status")
                        if ref:
                            cur.execute(psql.SQL(
                                "SELECT MAX(updated_at), COUNT(*) FROM {}.{}"
                            ).format(psql.Identifier(ref[0]), psql.Identifier(ref[1])))
                            row = cur.fetchone()
                            if row and row[0]:
                                lb_ts = row[0]
                                if lb_ts.tzinfo is None:
                                    lb_ts = lb_ts.replace(tzinfo=timezone.utc)
                                now = datetime.now(timezone.utc)
                                result["lb_latest_update"] = lb_ts
                                result["lb_age_seconds"] = max(0, (now - lb_ts).total_seconds())
                                result["lb_row_count"] = int(row[1])
                finally:
                    conn.close()
            except Exception as exc:
                result["lb_error"] = str(exc)

        return result

    def lakebase_available(self) -> bool:
        return bool(
            self.config.lakebase_host
            and self.config.lakebase_user
            and self.config.lakebase_password
        )

    def _resolve_table(self, cur, table_name: str) -> Optional[tuple[str, str]]:
        cur.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name = %s
              AND table_type = 'BASE TABLE'
            ORDER BY CASE WHEN table_schema = 'public' THEN 0 ELSE 1 END, table_schema
            LIMIT 1
            """,
            (table_name,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return row[0], row[1]

    def _query_mirror_metadata(self, cur) -> pd.DataFrame:
        table_ref = self._resolve_table(cur, "mirror_metadata")
        if not table_ref:
            raise RuntimeError(
                "Lakebase table machine_current_status is missing, and mirror_metadata is also unavailable."
            )
        query = psql.SQL(
            """
            SELECT
              instance_id,
              last_run_at,
              row_count,
              source_watermark,
              'mirror_metadata_fallback'::text AS source_kind
            FROM {}.{}
            ORDER BY last_run_at DESC
            LIMIT 20
            """
        ).format(psql.Identifier(table_ref[0]), psql.Identifier(table_ref[1]))
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return _localize_timestamps(pd.DataFrame(rows, columns=cols))

    def _lakebase_connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.config.lakebase_host,
            port=self.config.lakebase_port,
            dbname=self.config.lakebase_db,
            user=self.config.lakebase_user,
            password=self.config.lakebase_password,
            sslmode="require",
            connect_timeout=15,
        )

    def query_lakebase_machines(self) -> pd.DataFrame:
        """All machines with sensor data + line_name from dim_machine, one row per device."""
        if not self.lakebase_available():
            return pd.DataFrame()
        conn = self._lakebase_connect()
        try:
            with conn.cursor() as cur:
                status_ref = self._resolve_table(cur, "machine_current_status")
                if not status_ref:
                    raise RuntimeError("machine_current_status table not found in Lakebase")
                dim_ref = self._resolve_table(cur, "dim_machine")
                if dim_ref:
                    query = psql.SQL(
                        """
                        SELECT s.machine_id, s.state, s.last_event_time,
                               s.telemetry_lag_ms, s.ml_lag_ms,
                               s.temp_c, s.vibration_mm_s, s.throughput_cpm,
                               s.rpm, s.current_amps, s.humidity_pct,
                               s.load_pct, s.power_kw, s.power_factor,
                               s.voltage_v, s.pressure_bar, s.flow_rate_lpm,
                               s.oee_pct, s.anomaly_score, s.prob_fault_next_5m,
                               s.updated_at, d.line_name
                        FROM {s_schema}.{s_table} s
                        LEFT JOIN {d_schema}.{d_table} d
                          ON s.machine_id = d.machine_id
                        ORDER BY s.machine_id
                        """
                    ).format(
                        s_schema=psql.Identifier(status_ref[0]),
                        s_table=psql.Identifier(status_ref[1]),
                        d_schema=psql.Identifier(dim_ref[0]),
                        d_table=psql.Identifier(dim_ref[1]),
                    )
                else:
                    query = psql.SQL(
                        """
                        SELECT s.*, NULL::text AS line_name
                        FROM {s_schema}.{s_table} s
                        ORDER BY s.machine_id
                        """
                    ).format(
                        s_schema=psql.Identifier(status_ref[0]),
                        s_table=psql.Identifier(status_ref[1]),
                    )
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                return _localize_timestamps(pd.DataFrame(rows, columns=cols))
        finally:
            conn.close()

    def query_lakebase_status(self) -> pd.DataFrame:
        if not self.lakebase_available():
            return pd.DataFrame()
        conn = self._lakebase_connect()
        try:
            with conn.cursor() as cur:
                table_ref = self._resolve_table(cur, "machine_current_status")
                if not table_ref:
                    return self._query_mirror_metadata(cur)
                query = psql.SQL(
                    """
                    SELECT machine_id, state, prob_fault_next_5m, last_event_time, updated_at
                    FROM {}.{}
                    ORDER BY updated_at DESC
                    LIMIT 200
                    """
                ).format(psql.Identifier(table_ref[0]), psql.Identifier(table_ref[1]))
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                return _localize_timestamps(pd.DataFrame(rows, columns=cols))
        except psycopg2.OperationalError as exc:
            raise RuntimeError(f"Lakebase connectivity/auth error: {exc}") from exc
        finally:
            conn.close()

    # ── Service Requests ──────────────────────────────────────────────

    def _ensure_service_requests_table(self, cur) -> tuple[str, str]:
        """Create the table if missing and return its (schema, name) ref."""
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS service_requests (
              id            TEXT PRIMARY KEY,
              machine_id    TEXT NOT NULL,
              priority      TEXT NOT NULL,
              request_type  TEXT NOT NULL,
              description   TEXT,
              requestor     TEXT,
              status        TEXT NOT NULL DEFAULT 'OPEN',
              created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_service_requests_status
            ON service_requests(status, created_at DESC)
            """
        )
        ref = self._resolve_table(cur, "service_requests")
        if not ref:
            raise RuntimeError("service_requests table could not be resolved after creation")
        return ref

    def create_service_request(
        self,
        machine_ids: list[str],
        priority: str,
        request_type: str,
        description: str,
        requestor: str,
    ) -> str:
        """Insert one service request row per machine. Returns the batch UUID."""
        if not self.lakebase_available():
            raise RuntimeError("Lakebase is not configured")
        batch_id = uuid.uuid4().hex[:12]
        conn = self._lakebase_connect()
        try:
            with conn.cursor() as cur:
                ref = self._ensure_service_requests_table(cur)
                for idx, mid in enumerate(machine_ids):
                    row_id = f"{batch_id}-{idx}" if len(machine_ids) > 1 else batch_id
                    cur.execute(
                        psql.SQL(
                            "INSERT INTO {}.{} (id, machine_id, priority, request_type, description, requestor) "
                            "VALUES (%s, %s, %s, %s, %s, %s)"
                        ).format(psql.Identifier(ref[0]), psql.Identifier(ref[1])),
                        (row_id, mid, priority, request_type, description, requestor),
                    )
            conn.commit()
        finally:
            conn.close()
        return batch_id

    def query_service_requests(
        self,
        statuses: list[str] | None = None,
        priorities: list[str] | None = None,
        machine_ids: list[str] | None = None,
    ) -> pd.DataFrame:
        if not self.lakebase_available():
            return pd.DataFrame()
        conn = self._lakebase_connect()
        try:
            with conn.cursor() as cur:
                ref = self._resolve_table(cur, "service_requests")
                if not ref:
                    return pd.DataFrame()
                clauses: list[str] = []
                params: list = []
                if statuses:
                    clauses.append("status = ANY(%s)")
                    params.append(statuses)
                if priorities:
                    clauses.append("priority = ANY(%s)")
                    params.append(priorities)
                if machine_ids:
                    clauses.append("machine_id = ANY(%s)")
                    params.append(machine_ids)
                where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
                query = psql.SQL(
                    "SELECT id, machine_id, priority, request_type, description, "
                    "requestor, status, created_at, updated_at "
                    "FROM {}.{} " + where + " ORDER BY created_at DESC LIMIT 500"
                ).format(psql.Identifier(ref[0]), psql.Identifier(ref[1]))
                cur.execute(query, params)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                return _localize_timestamps(pd.DataFrame(rows, columns=cols))
        finally:
            conn.close()

    def update_service_request_status(self, request_id: str, new_status: str) -> None:
        if not self.lakebase_available():
            raise RuntimeError("Lakebase is not configured")
        conn = self._lakebase_connect()
        try:
            with conn.cursor() as cur:
                ref = self._resolve_table(cur, "service_requests")
                if not ref:
                    raise RuntimeError("service_requests table not found")
                cur.execute(
                    psql.SQL(
                        "UPDATE {}.{} SET status = %s, updated_at = NOW() WHERE id = %s"
                    ).format(psql.Identifier(ref[0]), psql.Identifier(ref[1])),
                    (new_status, request_id),
                )
            conn.commit()
        finally:
            conn.close()

    # ── AI-Generated Descriptions ─────────────────────────────────────

    def generate_sr_description(
        self,
        machine_ids: list[str],
        request_type: str,
    ) -> str:
        """Use ai_query() to generate a service request description from live ML and sensor data."""
        c = self.config.catalog
        s = self.config.schema
        ids_str = ",".join(f"'{m}'" for m in machine_ids)
        df = self.query_sql(
            f"""
            WITH machine_data AS (
              SELECT
                machine_id,
                COALESCE(state, 'UNKNOWN') AS state,
                ROUND(COALESCE(vibration_mm_s, 0), 2) AS vibration_mm_s,
                ROUND(COALESCE(temp_c, 0), 1) AS temp_c,
                ROUND(COALESCE(current_amps, 0), 2) AS current_amps,
                COALESCE(throughput_cpm, 0) AS throughput_cpm,
                COALESCE(rpm, 0) AS rpm,
                ROUND(COALESCE(oee_pct, 0), 1) AS oee_pct,
                ROUND(COALESCE(anomaly_score, 0), 3) AS anomaly_score,
                ROUND(COALESCE(prob_fault_next_5m, 0), 3) AS prob_fault_5m,
                ROUND(COALESCE(prob_fault_next_1h, 0), 3) AS prob_fault_1h,
                CASE
                  WHEN vibration_mm_s > 9.5 THEN 'OVER THRESHOLD (>9.5)'
                  WHEN vibration_mm_s > 8.0 THEN 'WARNING (>8.0)'
                  ELSE 'OK'
                END AS vibration_status,
                CASE
                  WHEN temp_c > 85 THEN 'OVER THRESHOLD (>85C)'
                  WHEN temp_c > 75 THEN 'WARNING (>75C)'
                  ELSE 'OK'
                END AS temp_status,
                CASE
                  WHEN current_amps > 12 THEN 'OVER THRESHOLD (>12A)'
                  WHEN current_amps > 10 THEN 'WARNING (>10A)'
                  ELSE 'OK'
                END AS current_status
              FROM {c}.{s}.vw_machine_current_status
              WHERE machine_id IN ({ids_str})
            ),
            summary AS (
              SELECT CONCAT_WS(CHAR(10),
                COLLECT_LIST(
                  CONCAT_WS('',
                    'Machine ', machine_id, ': state=', state,
                    ', vibration=', CAST(vibration_mm_s AS STRING), ' mm/s [', vibration_status, ']',
                    ', temp=', CAST(temp_c AS STRING), 'C [', temp_status, ']',
                    ', current=', CAST(current_amps AS STRING), 'A [', current_status, ']',
                    ', throughput=', CAST(throughput_cpm AS STRING), ' cpm',
                    ', rpm=', CAST(rpm AS STRING),
                    ', OEE=', CAST(oee_pct AS STRING), '%',
                    ', anomaly_score=', CAST(anomaly_score AS STRING),
                    ', fault_risk_5m=', CAST(prob_fault_5m AS STRING),
                    ', fault_risk_1h=', CAST(prob_fault_1h AS STRING)
                  )
                )
              ) AS machine_summary
              FROM machine_data
            )
            SELECT ai_query(
              'databricks-meta-llama-3-3-70b-instruct',
              CONCAT(
                'You are a manufacturing maintenance assistant writing a service request. ',
                'Based on the machine conditions below, write a brief 2-3 sentence description ',
                'explaining WHY this {request_type} service request is needed. ',
                'Cite specific sensor readings, ML risk scores, and threshold violations. ',
                'Be concise and factual — this will be read by a maintenance technician.\n\n',
                machine_summary
              )
            ) AS description
            FROM summary
            """
        )
        if df.empty or df.iloc[0]["description"] is None:
            raise RuntimeError("ai_query returned no result")
        raw = df.iloc[0]["description"]
        # ai_query may return a string or a struct — extract the text
        if isinstance(raw, dict):
            raw = raw.get("text") or raw.get("candidates", [{}])[0].get("text", "")
        return str(raw).strip()
