from __future__ import annotations

import logging
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Generator, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
from psycopg import sql as psql
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient

from config import AppConfig

logger = logging.getLogger(__name__)

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


# ── Lakebase connection factory (psycopg3 + SDK credential rotation) ──


class LakebaseConnectionFactory:
    """Creates psycopg3 connections with auto-rotating OAuth credentials.

    Each call to ``connection()`` generates a fresh short-lived token via
    the Databricks SDK, so expired credentials are never reused.
    """

    def __init__(self, instance_name: str, db_name: str, port: int = 5432):
        self._w = WorkspaceClient()
        self._instance_name = instance_name
        self._db_name = db_name
        self._port = port

        instance = self._w.database.get_database_instance(name=instance_name)
        self._host = instance.read_write_dns
        self._user = self._w.current_user.me().user_name
        logger.info(
            "Lakebase factory: host=%s user=%s db=%s",
            self._host, self._user, self._db_name,
        )

    def _generate_token(self) -> str:
        cred = self._w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[self._instance_name],
        )
        return cred.token

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        """Yield a fresh psycopg3 connection with a newly generated token."""
        conn = psycopg.connect(
            host=self._host,
            port=self._port,
            dbname=self._db_name,
            user=self._user,
            password=self._generate_token(),
            sslmode="require",
            connect_timeout=15,
        )
        try:
            yield conn
        finally:
            conn.close()


# ── DataClients ──────────────────────────────────────────────────────


@dataclass
class DataClients:
    config: AppConfig
    _lb_factory: Optional[LakebaseConnectionFactory] = field(default=None, init=False, repr=False)
    _lb_available: Optional[bool] = field(default=None, init=False, repr=False)
    _table_ref_cache: dict = field(default_factory=dict, init=False, repr=False)
    _sr_table_ensured: bool = field(default=False, init=False, repr=False)
    _ml_cache: Optional[tuple] = field(default=None, init=False, repr=False)  # (monotonic_ts, DataFrame)

    def query_sql(self, statement: str, parameters: dict | None = None) -> pd.DataFrame:
        with dbsql.connect(
            server_hostname=self.config.workspace_host.replace("https://", ""),
            http_path=self.config.sql_http_path,
            access_token=self.config.token,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(statement, parameters=parameters)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        return _localize_timestamps(pd.DataFrame(rows, columns=cols))

    def _build_in_clause(self, values: list[str], prefix: str) -> tuple[str, dict]:
        """Build a parameterized IN clause. Returns (sql_fragment, params_dict)."""
        params = {f"{prefix}{i}": v for i, v in enumerate(values)}
        placeholders = ", ".join(f":{k}" for k in params)
        return f"({placeholders})", params

    # ── SQL Warehouse queries ─────────────────────────────────────────

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
        all_params: dict = {}
        filters = [f"device_ts >= current_timestamp() - INTERVAL {minutes} MINUTES"]
        if machine_ids:
            in_sql, p = self._build_in_clause(machine_ids, "mid")
            filters.append(f"machine_id IN {in_sql}")
            all_params.update(p)
        if states:
            in_sql, p = self._build_in_clause(states, "st")
            filters.append(f"state IN {in_sql}")
            all_params.update(p)
        if line_names:
            in_sql, p = self._build_in_clause(line_names, "ln")
            filters.append(f"line_name IN {in_sql}")
            all_params.update(p)
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
            """,
            parameters=all_params or None,
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
                with self._get_lb_factory().connection() as conn:
                    with conn.cursor() as cur:
                        ref = self._resolve_table(cur, "machine_current_status")
                        if ref:
                            cur.execute(psql.SQL(
                                "SELECT MAX(last_event_time), COUNT(*) FROM {}.{}"
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
            except Exception as exc:
                result["lb_error"] = str(exc)

        return result

    # ── Lakebase connection management ────────────────────────────────

    def lakebase_available(self) -> bool:
        if self._lb_available is not None:
            return self._lb_available
        if not self.config.lakebase_instance_name:
            self._lb_available = False
            return False
        try:
            self._get_lb_factory()
            self._lb_available = True
        except Exception as exc:
            logger.warning("Lakebase unavailable: %s", exc)
            self._lb_available = False
        return self._lb_available

    def _get_lb_factory(self) -> LakebaseConnectionFactory:
        if self._lb_factory is None:
            self._lb_factory = LakebaseConnectionFactory(
                instance_name=self.config.lakebase_instance_name,
                db_name=self.config.lakebase_db,
                port=self.config.lakebase_port,
            )
        return self._lb_factory

    def _resolve_table(self, cur, table_name: str) -> Optional[tuple[str, str]]:
        if table_name in self._table_ref_cache:
            return self._table_ref_cache[table_name]
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
        ref = (row[0], row[1])
        self._table_ref_cache[table_name] = ref
        return ref

    # ── ML enrichment cache (SQL Warehouse, 30s TTL) ────────────────

    def _get_ml_enrichment(self) -> pd.DataFrame:
        """Cached ML enrichment from SQL Warehouse (OEE, anomaly, fault risk, line_name).

        The gold table synced to Lakebase via CONTINUOUS mode only has raw
        telemetry.  ML scores and dim_machine line_name come from the enriched
        view, cached here with a 30-second TTL since ML batch scores change
        infrequently.
        """
        import time as _time
        now = _time.monotonic()
        if self._ml_cache and (now - self._ml_cache[0]) < 30:
            return self._ml_cache[1]
        c = self.config.catalog
        s = self.config.schema
        df = self.query_sql(
            f"""
            SELECT machine_id, line_name,
                   oee_pct, availability_pct, performance_pct, quality_pct,
                   anomaly_score, prob_fault_next_5m,
                   anomaly_inference_type, fault_inference_type
            FROM {c}.{s}.vw_machine_current_status
            """
        )
        self._ml_cache = (now, df)
        return df

    # ── Lakebase machine queries ──────────────────────────────────────

    def query_lakebase_machines(self) -> pd.DataFrame:
        """Real-time telemetry from Lakebase + cached ML enrichment from SQL Warehouse.

        Lakebase is synced via CONTINUOUS mode from gold_machine_current_status
        (near-real-time telemetry).  ML scores, OEE, and line_name are merged
        from a cached SQL Warehouse query against the enriched view.
        """
        if not self.lakebase_available():
            return pd.DataFrame()
        with self._get_lb_factory().connection() as conn:
            with conn.cursor() as cur:
                ref = self._resolve_table(cur, "machine_current_status")
                if not ref:
                    raise RuntimeError("machine_current_status table not found in Lakebase")
                query = psql.SQL(
                    """
                    SELECT machine_id, state, last_event_time,
                           temp_c, vibration_mm_s, throughput_cpm,
                           rpm, current_amps, humidity_pct,
                           load_pct, power_kw, power_factor,
                           voltage_v, pressure_bar, flow_rate_lpm,
                           fault_code, iothub_device_id
                    FROM {}.{}
                    ORDER BY machine_id
                    """
                ).format(psql.Identifier(ref[0]), psql.Identifier(ref[1]))
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                lb_df = _localize_timestamps(pd.DataFrame(rows, columns=cols))

        if lb_df.empty:
            return lb_df

        # Compute telemetry lag client-side for accurate real-time display
        # (the gold table's pre-computed lag reflects DLT write time, not read time)
        if "last_event_time" in lb_df.columns and not lb_df["last_event_time"].empty:
            now = pd.Timestamp.now(tz="UTC")
            evt = pd.to_datetime(lb_df["last_event_time"], utc=True, errors="coerce")
            lag_seconds = (now - evt).dt.total_seconds()
            lb_df["telemetry_lag_ms"] = pd.to_numeric(lag_seconds * 1000, errors="coerce")
        else:
            lb_df["telemetry_lag_ms"] = None

        # Merge ML enrichment from cached SQL Warehouse query
        try:
            ml_df = self._get_ml_enrichment()
            if not ml_df.empty:
                lb_df = lb_df.merge(ml_df, on="machine_id", how="left")
            else:
                logger.warning("ML enrichment returned empty DataFrame")
        except Exception as exc:
            logger.error("ML enrichment query failed: %s", exc, exc_info=True)
        # Ensure expected columns exist even if ML enrichment failed or returned empty
        for col in ["oee_pct", "anomaly_score", "prob_fault_next_5m", "line_name",
                     "availability_pct", "performance_pct", "quality_pct"]:
            if col not in lb_df.columns:
                lb_df[col] = None

        return lb_df

    def query_lakebase_status(self) -> pd.DataFrame:
        if not self.lakebase_available():
            return pd.DataFrame()
        with self._get_lb_factory().connection() as conn:
            with conn.cursor() as cur:
                ref = self._resolve_table(cur, "machine_current_status")
                if not ref:
                    return pd.DataFrame()
                query = psql.SQL(
                    """
                    SELECT machine_id, state, last_event_time
                    FROM {}.{}
                    ORDER BY last_event_time DESC
                    LIMIT 200
                    """
                ).format(psql.Identifier(ref[0]), psql.Identifier(ref[1]))
                cur.execute(query)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                return _localize_timestamps(pd.DataFrame(rows, columns=cols))

    # ── Service Requests ──────────────────────────────────────────────

    def _ensure_service_requests_table(self, cur) -> tuple[str, str]:
        """Create the table if missing and return its (schema, name) ref."""
        if self._sr_table_ensured:
            ref = self._table_ref_cache.get("service_requests")
            if ref:
                return ref
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
        self._sr_table_ensured = True
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
        with self._get_lb_factory().connection() as conn:
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
        return batch_id

    def query_service_requests(
        self,
        statuses: list[str] | None = None,
        priorities: list[str] | None = None,
        machine_ids: list[str] | None = None,
    ) -> pd.DataFrame:
        if not self.lakebase_available():
            return pd.DataFrame()
        with self._get_lb_factory().connection() as conn:
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

    def update_service_request_status(self, request_id: str, new_status: str) -> None:
        if not self.lakebase_available():
            raise RuntimeError("Lakebase is not configured")
        with self._get_lb_factory().connection() as conn:
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

    # ── AI-Generated Descriptions ─────────────────────────────────────

    _VALID_REQUEST_TYPES = frozenset({"PREVENTIVE", "CORRECTIVE", "INSPECTION", "CALIBRATION"})
    _MACHINE_ID_RE = __import__("re").compile(r"^MC-\d{4}$")

    def generate_sr_description(
        self,
        machine_ids: list[str],
        request_type: str,
    ) -> str:
        """Use ai_query() to generate a service request description from live ML and sensor data."""
        # Validate inputs (these come from UI dropdowns, not free text)
        if request_type not in self._VALID_REQUEST_TYPES:
            raise ValueError(f"Invalid request_type: {request_type}")
        for mid in machine_ids:
            if not self._MACHINE_ID_RE.match(mid):
                raise ValueError(f"Invalid machine_id: {mid}")

        c = self.config.catalog
        s = self.config.schema
        in_list = ", ".join(f"'{mid}'" for mid in machine_ids)
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
              WHERE machine_id IN ({in_list})
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
                'Be concise and factual — this will be read by a maintenance technician.\\n\\n',
                machine_summary
              )
            ) AS description
            FROM summary
            """
        )
        if df.empty or df.iloc[0]["description"] is None:
            raise RuntimeError("ai_query returned no result")
        raw = df.iloc[0]["description"]
        if isinstance(raw, dict):
            raw = raw.get("text") or raw.get("candidates", [{}])[0].get("text", "")
        return str(raw).strip()
