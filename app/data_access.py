from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2 import sql as psql
from databricks import sql as dbsql

from config import AppConfig


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
        return pd.DataFrame(rows, columns=cols)

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
        return pd.DataFrame(rows, columns=cols)

    def query_lakebase_status(self) -> pd.DataFrame:
        if not self.lakebase_available():
            return pd.DataFrame()
        conn = psycopg2.connect(
            host=self.config.lakebase_host,
            port=self.config.lakebase_port,
            dbname=self.config.lakebase_db,
            user=self.config.lakebase_user,
            password=self.config.lakebase_password,
            sslmode="require",
        )
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
                return pd.DataFrame(rows, columns=cols)
        except psycopg2.OperationalError as exc:
            raise RuntimeError(f"Lakebase connectivity/auth error: {exc}") from exc
        finally:
            conn.close()
