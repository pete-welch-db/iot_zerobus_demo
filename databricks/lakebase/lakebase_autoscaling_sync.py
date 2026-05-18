"""
Sync gold_machine_latest_status -> Autoscaling Lakebase.

Triggered by table updates on the gold table.
Uses the /api/2.0/postgres/credentials endpoint for Autoscaling auth.
"""

import argparse
import logging
import time

import psycopg
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("lakebase-autoscaling-sync")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--source-table", default="gold_machine_latest_status")
    parser.add_argument("--dest-table", default="machine_current_status")
    parser.add_argument("--dest-schema", default="iot_demo")
    parser.add_argument("--lakebase-host", required=True)
    parser.add_argument("--lakebase-db", default="iot_demo")
    parser.add_argument("--lakebase-port", type=int, default=5432)
    parser.add_argument("--lakebase-endpoint", required=True,
                        help="e.g. projects/iot-demo-lakebase/branches/production/endpoints/primary")
    return parser.parse_args()


def _get_credential(workspace_url: str, api_token: str, endpoint: str) -> str:
    resp = requests.post(
        f"{workspace_url.rstrip('/')}/api/2.0/postgres/credentials",
        headers={"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"},
        json={"endpoint": endpoint},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("token", "")
    if not token:
        raise RuntimeError("Empty token from postgres/credentials")
    return token


def main() -> None:
    args = parse_args()
    start = time.time()

    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    workspace_url = ctx.apiUrl().get()
    api_token = ctx.apiToken().get()
    user = ctx.userName().get()

    source = f"{args.catalog}.{args.schema}.{args.source_table}"
    LOGGER.info("Reading %s", source)

    df = spark.table(source)
    columns = df.columns
    rows = df.collect()
    LOGGER.info("Collected %d rows", len(rows))

    token = _get_credential(workspace_url, api_token, args.lakebase_endpoint)

    conninfo = (
        f"host={args.lakebase_host} port={args.lakebase_port} "
        f"dbname={args.lakebase_db} user={user} password={token} sslmode=require"
    )

    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    update_set = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in columns if c != "machine_id"
    )
    upsert_sql = (
        f"INSERT INTO {args.dest_schema}.{args.dest_table} ({col_list}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (machine_id) DO UPDATE SET {update_set}"
    )

    with psycopg.connect(conninfo, connect_timeout=30) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {args.dest_schema}.{args.dest_table} ("
                + ", ".join(
                    f"{c} {'TEXT PRIMARY KEY' if c == 'machine_id' else 'TEXT'}"
                    for c in columns
                )
                + ")"
            )
            conn.commit()

            for row in rows:
                vals = [str(v) if v is not None else None for v in row]
                cur.execute(upsert_sql, vals)
            conn.commit()

        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*), max(last_event_time) FROM {args.dest_schema}.{args.dest_table}")
            r = cur.fetchone()
            LOGGER.info("Synced %d rows, latest=%s (%.1fs)", r[0], r[1], time.time() - start)


if __name__ == "__main__":
    main()
