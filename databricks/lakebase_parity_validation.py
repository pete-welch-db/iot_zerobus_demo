"""
Validate OLAP vs Lakebase OLTP parity for mirrored current-state rows.
"""

import argparse

import psycopg2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Lakebase parity against vw_machine_current_status.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--user-secret-key", required=True)
    parser.add_argument("--password-secret-key", required=True)
    return parser.parse_args()


def _jdbc_to_pg_dsn(jdbc_url: str, user: str, password: str) -> str:
    if not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError(f"Unsupported JDBC URL format: {jdbc_url}")
    dsn = jdbc_url.replace("jdbc:", "", 1)
    return f"{dsn}?sslmode=require&user={user}&password={password}"


def main() -> None:
    args = parse_args()
    view_name = f"{args.catalog}.{args.schema}.vw_machine_current_status"
    olap_count = spark.table(view_name).count()
    olap_max_event = spark.table(view_name).agg({"last_event_time": "max"}).collect()[0][0]

    user = dbutils.secrets.get(scope=args.secret_scope, key=args.user_secret_key)
    password = dbutils.secrets.get(scope=args.secret_scope, key=args.password_secret_key)
    conn = psycopg2.connect(_jdbc_to_pg_dsn(args.jdbc_url, user, password))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MAX(last_event_time), MAX(updated_at) FROM machine_current_status")
            oltp_count, oltp_max_event, oltp_updated = cur.fetchone()
    finally:
        conn.close()

    print(f"OLAP rows={olap_count} OLTP rows={oltp_count}")
    print(f"OLAP max_event={olap_max_event} OLTP max_event={oltp_max_event} OLTP updated_at={oltp_updated}")
    if int(olap_count) != int(oltp_count):
        raise RuntimeError("Parity check failed: row counts differ.")


if __name__ == "__main__":
    main()
