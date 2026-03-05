"""
Execute dashboard source queries after DLT/semantic refresh.

Lakeview dashboards can be configured with scheduled refreshes, but this demo
refreshes data on-demand in the job graph immediately after medallion updates.
"""

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh dashboard source datasets.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    catalog = args.catalog
    schema = args.schema

    checks = {
        "live_rows": f"SELECT COUNT(*) AS c FROM {catalog}.{schema}.vw_machine_telemetry_live",
        "health_rows": f"SELECT COUNT(*) AS c FROM {catalog}.{schema}.vw_machine_health",
        "status_rows": f"SELECT COUNT(*) AS c FROM {catalog}.{schema}.vw_machine_current_status",
        "downtime_rows": (
            "SELECT COUNT(*) AS c FROM "
            f"(SELECT machine_id, window_end FROM {catalog}.{schema}.vw_machine_health) t"
        ),
    }

    results = {}
    for key, query in checks.items():
        row = spark.sql(query).first()
        results[key] = int(row["c"]) if row and row["c"] is not None else 0

    print(f"Dashboard source refresh complete for {catalog}.{schema}: {results}")


if __name__ == "__main__":
    main()
