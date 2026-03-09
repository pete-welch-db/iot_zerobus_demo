"""
Validate post-run data health for the demo workflow.
"""

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate post-run output tables.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args()


def count_rows(table_name: str) -> int:
    row = spark.sql(f"SELECT COUNT(*) AS row_count FROM {table_name}").collect()[0]
    return int(row["row_count"])


def max_timestamp(table_name: str, column_name: str):
    row = spark.sql(f"SELECT MAX({column_name}) AS max_ts FROM {table_name}").collect()[0]
    return row["max_ts"]


def main() -> None:
    args = parse_args()
    c = args.catalog
    s = args.schema

    required_positive = {
        f"{c}.{s}.bronze_iot_telemetry": count_rows(f"{c}.{s}.bronze_iot_telemetry"),
        f"{c}.{s}.silver_machine_telemetry": count_rows(f"{c}.{s}.silver_machine_telemetry"),
        f"{c}.{s}.ml_anomaly_scores": count_rows(f"{c}.{s}.ml_anomaly_scores"),
        f"{c}.{s}.ml_fault_predictions": count_rows(f"{c}.{s}.ml_fault_predictions"),
    }

    failed = [name for name, n in required_positive.items() if n <= 0]
    if failed:
        raise ValueError(f"Post-run validation failed. Zero-row tables: {', '.join(failed)}")

    gold_table = f"{c}.{s}.gold_machine_health_5m"
    gold_count = count_rows(gold_table)
    if gold_count <= 0:
        print(
            "Warning: gold_machine_health_5m has 0 rows. "
            "This can happen with watermark/window timing in short demo runs."
        )

    latest_event = max_timestamp(f"{c}.{s}.silver_machine_telemetry", "event_time")
    latest_ml = max_timestamp(f"{c}.{s}.ml_fault_predictions", "scored_at")

    print("Post-run validation passed.")
    print(f"Latest event_time: {latest_event}")
    print(f"Latest ML scored_at: {latest_ml}")
    print(f"Gold row count: {gold_count}")


if __name__ == "__main__":
    main()
