"""
Latency proof notebook (python script format) for real-time demo narration.
"""

import argparse
import time

from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show telemetry + ML lag in milliseconds.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--loop-seconds", type=int, default=0, help="Optional live loop duration.")
    parser.add_argument("--sleep-seconds", type=int, default=5, help="Loop sleep interval.")
    parser.add_argument("--top-devices", type=int, default=20)
    return parser.parse_args()


def snapshot_status(view_name: str, top_devices: int) -> None:
    df = spark.table(view_name)
    print("=== Fleet latency summary (ms) ===")
    df.select(
        F.count("*").alias("device_count"),
        F.expr("percentile_approx(telemetry_lag_ms, 0.5)").alias("telemetry_p50_ms"),
        F.expr("percentile_approx(telemetry_lag_ms, 0.95)").alias("telemetry_p95_ms"),
        F.expr("percentile_approx(telemetry_lag_ms, 0.99)").alias("telemetry_p99_ms"),
        F.expr("percentile_approx(ml_lag_ms, 0.5)").alias("ml_p50_ms"),
        F.expr("percentile_approx(ml_lag_ms, 0.95)").alias("ml_p95_ms"),
        F.expr("percentile_approx(ml_lag_ms, 0.99)").alias("ml_p99_ms"),
    ).show(truncate=False)

    print("=== Per-device spread (worst telemetry lag first) ===")
    (
        df.select(
            "machine_id",
            "state",
            "last_event_time",
            "telemetry_lag_ms",
            "ml_lag_ms",
            "prob_fault_next_5m",
            "anomaly_score",
        )
        .orderBy(F.col("telemetry_lag_ms").desc(), F.col("ml_lag_ms").desc())
        .limit(top_devices)
        .show(truncate=False)
    )


def main() -> None:
    args = parse_args()
    view_name = f"{args.catalog}.{args.schema}.vw_machine_current_status"
    if args.loop_seconds <= 0:
        snapshot_status(view_name, args.top_devices)
        return

    start = time.time()
    while time.time() - start < args.loop_seconds:
        print("")
        print(f"=== Live snapshot @ {int(time.time())} ===")
        snapshot_status(view_name, args.top_devices)
        time.sleep(max(1, args.sleep_seconds))


if __name__ == "__main__":
    main()
