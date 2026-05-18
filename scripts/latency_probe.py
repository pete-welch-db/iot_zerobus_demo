#!/usr/bin/env python3
"""Polls Lakebase for a target machine every 1s, recording when last_event_time
changes and computing the true device-to-Lakebase lag visible in Machine Explorer.

Usage:
    python scripts/latency_probe.py [--machine MC-0001] [--duration 520] [--output latency_samples.csv]
"""
from __future__ import annotations

import argparse
import csv
import statistics
import time
import uuid
from datetime import datetime, timezone

import psycopg
from databricks.sdk import WorkspaceClient


def main():
    parser = argparse.ArgumentParser(description="Lakebase latency probe")
    parser.add_argument("--machine", default="MC-0001")
    parser.add_argument("--duration", type=int, default=520,
                        help="How long to poll in seconds (slightly longer than simulator)")
    parser.add_argument("--output", default="latency_samples.csv")
    parser.add_argument("--instance-name", default="iot-demo-lakebase")
    parser.add_argument("--db-name", default="iot_demo")
    args = parser.parse_args()

    w = WorkspaceClient()
    instance = w.database.get_database_instance(name=args.instance_name)
    host = instance.read_write_dns
    user = w.current_user.me().user_name

    def fresh_conn():
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[args.instance_name],
        )
        return psycopg.connect(
            host=host, port=5432, dbname=args.db_name,
            user=user, password=cred.token, sslmode="require",
            connect_timeout=15,
        )

    print(f"[probe] Target machine: {args.machine}")
    print(f"[probe] Polling Lakebase for {args.duration}s ...")

    samples: list[dict] = []
    prev_event_time = None
    conn = fresh_conn()
    token_age = time.time()
    end_time = time.time() + args.duration

    while time.time() < end_time:
        # Refresh connection every 4 minutes (token lifetime)
        if time.time() - token_age > 240:
            conn.close()
            conn = fresh_conn()
            token_age = time.time()

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT last_event_time FROM iot_demo.machine_current_status "
                    "WHERE machine_id = %s",
                    (args.machine,),
                )
                row = cur.fetchone()
        except Exception as exc:
            print(f"[probe] query error: {exc}")
            conn.close()
            conn = fresh_conn()
            token_age = time.time()
            time.sleep(1)
            continue

        if row and row[0]:
            event_time = row[0]
            if event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=timezone.utc)

            now = datetime.now(timezone.utc)
            lag_ms = (now - event_time).total_seconds() * 1000

            if prev_event_time is None or event_time != prev_event_time:
                if prev_event_time is not None:
                    sample = {
                        "poll_time_utc": now.isoformat(),
                        "device_event_time_utc": event_time.isoformat(),
                        "lag_ms": round(lag_ms, 1),
                    }
                    samples.append(sample)
                    print(
                        f"[probe] UPDATE detected: event={event_time.strftime('%H:%M:%S.%f')[:-3]} "
                        f"lag={lag_ms:.0f}ms  (sample #{len(samples)})"
                    )
                else:
                    print(f"[probe] Initial value: event={event_time.strftime('%H:%M:%S.%f')[:-3]}")
                prev_event_time = event_time

        time.sleep(1)

    conn.close()

    if samples:
        lags = [s["lag_ms"] for s in samples]
        lags_sorted = sorted(lags)
        p50_idx = int(len(lags_sorted) * 0.50)
        p95_idx = min(int(len(lags_sorted) * 0.95), len(lags_sorted) - 1)

        print("\n" + "=" * 60)
        print(f"LAKEBASE LATENCY PROBE RESULTS  (machine={args.machine})")
        print("=" * 60)
        print(f"  Samples collected : {len(samples)}")
        print(f"  Min lag           : {min(lags):,.0f} ms")
        print(f"  P50 lag           : {lags_sorted[p50_idx]:,.0f} ms")
        print(f"  Mean lag          : {statistics.mean(lags):,.0f} ms")
        print(f"  P95 lag           : {lags_sorted[p95_idx]:,.0f} ms")
        print(f"  Max lag           : {max(lags):,.0f} ms")
        print(f"  Std dev           : {statistics.stdev(lags):,.0f} ms" if len(lags) > 1 else "")
        print("=" * 60)

        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["poll_time_utc", "device_event_time_utc", "lag_ms"])
            writer.writeheader()
            writer.writerows(samples)
        print(f"  Samples saved to  : {args.output}")
    else:
        print("[probe] No update events detected during the polling window.")


if __name__ == "__main__":
    main()
