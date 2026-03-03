import argparse
import csv
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List


@dataclass
class GeneratorConfig:
    num_devices: int
    samples_per_device: int
    sample_interval_seconds: int
    temp_fault_threshold: float
    vibration_fault_threshold: float
    seed: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic IoT training data with threshold-triggered faults.")
    parser.add_argument("--num-devices", type=int, default=100)
    parser.add_argument("--samples-per-device", type=int, default=5000)
    parser.add_argument("--sample-interval-seconds", type=int, default=5)
    parser.add_argument("--temp-fault-threshold", type=float, default=85.0)
    parser.add_argument("--vibration-fault-threshold", type=float, default=9.5)
    parser.add_argument("--output-jsonl", default="synthetic_fault_training.jsonl")
    parser.add_argument("--output-csv", default="synthetic_fault_training.csv")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def make_device_ids(count: int) -> List[str]:
    return [f"MACH_{i:04d}" for i in range(1, count + 1)]


def generate_rows_for_device(
    machine_id: str,
    start_ts: datetime,
    cfg: GeneratorConfig,
    rnd: random.Random,
) -> Iterable[Dict]:
    temp = rnd.uniform(58.0, 67.0)
    vibration = rnd.uniform(2.0, 4.0)
    throughput = rnd.randint(85, 110)
    rpm = rnd.randint(1800, 2400)
    current_amps = rnd.uniform(4.0, 7.0)
    humidity_pct = rnd.uniform(35.0, 55.0)

    cycle = 600
    normal_cutoff = int(cycle * 0.55)
    warning_cutoff = int(cycle * 0.85)
    fault_cutoff = int(cycle * 0.95)

    rows: List[Dict] = []
    for idx in range(cfg.samples_per_device):
        ts = start_ts + timedelta(seconds=idx * cfg.sample_interval_seconds)
        phase = idx % cycle

        if phase < normal_cutoff:
            state = "RUN"
            fault_code = None
            temp = max(35.0, min(80.0, temp + rnd.uniform(-0.5, 0.5)))
            vibration = max(0.8, min(8.5, vibration + rnd.uniform(-0.25, 0.25)))
            throughput = max(70, min(120, throughput + rnd.randint(-2, 2)))
            rpm = max(1200, min(2800, rpm + rnd.randint(-20, 20)))
            current_amps = max(3.0, min(10.0, current_amps + rnd.uniform(-0.2, 0.2)))
            humidity_pct = max(25.0, min(80.0, humidity_pct + rnd.uniform(-0.5, 0.5)))
        elif phase < warning_cutoff:
            state = "RUN"
            fault_code = None
            p = (phase - normal_cutoff) / max(1, (warning_cutoff - normal_cutoff))
            temp = 74.0 + p * (cfg.temp_fault_threshold + 6.0 - 74.0) + rnd.uniform(-0.3, 0.3)
            vibration = 6.5 + p * (cfg.vibration_fault_threshold + 1.8 - 6.5) + rnd.uniform(-0.2, 0.2)
            throughput = max(25, int(95 - 55 * p + rnd.uniform(-2, 2)))
            rpm = int(2400 + p * 600 + rnd.uniform(-15, 15))
            current_amps = 7.0 + p * 5.5 + rnd.uniform(-0.3, 0.3)
            humidity_pct = 50.0 + p * 20.0 + rnd.uniform(-1, 1)
        elif phase < fault_cutoff:
            state = "FAULT"
            if current_amps >= 12.0:
                fault_code = "OVERCURRENT"
            elif temp >= cfg.temp_fault_threshold:
                fault_code = "F_OVERHEAT"
            else:
                fault_code = "F_VIBRATION"
            temp = max(cfg.temp_fault_threshold + 0.1, temp + rnd.uniform(-0.4, 0.8))
            vibration = max(cfg.vibration_fault_threshold + 0.1, vibration + rnd.uniform(-0.3, 0.6))
            throughput = rnd.randint(0, 10)
            rpm = rnd.randint(0, 200)
            current_amps = max(12.0, current_amps + rnd.uniform(-0.5, 1.0))
            humidity_pct = max(60.0, humidity_pct + rnd.uniform(-0.5, 1.5))
        else:
            state = "STOPPED"
            fault_code = None
            temp = max(45.0, temp - rnd.uniform(0.4, 1.1))
            vibration = max(1.0, vibration - rnd.uniform(0.3, 0.7))
            throughput = rnd.randint(0, 6)
            rpm = 0
            current_amps = max(0.5, current_amps - rnd.uniform(0.3, 0.8))
            humidity_pct = max(30.0, humidity_pct - rnd.uniform(0.3, 0.8))

        is_fault = state == "FAULT"
        threshold_crossed = (
            temp >= cfg.temp_fault_threshold
            or vibration >= cfg.vibration_fault_threshold
            or current_amps >= 12.0
        )
        row = {
            "machine_id": machine_id,
            "event_time": ts.isoformat().replace("+00:00", "Z"),
            "vibration_mm_s": round(vibration, 4),
            "temp_c": round(temp, 4),
            "throughput_cpm": int(throughput),
            "rpm": int(rpm),
            "current_amps": round(current_amps, 4),
            "humidity_pct": round(humidity_pct, 1),
            "state": state,
            "fault_code": fault_code,
            "is_fault": is_fault,
            "threshold_crossed": threshold_crossed,
            "temp_fault_threshold": cfg.temp_fault_threshold,
            "vibration_fault_threshold": cfg.vibration_fault_threshold,
        }
        rows.append(row)

    # Build supervised label: whether fault appears in next 5 minutes.
    horizon_steps = max(1, int(300 / cfg.sample_interval_seconds))
    future_fault_flags = [1 if r["is_fault"] else 0 for r in rows]
    for i in range(len(rows)):
        future_window = future_fault_flags[i + 1 : i + 1 + horizon_steps]
        rows[i]["label_fault_next_5m"] = 1 if any(future_window) else 0
        yield rows[i]


def main() -> None:
    args = parse_args()
    cfg = GeneratorConfig(
        num_devices=args.num_devices,
        samples_per_device=args.samples_per_device,
        sample_interval_seconds=args.sample_interval_seconds,
        temp_fault_threshold=args.temp_fault_threshold,
        vibration_fault_threshold=args.vibration_fault_threshold,
        seed=args.seed,
    )
    rnd = random.Random(cfg.seed)
    start_ts = datetime.now(timezone.utc) - timedelta(
        seconds=cfg.samples_per_device * cfg.sample_interval_seconds
    )

    devices = make_device_ids(cfg.num_devices)
    all_rows: List[Dict] = []
    for machine_id in devices:
        all_rows.extend(generate_rows_for_device(machine_id, start_ts, cfg, rnd))

    jsonl_path = Path(args.output_jsonl)
    csv_path = Path(args.output_csv)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with jsonl_path.open("w", encoding="utf-8") as jf:
        for row in all_rows:
            jf.write(json.dumps(row) + "\n")

    fieldnames = list(all_rows[0].keys()) if all_rows else []
    with csv_path.open("w", encoding="utf-8", newline="") as cf:
        writer = csv.DictWriter(cf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Wrote {len(all_rows)} rows for {cfg.num_devices} devices.")
    print(f"JSONL: {jsonl_path}")
    print(f"CSV:   {csv_path}")


if __name__ == "__main__":
    main()
