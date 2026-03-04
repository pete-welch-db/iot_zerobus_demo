import argparse
import subprocess
import sys
import warnings
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "DEPRECATED wrapper. Use simulate_fleet_iothub.py --mode export-training directly. "
            "This wrapper remains for one migration cycle."
        )
    )
    parser.add_argument("--num-devices", type=int, default=100)
    parser.add_argument("--samples-per-device", type=int, default=5000)
    parser.add_argument("--sample-interval-seconds", type=int, default=5)
    parser.add_argument("--temp-fault-threshold", type=float, default=85.0)
    parser.add_argument("--vibration-fault-threshold", type=float, default=9.5)
    parser.add_argument("--output-jsonl", default="synthetic_fault_training.jsonl")
    parser.add_argument("--output-csv", default="synthetic_fault_training.csv")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    warnings.warn(
        "generate_fault_training_data.py is deprecated. "
        "Use simulate_fleet_iothub.py --mode export-training.",
        DeprecationWarning,
        stacklevel=2,
    )
    target_total_records = args.num_devices * args.samples_per_device
    script_path = Path(__file__).with_name("simulate_fleet_iothub.py")
    cmd = [
        sys.executable,
        str(script_path),
        "--mode",
        "export-training",
        "--num-devices",
        str(args.num_devices),
        "--target-total-records",
        str(target_total_records),
        "--sample-interval-seconds",
        str(args.sample_interval_seconds),
        "--temp-fault-threshold",
        str(args.temp_fault_threshold),
        "--vibration-fault-threshold",
        str(args.vibration_fault_threshold),
        "--output-jsonl",
        args.output_jsonl,
        "--output-csv",
        args.output_csv,
        "--seed",
        str(args.seed),
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
