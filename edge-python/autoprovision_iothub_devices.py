import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List


def run_az(cmd: List[str]) -> Dict:
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Azure CLI command failed ({' '.join(cmd)}):\n{result.stderr.strip()}"
        )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed parsing Azure CLI JSON output:\n{result.stdout}") from exc


def ensure_device_identity(iothub_name: str, device_id: str) -> Dict:
    create_cmd = [
        "az",
        "iot",
        "hub",
        "device-identity",
        "create",
        "--hub-name",
        iothub_name,
        "--device-id",
        device_id,
        "--auth-method",
        "shared_private_key",
        "--output",
        "json",
    ]
    try:
        return run_az(create_cmd)
    except RuntimeError as exc:
        message = str(exc).lower()
        if (
            "already exists" not in message
            and "devicealreadyexists" not in message
            and "conflict" not in message
        ):
            raise

        show_cmd = [
            "az",
            "iot",
            "hub",
            "device-identity",
            "show",
            "--hub-name",
            iothub_name,
            "--device-id",
            device_id,
            "--output",
            "json",
        ]
        return run_az(show_cmd)


def get_device_key(iothub_name: str, device_id: str) -> str:
    show_cmd = [
        "az",
        "iot",
        "hub",
        "device-identity",
        "show",
        "--hub-name",
        iothub_name,
        "--device-id",
        device_id,
        "--query",
        "authentication.symmetricKey.primaryKey",
        "--output",
        "tsv",
    ]
    result = subprocess.run(
        show_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed retrieving device key for {device_id}:\n{result.stderr.strip()}"
        )
    key = result.stdout.strip()
    if not key:
        raise RuntimeError(f"Primary key was empty for device {device_id}.")
    return key


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk provision Azure IoT Hub devices for fleet simulator."
    )
    parser.add_argument("--iothub-name", required=True, help="IoT Hub name")
    parser.add_argument(
        "--count",
        type=int,
        required=True,
        help="Number of devices to create",
    )
    parser.add_argument(
        "--device-prefix",
        default="sim-device",
        help="Device ID prefix (default: sim-device)",
    )
    parser.add_argument(
        "--machine-prefix",
        default="MACH",
        help="Machine ID prefix in manifest (default: MACH)",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=1,
        help="Starting numeric suffix (default: 1)",
    )
    parser.add_argument(
        "--padding",
        type=int,
        default=3,
        help="Zero-padding width for numeric suffix (default: 3)",
    )
    parser.add_argument(
        "--output-file",
        default="devices.json",
        help="Output manifest path for simulator (default: devices.json)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.count <= 0:
        raise ValueError("--count must be > 0")

    manifest = []
    for i in range(args.start_index, args.start_index + args.count):
        suffix = str(i).zfill(args.padding)
        device_id = f"{args.device_prefix}-{suffix}"
        machine_id = f"{args.machine_prefix}_{suffix}"

        ensure_device_identity(args.iothub_name, device_id)
        device_key = get_device_key(args.iothub_name, device_id)

        manifest.append(
            {
                "device_id": device_id,
                "machine_id": machine_id,
                "device_key": device_key,
            }
        )
        print(f"Provisioned: {device_id} -> {machine_id}")

    output_path = Path(args.output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"\nWrote {len(manifest)} devices to: {output_path}")
    print("Use this file with simulate_fleet_iothub.py --devices-file")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
