import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List


def run_az(cmd: List[str]) -> str:
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Azure CLI command failed ({' '.join(cmd)}):\n{result.stderr.strip()}")
    return result.stdout


def run_az_json(cmd: List[str]) -> Dict:
    output = run_az(cmd)
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Failed parsing Azure CLI JSON output:\n{output}") from exc


def list_devices(iothub_name: str) -> List[str]:
    output = run_az(
        [
            "az",
            "iot",
            "hub",
            "device-identity",
            "list",
            "--hub-name",
            iothub_name,
            "--query",
            "[].deviceId",
            "--output",
            "tsv",
        ]
    )
    return [line.strip() for line in output.splitlines() if line.strip()]


def delete_device(iothub_name: str, device_id: str) -> None:
    run_az(
        [
            "az",
            "iot",
            "hub",
            "device-identity",
            "delete",
            "--hub-name",
            iothub_name,
            "--device-id",
            device_id,
        ]
    )


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
        return run_az_json(create_cmd)
    except RuntimeError as exc:
        message = str(exc).lower()
        if "already exists" not in message and "devicealreadyexists" not in message and "conflict" not in message:
            raise
        return run_az_json(
            [
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
        )


def get_device_key(iothub_name: str, device_id: str) -> str:
    output = run_az(
        [
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
    )
    key = output.strip()
    if not key:
        raise RuntimeError(f"Primary key was empty for device {device_id}.")
    return key


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete and recreate IoT Hub identities for demo (1 physical + N virtual)."
    )
    parser.add_argument("--iothub-name", required=True, help="IoT Hub name")
    parser.add_argument("--virtual-count", type=int, default=100, help="Number of virtual devices")
    parser.add_argument("--virtual-start-index", type=int, default=1, help="Starting suffix for virtual devices")
    parser.add_argument("--padding", type=int, default=4, help="Numeric suffix zero padding width")
    parser.add_argument("--device-prefix", default="iotdev", help="Device ID prefix")
    parser.add_argument("--machine-prefix", default="MC", help="Machine ID prefix")
    parser.add_argument("--physical-device-id", default="iotdev-0000", help="Physical Arduino device ID")
    parser.add_argument("--physical-machine-id", default="MC-0000", help="Physical Arduino machine ID")
    parser.add_argument(
        "--delete-all-existing",
        action="store_true",
        help="Delete all existing IoT Hub device identities before reprovisioning.",
    )
    parser.add_argument(
        "--delete-prefix",
        default="",
        help="Optional prefix filter when deleting (ignored when --delete-all-existing).",
    )
    parser.add_argument(
        "--output-virtual-devices-file",
        default="devices.json",
        help="Manifest output path for virtual simulator devices.",
    )
    parser.add_argument(
        "--output-physical-device-file",
        default="arduino_device.json",
        help="Output path for physical device identity metadata.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.virtual_count <= 0:
        raise ValueError("--virtual-count must be > 0")

    existing_devices = list_devices(args.iothub_name)
    to_delete = existing_devices
    if not args.delete_all_existing and args.delete_prefix:
        to_delete = [d for d in existing_devices if d.startswith(args.delete_prefix)]

    if to_delete:
        print(f"Deleting {len(to_delete)} IoT Hub devices...")
        for device_id in to_delete:
            delete_device(args.iothub_name, device_id)
            print(f"Deleted: {device_id}")
    else:
        print("No matching devices to delete.")

    ensure_device_identity(args.iothub_name, args.physical_device_id)
    physical_key = get_device_key(args.iothub_name, args.physical_device_id)
    physical_manifest = {
        "device_id": args.physical_device_id,
        "machine_id": args.physical_machine_id,
        "device_key": physical_key,
    }

    virtual_manifest = []
    for idx in range(args.virtual_start_index, args.virtual_start_index + args.virtual_count):
        suffix = str(idx).zfill(args.padding)
        device_id = f"{args.device_prefix}-{suffix}"
        machine_id = f"{args.machine_prefix}-{suffix}"
        ensure_device_identity(args.iothub_name, device_id)
        device_key = get_device_key(args.iothub_name, device_id)
        virtual_manifest.append(
            {
                "device_id": device_id,
                "machine_id": machine_id,
                "device_key": device_key,
            }
        )
        print(f"Provisioned virtual: {device_id} -> {machine_id}")

    virtual_output = Path(args.output_virtual_devices_file)
    virtual_output.parent.mkdir(parents=True, exist_ok=True)
    virtual_output.write_text(json.dumps(virtual_manifest, indent=2) + "\n")

    physical_output = Path(args.output_physical_device_file)
    physical_output.parent.mkdir(parents=True, exist_ok=True)
    physical_output.write_text(json.dumps(physical_manifest, indent=2) + "\n")

    print("")
    print(f"Physical device: {args.physical_device_id} -> {args.physical_machine_id}")
    print(f"Wrote {len(virtual_manifest)} virtual devices to: {virtual_output}")
    print(f"Wrote physical device metadata to: {physical_output}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
