import argparse
import csv
import json
import logging
import math
import random
import ssl
import threading
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt

from generate_sas_token import generate_sas_token


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("fleet-simulator")

TARGET_FIELDS = {
    "machine_id",
    "vibration_mm_s",
    "temp_c",
    "throughput_cpm",
    "rpm",
    "current_amps",
    "humidity_pct",
    "load_pct",
    "power_kw",
    "power_factor",
    "voltage_v",
    "pressure_bar",
    "flow_rate_lpm",
    "state",
    "fault_code",
    "ts",
}


@dataclass
class DeviceConfig:
    device_id: str
    machine_id: str
    device_key: Optional[str] = None


class ScenarioRuntime:
    """Shared telemetry scenario engine used for stream and export modes."""

    def __init__(
        self,
        machine_id: str,
        fault_period_seconds: int,
        temp_fault_threshold: float,
        vibration_fault_threshold: float,
        seed: int,
        risk_profile: str = "healthy",
        phase_offset_seconds: float = 0.0,
        wave_mode: str = "none",
        wave_ramp_seconds: int = 600,
    ) -> None:
        self.machine_id = machine_id
        self.fault_period_seconds = max(30, fault_period_seconds)
        self.temp_fault_threshold = temp_fault_threshold
        self.vibration_fault_threshold = vibration_fault_threshold
        self.random = random.Random(seed)
        self.risk_profile = risk_profile
        self.phase_offset_seconds = max(0.0, phase_offset_seconds)
        self.wave_mode = wave_mode
        self.wave_ramp_seconds = max(60, wave_ramp_seconds)

        self.temp_c = self.random.uniform(58.0, 67.0)
        self.vibration_mm_s = self.random.uniform(2.5, 4.5)
        self.throughput_cpm = self.random.randint(80, 115)
        self.rpm = self.random.randint(1800, 2400)
        self.current_amps = self.random.uniform(4.0, 7.0)
        self.humidity_pct = self.random.uniform(35.0, 55.0)
        self.state = "RUN"
        self.fault_code = None

    def _profile_multiplier(self) -> float:
        if self.risk_profile == "risky":
            return 1.30
        if self.risk_profile == "degrading":
            return 1.10
        return 0.90

    def _wave_factor(self, elapsed_seconds: float) -> float:
        if self.wave_mode != "wave":
            return 1.0
        return min(1.0, max(0.0, elapsed_seconds / float(self.wave_ramp_seconds)))

    def update_state(self, elapsed_seconds: float) -> None:
        adjusted_elapsed = max(0.0, elapsed_seconds + self.phase_offset_seconds)
        phase = adjusted_elapsed % self.fault_period_seconds
        stress = self._profile_multiplier() * (0.35 + 0.65 * self._wave_factor(elapsed_seconds))
        normal_fraction = max(0.35, 0.72 - (0.20 * stress))
        warning_fraction = max(normal_fraction + 0.05, min(0.92, 0.91 - (0.08 * stress)))
        fault_fraction = max(warning_fraction + 0.02, min(0.98, 0.97 - (0.04 * stress)))
        normal_cutoff = normal_fraction * self.fault_period_seconds
        warning_cutoff = warning_fraction * self.fault_period_seconds
        fault_cutoff = fault_fraction * self.fault_period_seconds

        if phase < normal_cutoff:
            self.state = "RUN"
            self.fault_code = None
            self.temp_c = max(35.0, min(85.0, self.temp_c + self.random.uniform(-0.4, 0.4)))
            self.vibration_mm_s = max(0.8, min(8.0, self.vibration_mm_s + self.random.uniform(-0.25, 0.25)))
            self.throughput_cpm = max(60, min(125, self.throughput_cpm + self.random.randint(-2, 2)))
            self.rpm = max(1200, min(2800, self.rpm + self.random.randint(-20, 20)))
            self.current_amps = max(3.0, min(10.0, self.current_amps + self.random.uniform(-0.2, 0.2)))
            self.humidity_pct = max(25.0, min(80.0, self.humidity_pct + self.random.uniform(-0.5, 0.5)))
            return

        if phase < warning_cutoff:
            self.state = "RUN"
            self.fault_code = None
            progress = (phase - normal_cutoff) / (warning_cutoff - normal_cutoff)
            profile_boost = self._profile_multiplier()
            self.temp_c = 74.0 + progress * (self.temp_fault_threshold + (8.0 * profile_boost) - 74.0)
            self.vibration_mm_s = 6.5 + progress * (
                self.vibration_fault_threshold + (2.0 * profile_boost) - 6.5
            )
            self.throughput_cpm = max(30, int(95 - 50 * progress + self.random.uniform(-2, 2)))
            self.rpm = int(2350 + progress * (650 * profile_boost) + self.random.uniform(-15, 15))
            self.current_amps = 6.8 + progress * (5.8 * profile_boost) + self.random.uniform(-0.3, 0.3)
            self.humidity_pct = 50.0 + progress * 20.0 + self.random.uniform(-1, 1)
            return

        if phase < fault_cutoff:
            self.state = "FAULT"
            if self.current_amps >= 12.0:
                self.fault_code = "OVERCURRENT"
            elif self.vibration_mm_s >= self.vibration_fault_threshold and self.rpm > 2000:
                self.fault_code = "BEARING_WEAR"
            elif self.temp_c >= self.temp_fault_threshold:
                self.fault_code = "OVERTEMP"
            else:
                self.fault_code = "VIBRATION"
            self.temp_c = max(self.temp_fault_threshold + 0.2, self.temp_c + self.random.uniform(-0.6, 0.8))
            self.vibration_mm_s = max(
                self.vibration_fault_threshold + 0.2, self.vibration_mm_s + self.random.uniform(-0.4, 0.6)
            )
            self.throughput_cpm = self.random.randint(0, 12)
            self.rpm = self.random.randint(0, 200)
            self.current_amps = max(12.0, self.current_amps + self.random.uniform(-0.5, 1.0))
            self.humidity_pct = max(60.0, self.humidity_pct + self.random.uniform(-0.5, 1.5))
            return

        self.state = "STOPPED"
        self.fault_code = None
        self.temp_c = max(45.0, self.temp_c - self.random.uniform(0.4, 1.0))
        self.vibration_mm_s = max(1.0, self.vibration_mm_s - self.random.uniform(0.3, 0.7))
        self.throughput_cpm = self.random.randint(0, 5)
        self.rpm = 0
        self.current_amps = max(0.5, self.current_amps - self.random.uniform(0.3, 0.8))
        self.humidity_pct = max(30.0, self.humidity_pct - self.random.uniform(0.3, 0.8))

    def build_payload(self, event_ts: Optional[datetime] = None) -> Dict[str, Any]:
        load_pct = max(0.0, min(100.0, (self.throughput_cpm / 120.0) * 100.0))
        voltage_v = 230.0
        power_factor = 0.92
        power_kw = (voltage_v * self.current_amps * power_factor * math.sqrt(3)) / 1000.0
        pressure_bar = max(1.0, 2.5 + load_pct / 45.0)
        flow_rate_lpm = max(5.0, 40.0 + (self.throughput_cpm * 0.95))
        ts = event_ts or datetime.now(timezone.utc)
        return {
            "machine_id": self.machine_id,
            "vibration_mm_s": round(self.vibration_mm_s, 3),
            "temp_c": round(self.temp_c, 3),
            "throughput_cpm": int(self.throughput_cpm),
            "rpm": int(self.rpm),
            "current_amps": round(self.current_amps, 3),
            "humidity_pct": round(self.humidity_pct, 1),
            "load_pct": round(load_pct, 2),
            "power_kw": round(power_kw, 3),
            "power_factor": round(power_factor, 3),
            "voltage_v": round(voltage_v, 1),
            "pressure_bar": round(pressure_bar, 3),
            "flow_rate_lpm": round(flow_rate_lpm, 2),
            "state": self.state,
            "fault_code": self.fault_code,
            "ts": ts.isoformat().replace("+00:00", "Z"),
        }

    def validate_target_fields(self) -> None:
        payload = self.build_payload()
        missing = sorted(TARGET_FIELDS.difference(payload.keys()))
        if missing:
            raise ValueError(f"{self.machine_id} missing target payload fields: {missing}")


class DeviceRuntime(ScenarioRuntime):
    def __init__(
        self,
        config: DeviceConfig,
        iothub_name: str,
        token_ttl_seconds: int,
        message_rate_hz: float,
        fault_period_seconds: int,
        temp_fault_threshold: float,
        vibration_fault_threshold: float,
        seed: int,
        risk_profile: str,
        phase_offset_seconds: float,
        wave_mode: str,
        wave_ramp_seconds: int,
    ) -> None:
        super().__init__(
            machine_id=config.machine_id,
            fault_period_seconds=fault_period_seconds,
            temp_fault_threshold=temp_fault_threshold,
            vibration_fault_threshold=vibration_fault_threshold,
            seed=seed,
            risk_profile=risk_profile,
            phase_offset_seconds=phase_offset_seconds,
            wave_mode=wave_mode,
            wave_ramp_seconds=wave_ramp_seconds,
        )
        if not config.device_key:
            raise ValueError(f"Missing device_key for stream mode device {config.device_id}")
        self.config = config
        self.token_ttl_seconds = token_ttl_seconds
        self.message_rate_hz = max(0.1, message_rate_hz)
        self.phase_start = time.time()
        self.host = f"{iothub_name}.azure-devices.net"
        self.topic = f"devices/{config.device_id}/messages/events/"
        self.username = f"{self.host}/{config.device_id}/?api-version=2021-04-12"
        self._sas_expiry = 0
        self._connect_mqtt()

    def _build_sas(self) -> str:
        resource_uri = f"{self.host}/devices/{self.config.device_id}"
        self._sas_expiry = int(time.time()) + self.token_ttl_seconds
        return generate_sas_token(resource_uri, self.config.device_key, self._sas_expiry)

    def _connect_mqtt(self) -> None:
        sas = self._build_sas()
        self.client = mqtt.Client(client_id=self.config.device_id, protocol=mqtt.MQTTv311)
        self.client.username_pw_set(self.username, sas)
        self.client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.connect(self.host, 8883, keepalive=60)
        self.client.loop_start()

    def _refresh_sas_if_needed(self) -> None:
        if time.time() > (self._sas_expiry - 300):
            LOGGER.info("Refreshing SAS token for %s", self.config.device_id)
            self.client.loop_stop()
            self.client.disconnect()
            self._connect_mqtt()

    def _on_connect(self, _client: mqtt.Client, _userdata, _flags, rc: int, _props=None) -> None:
        if rc != 0:
            LOGGER.error("Device %s failed MQTT connect rc=%s", self.config.device_id, rc)

    def _on_disconnect(self, _client: mqtt.Client, _userdata, rc: int, _props=None) -> None:
        if rc != 0:
            LOGGER.warning("Device %s disconnected rc=%s", self.config.device_id, rc)

    def publish_once(self) -> None:
        self._refresh_sas_if_needed()
        self.update_state(time.time() - self.phase_start)
        payload = self.build_payload()
        result = self.client.publish(self.topic, payload=json.dumps(payload), qos=1)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error("Publish failed for %s rc=%s", self.config.device_id, result.rc)

    def close(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified IoT telemetry simulator and training exporter.")
    parser.add_argument("--mode", choices=["stream", "export-training"], default="stream")
    parser.add_argument("--iothub-name", default="", help="Required for stream mode")
    parser.add_argument("--devices-file", default="", help="Required for stream mode")
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--target-total-records", type=int, default=10000)
    parser.add_argument("--message-rate-hz", type=float, default=1.0)
    parser.add_argument("--fault-period-seconds", type=int, default=180)
    parser.add_argument("--temp-fault-threshold", type=float, default=85.0)
    parser.add_argument("--vibration-fault-threshold", type=float, default=9.5)
    parser.add_argument("--token-ttl-seconds", type=int, default=3600)
    parser.add_argument("--log-every", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--num-devices", type=int, default=100, help="Used by export-training mode")
    parser.add_argument("--machine-prefix", default="MC", help="Used by export-training mode")
    parser.add_argument("--device-prefix", default="iotdev", help="Used by export-training mode")
    parser.add_argument("--padding", type=int, default=4, help="Used by export-training mode")
    parser.add_argument("--sample-interval-seconds", type=int, default=5, help="Used by export-training mode")
    parser.add_argument("--phase-stagger-seconds", type=float, default=2.0)
    parser.add_argument("--degrading-device-fraction", type=float, default=0.25)
    parser.add_argument("--risky-device-fraction", type=float, default=0.15)
    parser.add_argument("--wave-mode", choices=["none", "wave"], default="none")
    parser.add_argument("--wave-ramp-seconds", type=int, default=600)
    parser.add_argument("--output-jsonl", default="synthetic_fault_training.jsonl")
    parser.add_argument("--output-csv", default="synthetic_fault_training.csv")
    parser.add_argument("--output-parquet", default="", help="Optional parquet output path (if pandas+pyarrow installed)")
    return parser.parse_args()


def load_devices(path: str) -> List[DeviceConfig]:
    raw = json.loads(Path(path).read_text())
    configs = [
        DeviceConfig(
            device_id=item["device_id"],
            machine_id=item.get("machine_id", item["device_id"]),
            device_key=item.get("device_key"),
        )
        for item in raw
    ]
    if not configs:
        raise ValueError("No devices found in devices-file.")
    return configs


def _assign_risk_profile(args: argparse.Namespace, idx: int) -> str:
    seeded = random.Random(args.seed + (idx * 7919))
    roll = seeded.random()
    risky_cutoff = max(0.0, min(1.0, args.risky_device_fraction))
    degrading_cutoff = max(0.0, min(1.0, args.degrading_device_fraction))
    if roll < risky_cutoff:
        return "risky"
    if roll < (risky_cutoff + degrading_cutoff):
        return "degrading"
    return "healthy"


def run_device_loop(
    runtime: DeviceRuntime,
    end_time: float,
    total_budget: int,
    global_counter: Dict[str, int],
    stats: Dict[str, int],
    lock: threading.Lock,
) -> None:
    interval = 1.0 / runtime.message_rate_hz
    sent = 0
    try:
        while time.time() < end_time:
            with lock:
                if total_budget > 0 and global_counter["sent"] >= total_budget:
                    break
                global_counter["sent"] += 1
            started = time.time()
            runtime.publish_once()
            sent += 1
            sleep_for = max(0.0, interval - (time.time() - started))
            if sleep_for > 0:
                time.sleep(sleep_for)
    finally:
        runtime.close()
        with lock:
            stats[runtime.config.device_id] = sent


def run_stream_mode(args: argparse.Namespace) -> None:
    if not args.iothub_name or not args.devices_file:
        raise ValueError("--iothub-name and --devices-file are required in stream mode.")
    devices = load_devices(args.devices_file)
    end_time = time.time() + args.duration_seconds

    runtimes: List[DeviceRuntime] = []
    for idx, device in enumerate(devices):
        profile = _assign_risk_profile(args, idx)
        phase_offset = idx * max(0.0, args.phase_stagger_seconds)
        runtimes.append(
            DeviceRuntime(
                config=device,
                iothub_name=args.iothub_name,
                token_ttl_seconds=args.token_ttl_seconds,
                message_rate_hz=args.message_rate_hz,
                fault_period_seconds=args.fault_period_seconds,
                temp_fault_threshold=args.temp_fault_threshold,
                vibration_fault_threshold=args.vibration_fault_threshold,
                seed=args.seed + idx,
                risk_profile=profile,
                phase_offset_seconds=phase_offset,
                wave_mode=args.wave_mode,
                wave_ramp_seconds=args.wave_ramp_seconds,
            )
        )
    for runtime in runtimes:
        runtime.validate_target_fields()
    LOGGER.info("Payload field validation passed for %s devices.", len(runtimes))

    if args.validate_only:
        for runtime in runtimes:
            runtime.close()
        LOGGER.info("Validation-only mode complete.")
        return

    stats: Dict[str, int] = {}
    lock = threading.Lock()
    global_counter = {"sent": 0}
    threads = [
        threading.Thread(
            target=run_device_loop,
            args=(runtime, end_time, args.target_total_records, global_counter, stats, lock),
            daemon=True,
        )
        for runtime in runtimes
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    total_sent = sum(stats.values())
    avg_per_device = total_sent / max(1, len(stats))
    LOGGER.info(
        "Fleet simulation complete: devices=%s total_messages=%s avg_per_device=%.2f duration_s=%s",
        len(stats),
        total_sent,
        avg_per_device,
        args.duration_seconds,
    )


def _label_rows(rows: List[Dict[str, Any]], sample_interval_seconds: int) -> List[Dict[str, Any]]:
    horizon_steps = max(1, int(300 / sample_interval_seconds))
    flags = [1 if row["is_fault"] else 0 for row in rows]
    for i in range(len(rows)):
        rows[i]["label_fault_next_5m"] = 1 if any(flags[i + 1 : i + 1 + horizon_steps]) else 0
    return rows


def run_export_training_mode(args: argparse.Namespace) -> None:
    if args.num_devices <= 0:
        raise ValueError("--num-devices must be > 0 in export-training mode.")
    samples_per_device = max(1, math.ceil(args.target_total_records / args.num_devices))
    start_ts = datetime.now(timezone.utc) - timedelta(seconds=samples_per_device * args.sample_interval_seconds)
    all_rows: List[Dict[str, Any]] = []

    for idx in range(1, args.num_devices + 1):
        suffix = str(idx).zfill(args.padding)
        machine_id = f"{args.machine_prefix}-{suffix}"
        profile = _assign_risk_profile(args, idx)
        phase_offset = idx * max(0.0, args.phase_stagger_seconds)
        runtime = ScenarioRuntime(
            machine_id=machine_id,
            fault_period_seconds=args.fault_period_seconds,
            temp_fault_threshold=args.temp_fault_threshold,
            vibration_fault_threshold=args.vibration_fault_threshold,
            seed=args.seed + idx,
            risk_profile=profile,
            phase_offset_seconds=phase_offset,
            wave_mode=args.wave_mode,
            wave_ramp_seconds=args.wave_ramp_seconds,
        )
        runtime.validate_target_fields()
        machine_rows: List[Dict[str, Any]] = []
        for step in range(samples_per_device):
            elapsed = step * args.sample_interval_seconds
            event_ts = start_ts + timedelta(seconds=elapsed)
            runtime.update_state(elapsed)
            payload = runtime.build_payload(event_ts=event_ts)
            threshold_crossed = (
                payload["temp_c"] >= args.temp_fault_threshold
                or payload["vibration_mm_s"] >= args.vibration_fault_threshold
                or payload["current_amps"] >= 12.0
            )
            row = {
                "event_time": payload["ts"],
                **payload,
                "is_fault": payload["state"] == "FAULT",
                "threshold_crossed": threshold_crossed,
                "temp_fault_threshold": args.temp_fault_threshold,
                "vibration_fault_threshold": args.vibration_fault_threshold,
            }
            machine_rows.append(row)
        all_rows.extend(_label_rows(machine_rows, args.sample_interval_seconds))

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

    if args.output_parquet:
        try:
            import pandas as pd

            pq_path = Path(args.output_parquet)
            pq_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(all_rows).to_parquet(pq_path, index=False)
            LOGGER.info("Parquet: %s", pq_path)
        except Exception as exc:
            LOGGER.warning("Skipping parquet export (%s).", exc)

    LOGGER.info("Exported %s training rows for %s devices.", len(all_rows), args.num_devices)
    LOGGER.info("JSONL: %s", jsonl_path)
    LOGGER.info("CSV:   %s", csv_path)


def main() -> None:
    args = parse_args()
    if args.mode == "stream":
        run_stream_mode(args)
        return
    warnings.warn(
        "Training data export now lives in simulate_fleet_iothub.py --mode export-training.",
        DeprecationWarning,
        stacklevel=2,
    )
    run_export_training_mode(args)


if __name__ == "__main__":
    main()
