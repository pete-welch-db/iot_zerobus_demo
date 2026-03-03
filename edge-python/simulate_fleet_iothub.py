import argparse
import json
import logging
import math
import random
import ssl
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import paho.mqtt.client as mqtt

from generate_sas_token import generate_sas_token


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("fleet-simulator")


@dataclass
class DeviceConfig:
    device_id: str
    machine_id: str
    device_key: str


class DeviceRuntime:
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
    ) -> None:
        self.config = config
        self.iothub_name = iothub_name
        self.token_ttl_seconds = token_ttl_seconds
        self.message_rate_hz = max(0.1, message_rate_hz)
        self.fault_period_seconds = max(30, fault_period_seconds)
        self.temp_fault_threshold = temp_fault_threshold
        self.vibration_fault_threshold = vibration_fault_threshold
        self.random = random.Random(seed)
        self.phase_start = time.time()

        self.temp_c = self.random.uniform(58.0, 67.0)
        self.vibration_mm_s = self.random.uniform(2.5, 4.5)
        self.throughput_cpm = self.random.randint(80, 115)
        self.state = "RUN"
        self.fault_code = None

        host = f"{iothub_name}.azure-devices.net"
        self.topic = f"devices/{config.device_id}/messages/events/"
        username = f"{host}/{config.device_id}/?api-version=2021-04-12"
        sas = self._build_sas(host)
        self.client = mqtt.Client(client_id=config.device_id, protocol=mqtt.MQTTv311)
        self.client.username_pw_set(username, sas)
        self.client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.connect(host, 8883, keepalive=60)
        self.client.loop_start()

    def _build_sas(self, host: str) -> str:
        resource_uri = f"{host}/devices/{self.config.device_id}"
        expiry = int(time.time()) + self.token_ttl_seconds
        return generate_sas_token(resource_uri, self.config.device_key, expiry)

    def _on_connect(self, _client: mqtt.Client, _userdata, _flags, rc: int, _props=None) -> None:
        if rc != 0:
            LOGGER.error("Device %s failed MQTT connect rc=%s", self.config.device_id, rc)

    def _on_disconnect(self, _client: mqtt.Client, _userdata, rc: int, _props=None) -> None:
        if rc != 0:
            LOGGER.warning("Device %s disconnected rc=%s", self.config.device_id, rc)

    def _update_state(self, now: float) -> None:
        cycle_seconds = now - self.phase_start
        phase = cycle_seconds % self.fault_period_seconds

        # Phase model:
        # 0-55% normal run, 55-85% warning ramp, 85-95% fault, 95-100% recovery.
        normal_cutoff = 0.55 * self.fault_period_seconds
        warning_cutoff = 0.85 * self.fault_period_seconds
        fault_cutoff = 0.95 * self.fault_period_seconds

        if phase < normal_cutoff:
            self.state = "RUN"
            self.fault_code = None
            self.temp_c = max(35.0, min(85.0, self.temp_c + self.random.uniform(-0.4, 0.4)))
            self.vibration_mm_s = max(0.8, min(8.0, self.vibration_mm_s + self.random.uniform(-0.25, 0.25)))
            self.throughput_cpm = max(60, min(125, self.throughput_cpm + self.random.randint(-2, 2)))
            return

        if phase < warning_cutoff:
            self.state = "RUN"
            self.fault_code = None
            temp_progress = (phase - normal_cutoff) / (warning_cutoff - normal_cutoff)
            vib_progress = temp_progress
            self.temp_c = 75.0 + temp_progress * (self.temp_fault_threshold + 8.0 - 75.0)
            self.vibration_mm_s = 7.0 + vib_progress * (self.vibration_fault_threshold + 2.0 - 7.0)
            self.throughput_cpm = max(30, int(95 - 50 * temp_progress + self.random.uniform(-2, 2)))
            return

        if phase < fault_cutoff:
            self.state = "FAULT"
            self.fault_code = "F_OVERHEAT" if self.temp_c >= self.temp_fault_threshold else "F_VIBRATION"
            self.temp_c = max(self.temp_fault_threshold + 0.2, self.temp_c + self.random.uniform(-0.6, 0.8))
            self.vibration_mm_s = max(
                self.vibration_fault_threshold + 0.2, self.vibration_mm_s + self.random.uniform(-0.4, 0.6)
            )
            self.throughput_cpm = self.random.randint(0, 12)
            return

        self.state = "STOPPED"
        self.fault_code = None
        self.temp_c = max(45.0, self.temp_c - self.random.uniform(0.4, 1.0))
        self.vibration_mm_s = max(1.0, self.vibration_mm_s - self.random.uniform(0.3, 0.7))
        self.throughput_cpm = self.random.randint(0, 5)

    def publish_once(self) -> None:
        now = time.time()
        self._update_state(now)

        payload = {
            "machine_id": self.config.machine_id,
            "vibration_mm_s": round(self.vibration_mm_s, 3),
            "temp_c": round(self.temp_c, 3),
            "throughput_cpm": int(self.throughput_cpm),
            "state": self.state,
            "fault_code": self.fault_code,
            "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        result = self.client.publish(self.topic, payload=json.dumps(payload), qos=1)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error("Publish failed for %s rc=%s", self.config.device_id, result.rc)

    def close(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate many IoT devices publishing to Azure IoT Hub.")
    parser.add_argument("--iothub-name", required=True)
    parser.add_argument("--devices-file", required=True, help="JSON array with device_id, machine_id, device_key")
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--message-rate-hz", type=float, default=1.0)
    parser.add_argument("--fault-period-seconds", type=int, default=180)
    parser.add_argument("--temp-fault-threshold", type=float, default=85.0)
    parser.add_argument("--vibration-fault-threshold", type=float, default=9.5)
    parser.add_argument("--token-ttl-seconds", type=int, default=3600)
    parser.add_argument("--log-every", type=int, default=100)
    return parser.parse_args()


def load_devices(path: str) -> List[DeviceConfig]:
    raw = json.loads(Path(path).read_text())
    configs = []
    for item in raw:
        configs.append(
            DeviceConfig(
                device_id=item["device_id"],
                machine_id=item.get("machine_id", item["device_id"]),
                device_key=item["device_key"],
            )
        )
    if not configs:
        raise ValueError("No devices found in devices-file.")
    return configs


def run_device_loop(runtime: DeviceRuntime, end_time: float, stats: Dict[str, int], lock: threading.Lock) -> None:
    interval = 1.0 / runtime.message_rate_hz
    sent = 0
    try:
        while time.time() < end_time:
            started = time.time()
            runtime.publish_once()
            sent += 1
            elapsed = time.time() - started
            sleep_for = max(0.0, interval - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)
    finally:
        runtime.close()
        with lock:
            stats[runtime.config.device_id] = sent


def main() -> None:
    args = parse_args()
    devices = load_devices(args.devices_file)
    end_time = time.time() + args.duration_seconds

    runtimes = [
        DeviceRuntime(
            config=device,
            iothub_name=args.iothub_name,
            token_ttl_seconds=args.token_ttl_seconds,
            message_rate_hz=args.message_rate_hz,
            fault_period_seconds=args.fault_period_seconds,
            temp_fault_threshold=args.temp_fault_threshold,
            vibration_fault_threshold=args.vibration_fault_threshold,
            seed=hash(device.device_id) % (2**31),
        )
        for device in devices
    ]

    stats: Dict[str, int] = {}
    lock = threading.Lock()
    threads = [
        threading.Thread(target=run_device_loop, args=(runtime, end_time, stats, lock), daemon=True)
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


if __name__ == "__main__":
    main()
