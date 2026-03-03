import json
import logging
import os
import ssl
import time
from datetime import datetime, timezone
from typing import Optional, Tuple

import paho.mqtt.client as mqtt
import serial
from serial.serialutil import SerialException


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("edge-sender")

# Fallback ingestion mode:
# Arduino serial CSV -> Python bridge -> Azure IoT Hub MQTT.
# Keep JSON payload fields aligned with direct Uno WiFi publish contract.

SERIAL_PORT = os.getenv("SERIAL_PORT", "/dev/cu.usbmodem101")
BAUD_RATE = int(os.getenv("BAUD_RATE", "115200"))
READ_TIMEOUT_SECONDS = float(os.getenv("SERIAL_TIMEOUT_SECONDS", "2.0"))

IOT_HUB_NAME = os.getenv("IOT_HUB_NAME", "iothub-zerobus-demo-welch")
DEVICE_ID = os.getenv("DEVICE_ID", "arduino-panel")
SAS_TOKEN = os.getenv("SAS_TOKEN", "")
MACHINE_ID = os.getenv("MACHINE_ID", "MACH_A")

MQTT_HOST = f"{IOT_HUB_NAME}.azure-devices.net"
MQTT_PORT = 8883
MQTT_USERNAME = f"{MQTT_HOST}/{DEVICE_ID}/?api-version=2021-04-12"
MQTT_TOPIC = f"devices/{DEVICE_ID}/messages/events/"

ALLOWED_STATES = {"RUN", "STOPPED", "FAULT"}


def parse_serial_line(line: str) -> Optional[Tuple[float, float, int, str, Optional[str]]]:
    parts = [p.strip() for p in line.split(",")]
    if len(parts) < 5:
        LOGGER.warning("Skipping malformed line (field count): %s", line)
        return None

    # Backward/forward compatible with expanded CSV: consume first 5 fields,
    # ignore any additional diagnostics fields emitted by firmware.
    vib_raw, temp_raw, tput_raw, state_raw, fault_raw = parts[:5]
    if state_raw not in ALLOWED_STATES:
        LOGGER.warning("Skipping malformed line (state): %s", line)
        return None

    try:
        vibration = float(vib_raw)
        temp_c = float(temp_raw)
        throughput = int(tput_raw)
    except ValueError:
        LOGGER.warning("Skipping malformed line (numeric parse): %s", line)
        return None

    if throughput < 0:
        LOGGER.warning("Skipping malformed line (negative throughput): %s", line)
        return None

    fault_code = None if fault_raw in {"", "NONE", "NULL", "null"} else fault_raw
    return vibration, temp_c, throughput, state_raw, fault_code


def build_payload(parsed: Tuple[float, float, int, str, Optional[str]]) -> str:
    vibration, temp_c, throughput, state, fault_code = parsed
    payload = {
        "machine_id": MACHINE_ID,
        "vibration_mm_s": vibration,
        "temp_c": temp_c,
        "throughput_cpm": throughput,
        "state": state,
        "fault_code": fault_code,
        # Databricks parses this when present; otherwise IoT Hub enqueue time is used downstream.
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    return json.dumps(payload)


def build_mqtt_client() -> mqtt.Client:
    if not IOT_HUB_NAME or not SAS_TOKEN:
        raise RuntimeError("IOT_HUB_NAME and SAS_TOKEN must be set.")

    client = mqtt.Client(client_id=DEVICE_ID, protocol=mqtt.MQTTv311)
    client.username_pw_set(MQTT_USERNAME, SAS_TOKEN)
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
    if os.getenv("TLS_INSECURE", "false").lower() == "true":
        client.tls_insecure_set(True)

    def on_connect(_client: mqtt.Client, _userdata, _flags, rc: int, _properties=None) -> None:
        if rc == 0:
            LOGGER.info("Connected to IoT Hub MQTT endpoint.")
        else:
            LOGGER.error("MQTT connect failed with rc=%s", rc)

    def on_disconnect(_client: mqtt.Client, _userdata, rc: int, _properties=None) -> None:
        LOGGER.warning("MQTT disconnected rc=%s", rc)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    return client


def connect_serial() -> serial.Serial:
    LOGGER.info("Opening serial port %s @ %s baud", SERIAL_PORT, BAUD_RATE)
    return serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=READ_TIMEOUT_SECONDS)


def main() -> None:
    mqtt_client = build_mqtt_client()
    serial_conn: Optional[serial.Serial] = None

    mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    mqtt_client.loop_start()

    try:
        while True:
            if serial_conn is None:
                try:
                    serial_conn = connect_serial()
                except SerialException as exc:
                    LOGGER.error("Serial connect failed: %s. Retrying...", exc)
                    time.sleep(2)
                    continue

            try:
                raw = serial_conn.readline().decode("utf-8", errors="ignore").strip()
                if not raw:
                    continue
                parsed = parse_serial_line(raw)
                if parsed is None:
                    continue

                payload = build_payload(parsed)
                result = mqtt_client.publish(MQTT_TOPIC, payload=payload, qos=1)
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    LOGGER.error("Publish failed with rc=%s", result.rc)
                else:
                    LOGGER.info("Published telemetry: %s", payload)
            except SerialException as exc:
                LOGGER.error("Serial read failed: %s. Reconnecting serial...", exc)
                try:
                    serial_conn.close()
                except Exception:
                    pass
                serial_conn = None
                time.sleep(1)
            except Exception as exc:
                LOGGER.exception("Unexpected sender error: %s", exc)
                time.sleep(1)
    finally:
        if serial_conn is not None:
            serial_conn.close()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


if __name__ == "__main__":
    main()
