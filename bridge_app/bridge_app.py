"""
Databricks App: Azure IoT Hub -> GCP Zerobus bridge (plain Python, no Spark).

Validates network egress to Zerobus, creates a Zerobus stream, then consumes
device telemetry from the IoT Hub Event Hubs-compatible endpoint and forwards
each record into Zerobus.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import socket
import ssl
import sys
import threading
import time
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("iot-zerobus-bridge-app")


def get_secret(scope: str, key: str) -> str:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    resp = w.secrets.get_secret(scope=scope, key=key)
    return base64.b64decode(resp.value).decode("utf-8")


def probe(url: str) -> None:
    """Confirm TCP+TLS reachability — diagnostic only."""
    pu = urlparse(url)
    host, port = pu.hostname, pu.port or 443
    try:
        with socket.create_connection((host, port), timeout=10) as s:
            LOGGER.info("Probe TCP OK target=%s peer=%s", host, s.getpeername())
            ctx = ssl.create_default_context()
            ctx.set_alpn_protocols(["h2"])
            with ctx.wrap_socket(s, server_hostname=host) as ts:
                LOGGER.info("Probe TLS OK proto=%s alpn=%s cipher=%s",
                            ts.version(), ts.selected_alpn_protocol(), ts.cipher()[0])
    except Exception as e:
        LOGGER.error("Probe failed: %s", e)
        raise


def main() -> int:
    ingest_url = os.environ["ZEROBUS_INGEST_URL"]
    workspace_url = os.environ["ZEROBUS_WORKSPACE_URL"]
    bronze_full = os.environ["BRONZE_TABLE"]
    consumer_group = os.environ["IOTHUB_CONSUMER_GROUP"]
    scope = os.environ["SECRET_SCOPE"]

    LOGGER.info("Probing Zerobus network reachability from Apps runtime…")
    try:
        probe(ingest_url)
    except Exception as e:
        LOGGER.error("Aborting — Apps runtime cannot reach Zerobus over TLS (%s)", e)
        # Keep the App alive so logs are inspectable instead of crash-looping
        while True:
            time.sleep(60)

    LOGGER.info("Probe succeeded. Loading secrets…")
    sp_client_id = get_secret(scope, os.environ["SP_CLIENT_ID_KEY"])
    sp_client_secret = get_secret(scope, os.environ["SP_CLIENT_SECRET_KEY"])
    iothub_conn = get_secret(scope, os.environ["IOTHUB_CONN_KEY"])

    parts = dict(p.split("=", 1) for p in iothub_conn.split(";") if "=" in p)
    entity_path = parts.get("EntityPath", "")
    LOGGER.info("IoT Hub topic: %s", entity_path)

    from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
    from zerobus.sdk.sync import ZerobusSdk

    LOGGER.info("Creating Zerobus stream for %s @ %s", bronze_full, ingest_url)
    sdk = ZerobusSdk(ingest_url, unity_catalog_url=workspace_url)
    stream = sdk.create_stream(
        sp_client_id,
        sp_client_secret,
        TableProperties(bronze_full),
        StreamConfigurationOptions(record_type=RecordType.JSON),
    )
    LOGGER.info("Zerobus stream OK")

    from azure.eventhub import EventHubConsumerClient

    counters = {"received": 0, "ingested": 0, "errors": 0}

    def on_event(partition_context, event):
        if event is None:
            return
        try:
            counters["received"] += 1
            rec = json.loads(event.body_as_str())
            try:
                stream.ingest_record(rec)
                counters["ingested"] += 1
            except Exception as e:
                counters["errors"] += 1
                if counters["errors"] < 5:
                    LOGGER.warning("ingest_record failed: %s", e)
            if counters["received"] % 250 == 0:
                LOGGER.info("received=%d ingested=%d errors=%d",
                            counters["received"], counters["ingested"], counters["errors"])
            partition_context.update_checkpoint(event)
        except Exception as outer:
            counters["errors"] += 1
            LOGGER.exception("on_event failed: %s", outer)

    client = EventHubConsumerClient.from_connection_string(
        conn_str=iothub_conn,
        consumer_group=consumer_group,
    )
    LOGGER.info("Event Hubs client connected (consumer_group=%s). Receiving…", consumer_group)
    try:
        with client:
            client.receive(on_event=on_event, starting_position="-1", max_wait_time=15)
    finally:
        try: stream.flush()
        except Exception as e: LOGGER.warning("flush failed: %s", e)
        try: stream.close()
        except Exception as e: LOGGER.warning("close failed: %s", e)
        LOGGER.info("Bridge exiting. received=%d ingested=%d errors=%d",
                    counters["received"], counters["ingested"], counters["errors"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
