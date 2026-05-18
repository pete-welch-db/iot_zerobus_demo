"""
Plain-Python IoT Hub -> Zerobus bridge (no Spark).

Reads device telemetry from Azure IoT Hub's Event Hubs-compatible endpoint
using the Azure SDK and forwards each record to Zerobus via the Databricks
Zerobus ingest SDK. Designed to run as a Databricks serverless Python job
with `continuous: pause_status: UNPAUSED` for always-on operation.

Args mirror the Spark bridge so the bundle vars work unchanged.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("iothub-zerobus-bridge-py")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--workspace-url", required=True)
    p.add_argument("--ingest-url", required=True)
    p.add_argument("--sp-client-id-secret-scope", required=True)
    p.add_argument("--sp-client-id-secret-key", required=True)
    p.add_argument("--sp-client-secret-secret-scope", required=True)
    p.add_argument("--sp-client-secret-secret-key", required=True)
    p.add_argument("--iothub-connection-secret-scope", required=True)
    p.add_argument("--iothub-connection-secret-key", required=True)
    p.add_argument("--checkpoint-path", required=False, default="")  # accepted but unused
    p.add_argument("--starting-offsets", choices=["earliest", "latest"], default="latest")
    p.add_argument("--run-mode", choices=["continuous", "available-now"], default="available-now")
    p.add_argument("--processing-time", default="10 seconds")
    p.add_argument("--max-records-per-batch", type=int, default=5000)
    p.add_argument("--realtime-checkpoint-interval", default="5 minutes")
    # Plain-python only knobs
    p.add_argument("--consumer-group", default="databricks-zerobus")
    p.add_argument("--max-runtime-seconds", type=int, default=120,
                   help="Stop the receiver after this many seconds; continuous job will restart.")
    return p.parse_args()


def get_secret(scope: str, key: str) -> str:
    """Fetch a workspace secret. Uses dbutils on Databricks; falls back to SDK."""
    try:
        from databricks.sdk.runtime import dbutils  # type: ignore
        v = dbutils.secrets.get(scope=scope, key=key)
        if v:
            return v
    except Exception:
        pass
    from databricks.sdk import WorkspaceClient
    import base64
    w = WorkspaceClient()
    resp = w.secrets.get_secret(scope=scope, key=key)
    return base64.b64decode(resp.value).decode("utf-8")


def parse_eventhubs_connection(connection_string: str) -> tuple[str, str]:
    parts = dict(p.split("=", 1) for p in connection_string.split(";") if "=" in p)
    endpoint = parts.get("Endpoint", "").rstrip("/")
    entity_path = parts.get("EntityPath", "")
    if not endpoint or not entity_path:
        raise ValueError("Connection string missing Endpoint or EntityPath")
    # Azure SDK accepts the full connection string directly
    return connection_string, entity_path


def main() -> int:
    args = parse_args()
    LOGGER.info("Starting plain-Python bridge: %s.%s.%s", args.catalog, args.schema, args.table)

    sp_client_id = get_secret(args.sp_client_id_secret_scope, args.sp_client_id_secret_key)
    sp_client_secret = get_secret(args.sp_client_secret_secret_scope, args.sp_client_secret_secret_key)
    iothub_conn = get_secret(args.iothub_connection_secret_scope, args.iothub_connection_secret_key)
    _, entity_path = parse_eventhubs_connection(iothub_conn)
    LOGGER.info("Resolved IoT Hub topic: %s", entity_path)

    # --- Zerobus stream ---
    from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
    from zerobus.sdk.sync import ZerobusSdk

    full_table = f"{args.catalog}.{args.schema}.{args.table}"
    workspace_url = args.workspace_url.rstrip("/")
    if not workspace_url.startswith(("http://", "https://")):
        workspace_url = f"https://{workspace_url}"

    LOGGER.info("Zerobus ingest_url=%s workspace_url=%s", args.ingest_url, workspace_url)

    # --- DEEP PROBE: PSC-vs-public-IP TLS handshake ---
    import socket, ssl, os
    from urllib.parse import urlparse
    pu = urlparse(args.ingest_url)
    host, port = pu.hostname, pu.port or 443
    public_ip = os.environ.get("ZEROBUS_PUBLIC_IP", "34.128.32.17")

    def _try_tls(target, sni, label):
        try:
            with socket.create_connection((target, port), timeout=8) as s:
                LOGGER.info("[%s] TCP OK target=%s peer=%s", label, target, s.getpeername())
                ctx = ssl.create_default_context()
                ctx.set_alpn_protocols(["h2"])
                with ctx.wrap_socket(s, server_hostname=sni) as ts:
                    LOGGER.info("[%s] TLS OK proto=%s alpn=%s cipher=%s",
                                label, ts.version(), ts.selected_alpn_protocol(), ts.cipher()[0])
                    return True
        except Exception as e:
            LOGGER.error("[%s] FAILED: %s", label, e)
            return False

    _try_tls(host, host, "PSC-default")
    _try_tls(public_ip, host, "PUBLIC-IP-with-SNI")

    # If public-IP path works, monkey-patch socket.getaddrinfo so anything in this PROCESS
    # that uses libc-style resolution (Python sockets) routes through public IP. The Rust
    # SDK uses its own resolver so this only helps Python clients.
    try:
        _orig_gai = socket.getaddrinfo
        def _patched_gai(host_arg, *a, **kw):
            if isinstance(host_arg, str) and "zerobus" in host_arg and "gcp.databricks.com" in host_arg:
                LOGGER.info("getaddrinfo %s -> %s (patched)", host_arg, public_ip)
                return _orig_gai(public_ip, *a, **kw)
            return _orig_gai(host_arg, *a, **kw)
        socket.getaddrinfo = _patched_gai
        LOGGER.info("Patched socket.getaddrinfo for *zerobus*gcp.databricks.com -> %s", public_ip)
    except Exception as e:
        LOGGER.warning("Could not patch getaddrinfo: %s", e)
    LOGGER.info("Probing TCP %s:%s", host, port)
    try:
        with socket.create_connection((host, port), timeout=10) as s:
            LOGGER.info("TCP connect OK  local=%s peer=%s", s.getsockname(), s.getpeername())
            ctx = ssl.create_default_context()
            with ctx.wrap_socket(s, server_hostname=host) as ts:
                LOGGER.info("TLS handshake OK  proto=%s cipher=%s peer_cert_subject=%s",
                            ts.version(), ts.cipher()[0] if ts.cipher() else '?',
                            ts.getpeercert().get('subject'))
    except Exception as e:
        LOGGER.error("Network probe failed: %s", e)

    # Resolve via DNS to confirm IP path
    try:
        addrs = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        ips = sorted({a[4][0] for a in addrs})
        LOGGER.info("DNS %s -> %s", host, ips)
    except Exception as e:
        LOGGER.warning("DNS resolve failed: %s", e)

    sdk = ZerobusSdk(args.ingest_url, unity_catalog_url=workspace_url)
    stream = sdk.create_stream(
        sp_client_id,
        sp_client_secret,
        TableProperties(full_table),
        StreamConfigurationOptions(record_type=RecordType.JSON),
    )
    LOGGER.info("Zerobus stream created for %s", full_table)

    # --- Event Hubs consumer ---
    from azure.eventhub import EventHubConsumerClient  # type: ignore

    starting_position = "@latest" if args.starting_offsets == "latest" else "-1"

    counters = {"received": 0, "ingested": 0, "errors": 0, "started_at": time.monotonic()}

    def on_event(partition_context, event):
        if event is None:
            return
        try:
            counters["received"] += 1
            body = event.body_as_str()
            try:
                rec = json.loads(body)
            except Exception:
                # Non-JSON message; skip
                counters["errors"] += 1
                return
            # Enrich with EventHubs and bridge timestamps so silver DLT can compute lag.
            eq = getattr(event, "enqueued_time", None)
            if eq is not None:
                rec["iothub_enqueued_time"] = eq.isoformat() if hasattr(eq, "isoformat") else str(eq)
            from datetime import datetime, timezone
            rec["ingest_ts"] = datetime.now(timezone.utc).isoformat()
            try:
                stream.ingest_record(rec)
                counters["ingested"] += 1
            except Exception as e:
                counters["errors"] += 1
                LOGGER.warning("ingest_record failed: %s", e)
            partition_context.update_checkpoint(event)
            if counters["received"] % 500 == 0:
                LOGGER.info(
                    "received=%d ingested=%d errors=%d",
                    counters["received"], counters["ingested"], counters["errors"],
                )
        except Exception as outer:
            counters["errors"] += 1
            LOGGER.exception("on_event handler failed: %s", outer)

    client = EventHubConsumerClient.from_connection_string(
        conn_str=iothub_conn,
        consumer_group=args.consumer_group,
    )
    LOGGER.info("Event Hubs consumer connected (consumer_group=%s, starting=%s)",
                args.consumer_group, starting_position)

    # Stop receiving after max_runtime_seconds so the continuous job can restart cleanly.
    import threading
    def _shutdown():
        LOGGER.info("Max runtime (%ds) reached — closing Event Hubs client.", args.max_runtime_seconds)
        try:
            client.close()
        except Exception as e:
            LOGGER.warning("client.close() raised: %s", e)
    timer = threading.Timer(args.max_runtime_seconds, _shutdown)
    timer.daemon = True
    timer.start()

    try:
        with client:
            client.receive(
                on_event=on_event,
                starting_position=starting_position,
                max_wait_time=15,
            )
    except KeyboardInterrupt:
        pass
    finally:
        timer.cancel()
        try:
            stream.flush()
        except Exception as e:
            LOGGER.warning("Zerobus flush failed: %s", e)
        try:
            stream.close()
        except Exception as e:
            LOGGER.warning("Zerobus close failed: %s", e)
        elapsed = time.monotonic() - counters["started_at"]
        LOGGER.info(
            "Bridge run finished in %.1fs: received=%d ingested=%d errors=%d",
            elapsed, counters["received"], counters["ingested"], counters["errors"],
        )

    return 0


if __name__ == "__main__":
    main()
