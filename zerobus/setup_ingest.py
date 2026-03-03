"""
Zerobus Ingest setup script (bundle/job friendly).

This is a productionized version of the Databricks-generated starter notebook.
It is designed to run as a DAB spark_python_task and be idempotent.
"""

import argparse
import json
import logging
import os
from typing import Optional
from urllib.parse import urlparse

from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("zerobus-setup")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provision Zerobus ingest stream and target table.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--workspace-url", required=True)
    parser.add_argument("--workspace-region", required=True)
    parser.add_argument("--ingest-url", default="")
    parser.add_argument("--sp-client-id-secret-scope", required=True)
    parser.add_argument("--sp-client-id-secret-key", required=True)
    parser.add_argument("--sp-client-secret-secret-scope", required=True)
    parser.add_argument("--sp-client-secret-secret-key", required=True)
    parser.add_argument("--run-smoke-ingest", choices=["true", "false"], default="false")
    return parser.parse_args()


def get_secret(scope: str, key: str) -> str:
    value = dbutils.secrets.get(scope=scope, key=key)
    if not value:
        raise ValueError(f"Secret lookup returned empty value for scope={scope}, key={key}")
    return value


def ensure_target_table(catalog: str, schema: str, table: str) -> str:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    full_name = f"{catalog}.{schema}.{table}"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
          machine_id STRING,
          vibration_mm_s DOUBLE,
          temp_c DOUBLE,
          throughput_cpm INT,
          state STRING,
          fault_code STRING,
          ts STRING
        )
        """
    )
    return full_name


def grant_permissions(catalog: str, schema: str, table_name: str, client_id: str) -> None:
    principal = f"`{client_id}`"
    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO {principal}").collect()
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO {principal}").collect()
    spark.sql(f"GRANT MODIFY, SELECT ON TABLE {table_name} TO {principal}").collect()


def normalize_workspace_url(workspace_url: str) -> str:
    candidate = workspace_url.strip()
    if not candidate.startswith(("http://", "https://")):
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    return f"{parsed.scheme}://{parsed.netloc}"


def ensure_stream(
    ingest_url: Optional[str],
    workspace_id: str,
    workspace_region: str,
    workspace_url: str,
    table_name: str,
    client_id: str,
    client_secret: str,
    run_smoke_ingest: bool,
) -> None:
    normalized_workspace_url = normalize_workspace_url(workspace_url)
    if ingest_url:
        resolved_ingest_url = ingest_url
        if not resolved_ingest_url.startswith(("http://", "https://")):
            resolved_ingest_url = f"https://{resolved_ingest_url}"
    else:
        resolved_ingest_url = f"https://{workspace_id}.zerobus.{workspace_region}.azuredatabricks.net"
    LOGGER.info("Using Zerobus ingest endpoint: %s", resolved_ingest_url)
    LOGGER.info("Using workspace URL for UC auth: %s", normalized_workspace_url)
    sdk = ZerobusSdk(resolved_ingest_url, unity_catalog_url=normalized_workspace_url)
    table_properties = TableProperties(table_name)
    options = StreamConfigurationOptions(record_type=RecordType.JSON)

    stream = sdk.create_stream(client_id, client_secret, table_properties, options)
    try:
        if run_smoke_ingest:
            record = {
                "machine_id": "MACH_SETUP",
                "vibration_mm_s": 1.0,
                "temp_c": 42.0,
                "throughput_cpm": 10,
                "state": "RUN",
                "fault_code": None,
                "ts": None,
                "setup_payload": json.dumps({"source": "zerobus_setup_job"}),
            }
            stream.ingest_record(record)
            stream.flush()
            LOGGER.info("Smoke ingest record written to %s", table_name)
        else:
            LOGGER.info("Zerobus stream created/validated for %s (no smoke ingest).", table_name)
    finally:
        stream.close()


def main() -> None:
    args = parse_args()

    client_id = get_secret(args.sp_client_id_secret_scope, args.sp_client_id_secret_key)
    client_secret = get_secret(args.sp_client_secret_secret_scope, args.sp_client_secret_secret_key)

    table_name = ensure_target_table(args.catalog, args.schema, args.table)
    grant_permissions(args.catalog, args.schema, table_name, client_id)

    ensure_stream(
        workspace_id=args.workspace_id,
        workspace_region=args.workspace_region,
        ingest_url=args.ingest_url,
        workspace_url=args.workspace_url,
        table_name=table_name,
        client_id=client_id,
        client_secret=client_secret,
        run_smoke_ingest=args.run_smoke_ingest == "true",
    )

    LOGGER.info("Zerobus setup completed successfully for table %s", table_name)


if __name__ == "__main__":
    main()