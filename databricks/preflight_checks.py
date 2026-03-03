"""
Preflight checks for the IoT demo orchestration workflow.
"""

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run preflight checks for demo workflow.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--raw-table", required=True)
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--sp-client-id-key", required=True)
    parser.add_argument("--sp-client-secret-key", required=True)
    parser.add_argument("--iothub-connection-key", required=True)
    return parser.parse_args()


def require_secret(scope: str, key: str) -> str:
    value = dbutils.secrets.get(scope=scope, key=key)
    if not value:
        raise ValueError(f"Secret is empty for scope={scope}, key={key}")
    return value


def main() -> None:
    args = parse_args()
    full_raw = f"{args.catalog}.{args.schema}.{args.raw_table}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_raw} (
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
    spark.sql(f"SELECT COUNT(*) AS row_count FROM {full_raw}").collect()

    sp_client_id = require_secret(args.secret_scope, args.sp_client_id_key)
    require_secret(args.secret_scope, args.sp_client_secret_key)
    eventhubs_conn = require_secret(args.secret_scope, args.iothub_connection_key)

    if "Endpoint=" not in eventhubs_conn or "EntityPath=" not in eventhubs_conn:
        raise ValueError("IoT Hub/Event Hubs connection string secret is missing Endpoint or EntityPath.")

    print(
        f"Preflight checks passed for {full_raw}. "
        f"Validated secrets in scope '{args.secret_scope}' for client id '{sp_client_id}'."
    )


if __name__ == "__main__":
    main()
