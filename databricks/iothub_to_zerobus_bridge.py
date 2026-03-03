"""
Bridge Azure IoT Hub (Event Hubs-compatible Kafka endpoint) into a Zerobus stream.

This job consumes IoT Hub messages in micro-batch mode (availableNow) and ingests
records into a Unity Catalog Delta table through Zerobus SDK.
"""

import argparse
import logging
from urllib.parse import urlparse

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("iothub-zerobus-bridge")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge IoT Hub events into Zerobus stream.")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--workspace-url", required=True)
    parser.add_argument("--ingest-url", required=True)
    parser.add_argument("--sp-client-id-secret-scope", required=True)
    parser.add_argument("--sp-client-id-secret-key", required=True)
    parser.add_argument("--sp-client-secret-secret-scope", required=True)
    parser.add_argument("--sp-client-secret-secret-key", required=True)
    parser.add_argument("--iothub-connection-secret-scope", required=True)
    parser.add_argument("--iothub-connection-secret-key", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    return parser.parse_args()


def normalize_workspace_url(workspace_url: str) -> str:
    candidate = workspace_url.strip()
    if not candidate.startswith(("http://", "https://")):
        candidate = f"https://{candidate}"
    parsed = urlparse(candidate)
    return f"{parsed.scheme}://{parsed.netloc}"


def get_secret(scope: str, key: str) -> str:
    value = dbutils.secrets.get(scope=scope, key=key)
    if not value:
        raise ValueError(f"Secret lookup returned empty value for scope={scope}, key={key}")
    return value


def ensure_target_table(full_table_name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
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


def parse_eventhubs_connection_string(connection_string: str) -> tuple[str, str]:
    parts = {}
    for kv in connection_string.split(";"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            parts[k] = v
    endpoint = parts.get("Endpoint", "")
    entity_path = parts.get("EntityPath", "")
    if not endpoint or not entity_path:
        raise ValueError("IoT Hub/Event Hubs connection string must include Endpoint and EntityPath.")
    endpoint_host = urlparse(endpoint).netloc
    if not endpoint_host:
        raise ValueError("Could not parse Endpoint host from Event Hubs connection string.")
    bootstrap_servers = f"{endpoint_host}:9093"
    return bootstrap_servers, entity_path


def build_source_dataframe(connection_string: str):
    bootstrap_servers, topic = parse_eventhubs_connection_string(connection_string)
    telemetry_schema = StructType(
        [
            StructField("machine_id", StringType(), True),
            StructField("vibration_mm_s", DoubleType(), True),
            StructField("temp_c", DoubleType(), True),
            StructField("throughput_cpm", IntegerType(), True),
            StructField("state", StringType(), True),
            StructField("fault_code", StringType(), True),
            StructField("ts", StringType(), True),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{connection_string}";',
        )
        .load()
    )

    parsed_df = (
        kafka_df.select(
            F.col("value").cast("string").alias("raw_body"),
            F.col("timestamp").cast("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("parsed_json", F.from_json(F.col("raw_body"), telemetry_schema))
        .select(
            F.col("parsed_json.machine_id").cast("string").alias("machine_id"),
            F.col("parsed_json.vibration_mm_s").cast("double").alias("vibration_mm_s"),
            F.col("parsed_json.temp_c").cast("double").alias("temp_c"),
            F.col("parsed_json.throughput_cpm").cast("int").alias("throughput_cpm"),
            F.col("parsed_json.state").cast("string").alias("state"),
            F.col("parsed_json.fault_code").cast("string").alias("fault_code"),
            F.coalesce(
                F.col("parsed_json.ts").cast("string"),
                F.date_format(F.col("kafka_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
            ).alias("ts"),
        )
        .where("machine_id IS NOT NULL")
    )
    return parsed_df


def make_batch_writer(
    ingest_url: str,
    workspace_url: str,
    full_table_name: str,
    client_id: str,
    client_secret: str,
):
    def _write_batch(batch_df, batch_id: int) -> None:
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk

        LOGGER.info("Processing IoT Hub batch_id=%s", batch_id)
        sdk = ZerobusSdk(ingest_url, unity_catalog_url=workspace_url)
        table_properties = TableProperties(full_table_name)
        options = StreamConfigurationOptions(record_type=RecordType.JSON)
        stream = sdk.create_stream(client_id, client_secret, table_properties, options)
        try:
            rows = batch_df.toLocalIterator()
            count = 0
            for row in rows:
                stream.ingest_record(
                    {
                        "machine_id": row["machine_id"],
                        "vibration_mm_s": row["vibration_mm_s"],
                        "temp_c": row["temp_c"],
                        "throughput_cpm": row["throughput_cpm"],
                        "state": row["state"],
                        "fault_code": row["fault_code"],
                        "ts": row["ts"],
                    }
                )
                count += 1
            stream.flush()
            LOGGER.info("Ingested %s records to Zerobus for batch_id=%s", count, batch_id)
        finally:
            stream.close()

    return _write_batch


def main() -> None:
    args = parse_args()

    full_table_name = f"{args.catalog}.{args.schema}.{args.table}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    ensure_target_table(full_table_name)

    client_id = get_secret(args.sp_client_id_secret_scope, args.sp_client_id_secret_key)
    client_secret = get_secret(args.sp_client_secret_secret_scope, args.sp_client_secret_secret_key)
    iothub_connection = get_secret(args.iothub_connection_secret_scope, args.iothub_connection_secret_key)

    workspace_url = normalize_workspace_url(args.workspace_url)
    ingest_url = args.ingest_url if args.ingest_url.startswith(("http://", "https://")) else f"https://{args.ingest_url}"

    source_df = build_source_dataframe(iothub_connection)
    writer = make_batch_writer(
        ingest_url=ingest_url,
        workspace_url=workspace_url,
        full_table_name=full_table_name,
        client_id=client_id,
        client_secret=client_secret,
    )

    query = (
        source_df.writeStream.foreachBatch(writer)
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    LOGGER.info("IoT Hub -> Zerobus bridge run completed.")


if __name__ == "__main__":
    main()
