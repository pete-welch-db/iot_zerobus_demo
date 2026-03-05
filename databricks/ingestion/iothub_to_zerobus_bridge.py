"""
Bridge Azure IoT Hub (Event Hubs-compatible Kafka endpoint) into a Zerobus stream.

This job consumes IoT Hub messages in continuous streaming mode and ingests
records into a Unity Catalog Delta table through Zerobus SDK with robust
error handling and retry logic for "always on" operation.
"""

import argparse
import logging
import time
from urllib.parse import urlparse

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("iothub-zerobus-bridge")

# Retry configuration for Zerobus ingestion
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 2
MAX_BACKOFF_SECONDS = 60
BATCH_SIZE_LIMIT = 5000  # Limit records per batch to avoid OOM


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
    parser.add_argument(
        "--starting-offsets",
        choices=["earliest", "latest"],
        default="latest",
        help="Kafka/Event Hubs starting offsets mode. Use latest for live demos.",
    )
    parser.add_argument(
        "--run-mode",
        choices=["continuous", "available-now"],
        default="continuous",
        help="Streaming runtime mode. Use continuous for always-on ingestion.",
    )
    parser.add_argument(
        "--processing-time",
        default="10 seconds",
        help="Processing trigger interval for continuous mode (default: 10 seconds).",
    )
    parser.add_argument(
        "--max-records-per-batch",
        type=int,
        default=BATCH_SIZE_LIMIT,
        help=f"Maximum records to process per batch to avoid OOM (default: {BATCH_SIZE_LIMIT}).",
    )
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
          rpm INT,
          current_amps DOUBLE,
          humidity_pct DOUBLE,
          load_pct DOUBLE,
          power_kw DOUBLE,
          power_factor DOUBLE,
          voltage_v DOUBLE,
          pressure_bar DOUBLE,
          flow_rate_lpm DOUBLE,
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


def build_source_dataframe(connection_string: str, starting_offsets: str):
    bootstrap_servers, topic = parse_eventhubs_connection_string(connection_string)
    telemetry_schema = StructType(
        [
            StructField("machine_id", StringType(), True),
            StructField("vibration_mm_s", DoubleType(), True),
            StructField("temp_c", DoubleType(), True),
            StructField("throughput_cpm", IntegerType(), True),
            StructField("rpm", IntegerType(), True),
            StructField("current_amps", DoubleType(), True),
            StructField("humidity_pct", DoubleType(), True),
            StructField("load_pct", DoubleType(), True),
            StructField("power_kw", DoubleType(), True),
            StructField("power_factor", DoubleType(), True),
            StructField("voltage_v", DoubleType(), True),
            StructField("pressure_bar", DoubleType(), True),
            StructField("flow_rate_lpm", DoubleType(), True),
            StructField("state", StringType(), True),
            StructField("fault_code", StringType(), True),
            StructField("ts", StringType(), True),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", 10000)
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
            F.col("parsed_json.rpm").cast("int").alias("rpm"),
            F.col("parsed_json.current_amps").cast("double").alias("current_amps"),
            F.col("parsed_json.humidity_pct").cast("double").alias("humidity_pct"),
            F.col("parsed_json.load_pct").cast("double").alias("load_pct"),
            F.col("parsed_json.power_kw").cast("double").alias("power_kw"),
            F.col("parsed_json.power_factor").cast("double").alias("power_factor"),
            F.col("parsed_json.voltage_v").cast("double").alias("voltage_v"),
            F.col("parsed_json.pressure_bar").cast("double").alias("pressure_bar"),
            F.col("parsed_json.flow_rate_lpm").cast("double").alias("flow_rate_lpm"),
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


_RECORD_FIELDS = [
    "machine_id", "vibration_mm_s", "temp_c", "throughput_cpm",
    "rpm", "current_amps", "humidity_pct",
    "load_pct", "power_kw", "power_factor", "voltage_v", "pressure_bar", "flow_rate_lpm",
    "state", "fault_code", "ts",
]


def make_batch_writer(
    ingest_url: str,
    workspace_url: str,
    full_table_name: str,
    client_id: str,
    client_secret: str,
    max_records_per_batch: int,
):
    """
    Create a batch writer with robust error handling and retry logic.
    Uses connection pooling and health checks for "always on" operation.
    """
    _cached = {}
    _metrics = {"total_batches": 0, "total_records": 0, "total_errors": 0, "total_retries": 0}

    def _get_stream(force_reconnect: bool = False):
        """Get or create Zerobus stream with connection health check."""
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
        from zerobus.sdk.sync import ZerobusSdk

        if force_reconnect and "stream" in _cached:
            LOGGER.info("Forcing Zerobus stream reconnection.")
            try:
                _cached["stream"].close()
            except Exception as e:
                LOGGER.warning("Error closing stale stream: %s", e)
            del _cached["stream"]

        if "stream" not in _cached:
            LOGGER.info("Creating new Zerobus SDK stream for table %s", full_table_name)
            try:
                sdk = ZerobusSdk(ingest_url, unity_catalog_url=workspace_url)
                table_properties = TableProperties(full_table_name)
                options = StreamConfigurationOptions(record_type=RecordType.JSON)
                _cached["stream"] = sdk.create_stream(client_id, client_secret, table_properties, options)
                LOGGER.info("Zerobus stream created successfully.")
            except Exception as e:
                LOGGER.error("Failed to create Zerobus stream: %s", e)
                raise
        return _cached["stream"]

    def _ingest_with_retry(stream, records: list, attempt: int = 1) -> bool:
        """Ingest records with exponential backoff retry logic."""
        try:
            for record in records:
                stream.ingest_record(record)
            stream.flush()
            return True
        except Exception as e:
            _metrics["total_errors"] += 1
            if attempt >= MAX_RETRIES:
                LOGGER.error(
                    "Failed to ingest batch after %d attempts: %s. Skipping batch to prevent stream failure.",
                    MAX_RETRIES, e
                )
                return False
            
            backoff = min(INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1)), MAX_BACKOFF_SECONDS)
            LOGGER.warning(
                "Ingestion failed (attempt %d/%d): %s. Retrying in %d seconds...",
                attempt, MAX_RETRIES, e, backoff
            )
            _metrics["total_retries"] += 1
            time.sleep(backoff)
            
            # Reconnect on connection errors
            if "connection" in str(e).lower() or "timeout" in str(e).lower():
                LOGGER.info("Connection error detected, forcing reconnect.")
                stream = _get_stream(force_reconnect=True)
            
            return _ingest_with_retry(stream, records, attempt + 1)

    def _write_batch(batch_df, batch_id: int) -> None:
        """Process batch with error handling, size limiting, and monitoring."""
        start_time = time.time()
        LOGGER.info("Processing IoT Hub batch_id=%s", batch_id)
        _metrics["total_batches"] += 1
        
        try:
            # Limit batch size to prevent OOM
            record_count = batch_df.count()
            if record_count > max_records_per_batch:
                LOGGER.warning(
                    "Batch size %d exceeds limit %d. Processing first %d records.",
                    record_count, max_records_per_batch, max_records_per_batch
                )
                batch_df = batch_df.limit(max_records_per_batch)
            
            records = [
                {f: row[f] for f in _RECORD_FIELDS}
                for row in batch_df.collect()
            ]
            
            if not records:
                LOGGER.info("Empty batch_id=%s, skipping.", batch_id)
                return
            
            # Get stream and ingest with retry
            stream = _get_stream()
            success = _ingest_with_retry(stream, records)
            
            if success:
                _metrics["total_records"] += len(records)
                elapsed = time.time() - start_time
                LOGGER.info(
                    "Successfully ingested %d records to Zerobus for batch_id=%s in %.2fs (rate: %.1f rec/s). "
                    "Total: %d batches, %d records, %d errors, %d retries.",
                    len(records), batch_id, elapsed, len(records) / elapsed if elapsed > 0 else 0,
                    _metrics["total_batches"], _metrics["total_records"],
                    _metrics["total_errors"], _metrics["total_retries"]
                )
            else:
                LOGGER.error(
                    "Failed to ingest batch_id=%s after retries. Data may be lost. "
                    "Check Zerobus connection and credentials.",
                    batch_id
                )
        
        except Exception as e:
            _metrics["total_errors"] += 1
            LOGGER.error(
                "Unexpected error processing batch_id=%s: %s. Stream will continue. "
                "Metrics: %d batches, %d records, %d errors.",
                batch_id, e, _metrics["total_batches"], _metrics["total_records"], _metrics["total_errors"],
                exc_info=True
            )
            # Don't raise - let stream continue for "always on" operation

    return _write_batch


def main() -> None:
    args = parse_args()

    full_table_name = f"{args.catalog}.{args.schema}.{args.table}"
    LOGGER.info("Starting IoT Hub -> Zerobus bridge for table: %s", full_table_name)
    LOGGER.info("Run mode: %s, Processing time: %s, Max records/batch: %d",
                args.run_mode, args.processing_time, args.max_records_per_batch)
    
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    ensure_target_table(full_table_name)

    client_id = get_secret(args.sp_client_id_secret_scope, args.sp_client_id_secret_key)
    client_secret = get_secret(args.sp_client_secret_secret_scope, args.sp_client_secret_secret_key)
    iothub_connection = get_secret(args.iothub_connection_secret_scope, args.iothub_connection_secret_key)

    workspace_url = normalize_workspace_url(args.workspace_url)
    ingest_url = args.ingest_url if args.ingest_url.startswith(("http://", "https://")) else f"https://{args.ingest_url}"

    source_df = build_source_dataframe(iothub_connection, args.starting_offsets)
    writer = make_batch_writer(
        ingest_url=ingest_url,
        workspace_url=workspace_url,
        full_table_name=full_table_name,
        client_id=client_id,
        client_secret=client_secret,
        max_records_per_batch=args.max_records_per_batch,
    )

    query_builder = source_df.writeStream.foreachBatch(writer).option("checkpointLocation", args.checkpoint_path)
    
    if args.run_mode == "available-now":
        LOGGER.info("Starting bridge in available-now mode (one-shot sweep).")
        query = query_builder.trigger(availableNow=True).start()
    else:
        LOGGER.info("Starting bridge in CONTINUOUS mode with processing time %s for 'always on' operation.", args.processing_time)
        query = query_builder.trigger(processingTime=args.processing_time).start()
    
    query.awaitTermination()
    LOGGER.info("IoT Hub -> Zerobus bridge run completed or terminated.")


if __name__ == "__main__":
    main()
