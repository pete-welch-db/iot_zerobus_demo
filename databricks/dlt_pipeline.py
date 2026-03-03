import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


TARGET_THROUGHPUT_CPM = float(spark.conf.get("iot.target_throughput_cpm", "100"))

# This table is expected to be populated by the Zerobus/Lakeflow connector.
RAW_INPUT_TABLE = spark.conf.get("iot.raw_input_table", "raw_iothub_messages")


telemetry_schema = StructType(
    [
        StructField("schema_version", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("vibration_mm_s", DoubleType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("throughput_cpm", IntegerType(), True),
        StructField("state", StringType(), True),
        StructField("fault_code", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("rpm", IntegerType(), True),
        StructField("load_pct", DoubleType(), True),
        StructField("humidity_rh", DoubleType(), True),
        StructField("current_a", DoubleType(), True),
        StructField("power_kw", DoubleType(), True),
        StructField("power_factor", DoubleType(), True),
        StructField("voltage_v", DoubleType(), True),
        StructField("pressure_bar", DoubleType(), True),
        StructField("flow_rate_lpm", DoubleType(), True),
        StructField("cycle_count", StringType(), True),
        StructField("runtime_hours", DoubleType(), True),
    ]
)


@dlt.table(
    comment="Raw IoT Hub messages and transport metadata from Zerobus/Lakeflow source table.",
    table_properties={"quality": "bronze"},
)
def bronze_iot_raw():
    source_df = spark.readStream.table(RAW_INPUT_TABLE)
    columns = set(source_df.columns)

    # Support either Event Hubs envelope columns or already-normalized telemetry rows.
    if {"body", "enqueued_time"}.issubset(columns):
        return source_df.select(
            F.col("body").cast("string").alias("raw_body"),
            F.col("enqueued_time").cast(TimestampType()).alias("enqueued_time"),
            F.col("system_properties").alias("system_properties"),
            F.col("properties").alias("properties"),
            F.current_timestamp().alias("ingest_ts"),
        )

    payload_struct = F.struct(
        F.lit("1.0").alias("schema_version"),
        F.col("machine_id").cast("string").alias("machine_id"),
        F.col("vibration_mm_s").cast("double").alias("vibration_mm_s"),
        F.col("temp_c").cast("double").alias("temp_c"),
        F.col("throughput_cpm").cast("int").alias("throughput_cpm"),
        F.col("state").cast("string").alias("state"),
        F.col("fault_code").cast("string").alias("fault_code"),
        F.col("ts").cast("string").alias("ts"),
        F.col("rpm").cast("int").alias("rpm"),
        F.col("load_pct").cast("double").alias("load_pct"),
        F.col("humidity_rh").cast("double").alias("humidity_rh"),
        F.col("current_a").cast("double").alias("current_a"),
        F.col("power_kw").cast("double").alias("power_kw"),
        F.col("power_factor").cast("double").alias("power_factor"),
        F.col("voltage_v").cast("double").alias("voltage_v"),
        F.col("pressure_bar").cast("double").alias("pressure_bar"),
        F.col("flow_rate_lpm").cast("double").alias("flow_rate_lpm"),
        F.col("cycle_count").cast("string").alias("cycle_count"),
        F.col("runtime_hours").cast("double").alias("runtime_hours"),
    )

    return source_df.select(
        F.to_json(payload_struct).alias("raw_body"),
        F.to_timestamp(F.col("ts")).alias("enqueued_time"),
        F.lit(None).cast("map<string,string>").alias("system_properties"),
        F.lit(None).cast("map<string,string>").alias("properties"),
        F.current_timestamp().alias("ingest_ts"),
    )


@dlt.table(
    comment="Parsed telemetry with typed schema and quality constraints.",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_state", "state IN ('RUN', 'STOPPED', 'FAULT')")
@dlt.expect("non_negative_throughput", "throughput_cpm >= 0")
@dlt.expect("temp_reasonable", "temp_c BETWEEN -20 AND 250")
@dlt.expect("vibration_reasonable", "vibration_mm_s BETWEEN 0 AND 100")
@dlt.expect("load_pct_reasonable", "load_pct IS NULL OR (load_pct BETWEEN 0 AND 100)")
@dlt.expect("power_factor_reasonable", "power_factor IS NULL OR (power_factor BETWEEN 0 AND 1)")
def silver_machine_telemetry():
    parsed = dlt.read_stream("bronze_iot_raw").withColumn(
        "parsed_json",
        F.from_json(F.col("raw_body"), telemetry_schema),
    )

    return (
        parsed.select(
            F.coalesce(F.col("parsed_json.schema_version"), F.lit("1.0")).alias("schema_version"),
            F.coalesce(
                F.to_timestamp(F.col("parsed_json.ts")),
                F.col("enqueued_time"),
            ).alias("event_time"),
            F.col("parsed_json.machine_id").alias("machine_id"),
            F.col("parsed_json.vibration_mm_s").cast("double").alias("vibration_mm_s"),
            F.col("parsed_json.temp_c").cast("double").alias("temp_c"),
            F.col("parsed_json.throughput_cpm").cast("int").alias("throughput_cpm"),
            F.col("parsed_json.state").cast("string").alias("state"),
            F.col("parsed_json.fault_code").cast("string").alias("fault_code"),
            F.col("parsed_json.rpm").cast("int").alias("rpm"),
            F.col("parsed_json.load_pct").cast("double").alias("load_pct"),
            F.col("parsed_json.humidity_rh").cast("double").alias("humidity_rh"),
            F.col("parsed_json.current_a").cast("double").alias("current_a"),
            F.col("parsed_json.power_kw").cast("double").alias("power_kw"),
            F.col("parsed_json.power_factor").cast("double").alias("power_factor"),
            F.col("parsed_json.voltage_v").cast("double").alias("voltage_v"),
            F.col("parsed_json.pressure_bar").cast("double").alias("pressure_bar"),
            F.col("parsed_json.flow_rate_lpm").cast("double").alias("flow_rate_lpm"),
            F.col("parsed_json.cycle_count").cast("bigint").alias("cycle_count"),
            F.col("parsed_json.runtime_hours").cast("double").alias("runtime_hours"),
            F.coalesce(
                F.col("system_properties.iothub-connection-device-id").cast("string"),
                F.col("parsed_json.machine_id").cast("string"),
            ).alias("iothub_device_id"),
            F.col("enqueued_time"),
            F.col("ingest_ts"),
        )
        .where("machine_id IS NOT NULL")
        .where("event_time IS NOT NULL")
    )


@dlt.table(
    comment="Windowed machine health/OEE style KPI aggregates.",
    table_properties={"quality": "gold"},
)
def gold_machine_health_5m():
    silver = dlt.read_stream("silver_machine_telemetry").withWatermark("event_time", "10 minutes")

    windowed = silver.groupBy(
        "machine_id",
        F.window("event_time", "5 minutes").alias("w"),
    ).agg(
        F.avg("vibration_mm_s").alias("avg_vibration_mm_s"),
        F.avg("temp_c").alias("avg_temp_c"),
        F.avg("throughput_cpm").alias("avg_throughput_cpm"),
        F.sum(F.when(F.col("state") == "RUN", F.lit(5)).otherwise(F.lit(0))).alias("time_in_run_s"),
        F.sum(F.when(F.col("state") == "STOPPED", F.lit(5)).otherwise(F.lit(0))).alias("time_in_stopped_s"),
        F.sum(F.when(F.col("state") == "FAULT", F.lit(5)).otherwise(F.lit(0))).alias("time_in_fault_s"),
        F.avg(
            F.when((F.col("state") == "FAULT") | (F.col("fault_code").isNotNull()), F.lit(1.0)).otherwise(F.lit(0.0))
        ).alias("fault_rate"),
    )

    total_time = F.col("time_in_run_s") + F.col("time_in_stopped_s") + F.col("time_in_fault_s")

    availability = F.when(total_time > 0, F.col("time_in_run_s") / total_time).otherwise(F.lit(0.0))
    performance = F.least(F.col("avg_throughput_cpm") / F.lit(TARGET_THROUGHPUT_CPM), F.lit(1.0))
    quality = F.greatest(F.lit(1.0) - F.col("fault_rate"), F.lit(0.0))

    anomaly_score = F.greatest(
        F.lit(0.0),
        F.least(
            F.lit(1.0),
            (F.col("avg_vibration_mm_s") / F.lit(12.0)) * F.lit(0.4)
            + (F.col("avg_temp_c") / F.lit(110.0)) * F.lit(0.4)
            + (F.when(F.col("avg_throughput_cpm") < 20, 1.0).otherwise(0.0)) * F.lit(0.2),
        ),
    )

    return windowed.select(
        F.col("machine_id"),
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        F.col("avg_vibration_mm_s"),
        F.col("avg_temp_c"),
        F.col("avg_throughput_cpm"),
        F.col("time_in_run_s"),
        F.col("time_in_stopped_s"),
        F.col("time_in_fault_s"),
        (availability * 100.0).alias("availability_pct"),
        (performance * 100.0).alias("performance_pct"),
        (quality * 100.0).alias("quality_pct"),
        (availability * performance * quality * 100.0).alias("oee_pct"),
        anomaly_score.alias("anomaly_score"),
        (anomaly_score >= 0.7).alias("is_anomaly"),
        # Simple baseline probability. Replaced/overwritten by ML job output.
        F.greatest(F.lit(0.0), F.least(F.lit(1.0), anomaly_score * 0.9 + F.col("fault_rate") * 0.4)).alias(
            "prob_fault_next_5m"
        ),
    )
