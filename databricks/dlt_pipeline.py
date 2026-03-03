import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


TARGET_THROUGHPUT_CPM = float(spark.conf.get("iot.target_throughput_cpm", "100"))

# This table is expected to be populated by the Zerobus/Lakeflow connector.
RAW_INPUT_TABLE = spark.conf.get("iot.raw_input_table", "raw_iothub_messages")


telemetry_schema = StructType(
    [
        StructField("machine_id", StringType(), True),
        StructField("vibration_mm_s", DoubleType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("throughput_cpm", IntegerType(), True),
        StructField("rpm", IntegerType(), True),
        StructField("current_amps", DoubleType(), True),
        StructField("humidity_pct", DoubleType(), True),
        StructField("state", StringType(), True),
        StructField("fault_code", StringType(), True),
        StructField("ts", StringType(), True),
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
        F.col("machine_id").cast("string").alias("machine_id"),
        F.col("vibration_mm_s").cast("double").alias("vibration_mm_s"),
        F.col("temp_c").cast("double").alias("temp_c"),
        F.col("throughput_cpm").cast("int").alias("throughput_cpm"),
        F.col("rpm").cast("int").alias("rpm"),
        F.col("current_amps").cast("double").alias("current_amps"),
        F.col("humidity_pct").cast("double").alias("humidity_pct"),
        F.col("state").cast("string").alias("state"),
        F.col("fault_code").cast("string").alias("fault_code"),
        F.col("ts").cast("string").alias("ts"),
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
@dlt.expect("rpm_reasonable", "rpm BETWEEN 0 AND 10000")
@dlt.expect("current_reasonable", "current_amps BETWEEN 0 AND 50")
@dlt.expect("humidity_reasonable", "humidity_pct BETWEEN 0 AND 100")
def silver_machine_telemetry():
    parsed = dlt.read_stream("bronze_iot_raw").withColumn(
        "parsed_json",
        F.from_json(F.col("raw_body"), telemetry_schema),
    )

    return (
        parsed.select(
            F.coalesce(
                F.to_timestamp(F.col("parsed_json.ts")),
                F.col("enqueued_time"),
            ).alias("event_time"),
            F.col("parsed_json.machine_id").alias("machine_id"),
            F.col("parsed_json.vibration_mm_s").cast("double").alias("vibration_mm_s"),
            F.col("parsed_json.temp_c").cast("double").alias("temp_c"),
            F.col("parsed_json.throughput_cpm").cast("int").alias("throughput_cpm"),
            F.coalesce(F.col("parsed_json.rpm").cast("int"), F.lit(0)).alias("rpm"),
            F.coalesce(F.col("parsed_json.current_amps").cast("double"), F.lit(0.0)).alias("current_amps"),
            F.coalesce(F.col("parsed_json.humidity_pct").cast("double"), F.lit(40.0)).alias("humidity_pct"),
            F.col("parsed_json.state").cast("string").alias("state"),
            F.col("parsed_json.fault_code").cast("string").alias("fault_code"),
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

    event_count = F.count("*")

    windowed = silver.groupBy(
        "machine_id",
        F.window("event_time", "5 minutes").alias("w"),
    ).agg(
        F.avg("vibration_mm_s").alias("avg_vibration_mm_s"),
        F.avg("temp_c").alias("avg_temp_c"),
        F.avg("throughput_cpm").alias("avg_throughput_cpm"),
        F.avg("rpm").alias("avg_rpm"),
        F.avg("current_amps").alias("avg_current_amps"),
        F.avg("humidity_pct").alias("avg_humidity_pct"),
        event_count.alias("event_count"),
        F.count(F.when(F.col("state") == "RUN", True)).alias("run_event_count"),
        F.count(F.when(F.col("state") == "STOPPED", True)).alias("stopped_event_count"),
        F.count(F.when(F.col("state") == "FAULT", True)).alias("fault_event_count"),
        F.avg(
            F.when((F.col("state") == "FAULT") | (F.col("fault_code").isNotNull()), F.lit(1.0)).otherwise(F.lit(0.0))
        ).alias("fault_rate"),
    )

    window_seconds = F.lit(300.0)
    safe_event_count = F.greatest(F.col("event_count"), F.lit(1))
    time_in_run_s = (F.col("run_event_count") / safe_event_count) * window_seconds
    time_in_stopped_s = (F.col("stopped_event_count") / safe_event_count) * window_seconds
    time_in_fault_s = (F.col("fault_event_count") / safe_event_count) * window_seconds

    total_time = time_in_run_s + time_in_stopped_s + time_in_fault_s
    availability = F.when(total_time > 0, time_in_run_s / total_time).otherwise(F.lit(0.0))
    performance = F.least(F.col("avg_throughput_cpm") / F.lit(TARGET_THROUGHPUT_CPM), F.lit(1.0))
    quality = F.greatest(F.lit(1.0) - F.col("fault_rate"), F.lit(0.0))

    anomaly_score = F.greatest(
        F.lit(0.0),
        F.least(
            F.lit(1.0),
            (F.col("avg_vibration_mm_s") / F.lit(12.0)) * F.lit(0.25)
            + (F.col("avg_temp_c") / F.lit(110.0)) * F.lit(0.25)
            + (F.col("avg_current_amps") / F.lit(15.0)) * F.lit(0.2)
            + (F.when(F.col("avg_throughput_cpm") < 20, 1.0).otherwise(0.0)) * F.lit(0.15)
            + (F.when(F.col("avg_rpm") > 2800, 0.8).otherwise(F.col("avg_rpm") / F.lit(3500.0))) * F.lit(0.15),
        ),
    )

    return windowed.select(
        F.col("machine_id"),
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        F.col("avg_vibration_mm_s"),
        F.col("avg_temp_c"),
        F.col("avg_throughput_cpm"),
        F.col("avg_rpm"),
        F.col("avg_current_amps"),
        F.col("avg_humidity_pct"),
        F.col("event_count"),
        time_in_run_s.alias("time_in_run_s"),
        time_in_stopped_s.alias("time_in_stopped_s"),
        time_in_fault_s.alias("time_in_fault_s"),
        (availability * 100.0).alias("availability_pct"),
        (performance * 100.0).alias("performance_pct"),
        (quality * 100.0).alias("quality_pct"),
        (availability * performance * quality * 100.0).alias("oee_pct"),
        anomaly_score.alias("anomaly_score"),
        (anomaly_score >= 0.7).alias("is_anomaly"),
        F.greatest(F.lit(0.0), F.least(F.lit(1.0), anomaly_score * 0.9 + F.col("fault_rate") * 0.4)).alias(
            "prob_fault_next_5m"
        ),
    )
