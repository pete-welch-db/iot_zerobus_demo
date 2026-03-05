from pyspark import pipelines as dp
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


@dp.table(
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

    # Some environments can have a legacy raw table schema that is missing
    # newly added telemetry fields. Fill absent columns with null so pipeline
    # evolution remains backward-compatible.
    def _col_or_null(name, cast_type):
        if name in columns:
            return F.col(name).cast(cast_type).alias(name)
        return F.lit(None).cast(cast_type).alias(name)

    payload_struct = F.struct(
        _col_or_null("machine_id", "string"),
        _col_or_null("vibration_mm_s", "double"),
        _col_or_null("temp_c", "double"),
        _col_or_null("throughput_cpm", "int"),
        _col_or_null("rpm", "int"),
        _col_or_null("current_amps", "double"),
        _col_or_null("humidity_pct", "double"),
        _col_or_null("load_pct", "double"),
        _col_or_null("power_kw", "double"),
        _col_or_null("power_factor", "double"),
        _col_or_null("voltage_v", "double"),
        _col_or_null("pressure_bar", "double"),
        _col_or_null("flow_rate_lpm", "double"),
        _col_or_null("state", "string"),
        _col_or_null("fault_code", "string"),
        _col_or_null("ts", "string"),
    )

    return source_df.select(
        F.to_json(payload_struct).alias("raw_body"),
        F.to_timestamp(F.col("ts")).alias("enqueued_time"),
        F.lit(None).cast("map<string,string>").alias("system_properties"),
        F.lit(None).cast("map<string,string>").alias("properties"),
        F.current_timestamp().alias("ingest_ts"),
    )


@dp.table(
    comment="Parsed telemetry with typed schema and quality constraints.",
    table_properties={"quality": "silver"},
)
@dp.expect("valid_state", "state IN ('RUN', 'STOPPED', 'FAULT')")
@dp.expect("non_negative_throughput", "throughput_cpm >= 0")
@dp.expect("temp_reasonable", "temp_c BETWEEN -20 AND 250")
@dp.expect("vibration_reasonable", "vibration_mm_s BETWEEN 0 AND 100")
@dp.expect("rpm_reasonable", "rpm BETWEEN 0 AND 10000")
@dp.expect("current_reasonable", "current_amps BETWEEN 0 AND 50")
@dp.expect("humidity_reasonable", "humidity_pct BETWEEN 0 AND 100")
@dp.expect("load_reasonable", "load_pct BETWEEN 0 AND 100")
@dp.expect("power_reasonable", "power_kw BETWEEN 0 AND 200")
@dp.expect("power_factor_reasonable", "power_factor BETWEEN 0 AND 1")
@dp.expect("voltage_reasonable", "voltage_v BETWEEN 100 AND 600")
@dp.expect("pressure_reasonable", "pressure_bar BETWEEN 0 AND 50")
@dp.expect("flow_reasonable", "flow_rate_lpm BETWEEN 0 AND 1000")
def silver_machine_telemetry():
    parsed = spark.readStream.table("bronze_iot_raw").withColumn(
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
            F.coalesce(
                F.col("parsed_json.load_pct").cast("double"),
                F.greatest(F.lit(0.0), F.least(F.lit(100.0), F.col("parsed_json.throughput_cpm") / F.lit(1.2))),
            ).alias("load_pct"),
            F.coalesce(
                F.col("parsed_json.power_kw").cast("double"),
                (F.col("parsed_json.current_amps").cast("double") * F.lit(0.365)).cast("double"),
            ).alias("power_kw"),
            F.coalesce(F.col("parsed_json.power_factor").cast("double"), F.lit(0.92)).alias("power_factor"),
            F.coalesce(F.col("parsed_json.voltage_v").cast("double"), F.lit(230.0)).alias("voltage_v"),
            F.coalesce(F.col("parsed_json.pressure_bar").cast("double"), F.lit(2.5)).alias("pressure_bar"),
            F.coalesce(F.col("parsed_json.flow_rate_lpm").cast("double"), F.lit(40.0)).alias("flow_rate_lpm"),
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


@dp.table(
    comment="Windowed machine health/OEE style KPI aggregates.",
    table_properties={"quality": "gold"},
)
def gold_machine_health_5m():
    silver = spark.readStream.table("silver_machine_telemetry").withWatermark("event_time", "2 minutes")

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
