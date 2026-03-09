from pyspark import pipelines as dp
from pyspark.sql import functions as F


TARGET_THROUGHPUT_CPM = float(spark.conf.get("iot.target_throughput_cpm", "100"))

# Bronze table written directly by the Zerobus bridge (typed columns + raw_body + timestamps).
BRONZE_TABLE = spark.conf.get("iot.raw_input_table", "bronze_iot_telemetry")


@dp.table(
    comment="Cleaned telemetry with quality constraints. Reads directly from Zerobus-managed bronze table.",
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
    bronze = spark.readStream.table(BRONZE_TABLE)

    return (
        bronze.select(
            F.coalesce(
                F.to_timestamp(F.col("ts")),
                F.to_timestamp(F.col("iothub_enqueued_time")),
            ).alias("event_time"),
            F.col("machine_id"),
            F.col("vibration_mm_s").cast("double"),
            F.col("temp_c").cast("double"),
            F.col("throughput_cpm").cast("int"),
            F.coalesce(F.col("rpm").cast("int"), F.lit(0)).alias("rpm"),
            F.coalesce(F.col("current_amps").cast("double"), F.lit(0.0)).alias("current_amps"),
            F.coalesce(F.col("humidity_pct").cast("double"), F.lit(40.0)).alias("humidity_pct"),
            F.coalesce(
                F.col("load_pct").cast("double"),
                F.greatest(F.lit(0.0), F.least(F.lit(100.0), F.col("throughput_cpm") / F.lit(1.2))),
            ).alias("load_pct"),
            F.coalesce(
                F.col("power_kw").cast("double"),
                (F.col("current_amps").cast("double") * F.lit(0.365)).cast("double"),
            ).alias("power_kw"),
            F.coalesce(F.col("power_factor").cast("double"), F.lit(0.92)).alias("power_factor"),
            F.coalesce(F.col("voltage_v").cast("double"), F.lit(230.0)).alias("voltage_v"),
            F.coalesce(F.col("pressure_bar").cast("double"), F.lit(2.5)).alias("pressure_bar"),
            F.coalesce(F.col("flow_rate_lpm").cast("double"), F.lit(40.0)).alias("flow_rate_lpm"),
            F.col("state").cast("string"),
            F.col("fault_code").cast("string"),
            F.col("machine_id").alias("iothub_device_id"),
            F.to_timestamp(F.col("iothub_enqueued_time")).alias("enqueued_time"),
            F.to_timestamp(F.col("ingest_ts")).alias("ingest_ts"),
            (
                (F.unix_timestamp(F.to_timestamp(F.col("iothub_enqueued_time")))
                 - F.unix_timestamp(F.coalesce(
                     F.to_timestamp(F.col("ts")),
                     F.to_timestamp(F.col("iothub_enqueued_time")),
                 )))
                * F.lit(1000)
            ).cast("bigint").alias("device_to_hub_ms"),
            (
                (F.unix_timestamp(F.to_timestamp(F.col("ingest_ts")))
                 - F.unix_timestamp(F.to_timestamp(F.col("iothub_enqueued_time"))))
                * F.lit(1000)
            ).cast("bigint").alias("hub_to_bridge_ms"),
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


@dp.table(
    comment="Real-time current machine status. Streaming table optimized for <5s telemetry freshness.",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def gold_machine_current_status():
    silver = spark.readStream.table("silver_machine_telemetry").withWatermark("event_time", "0 seconds")

    # Short windows keep updates near real-time while preserving streaming-table semantics.
    latest_per_window = silver.groupBy(
        "machine_id",
        F.window("event_time", "10 seconds").alias("w"),
    ).agg(
        F.max("event_time").alias("last_event_time"),
        F.max_by(F.col("state"), F.col("event_time")).alias("state"),
        F.max_by(F.col("vibration_mm_s"), F.col("event_time")).alias("vibration_mm_s"),
        F.max_by(F.col("temp_c"), F.col("event_time")).alias("temp_c"),
        F.max_by(F.col("throughput_cpm"), F.col("event_time")).alias("throughput_cpm"),
        F.max_by(F.col("rpm"), F.col("event_time")).alias("rpm"),
        F.max_by(F.col("current_amps"), F.col("event_time")).alias("current_amps"),
        F.max_by(F.col("humidity_pct"), F.col("event_time")).alias("humidity_pct"),
        F.max_by(F.col("load_pct"), F.col("event_time")).alias("load_pct"),
        F.max_by(F.col("power_kw"), F.col("event_time")).alias("power_kw"),
        F.max_by(F.col("power_factor"), F.col("event_time")).alias("power_factor"),
        F.max_by(F.col("voltage_v"), F.col("event_time")).alias("voltage_v"),
        F.max_by(F.col("pressure_bar"), F.col("event_time")).alias("pressure_bar"),
        F.max_by(F.col("flow_rate_lpm"), F.col("event_time")).alias("flow_rate_lpm"),
        F.max_by(F.col("fault_code"), F.col("event_time")).alias("fault_code"),
        F.max_by(F.col("iothub_device_id"), F.col("event_time")).alias("iothub_device_id"),
    )

    return latest_per_window.select(
        F.col("machine_id"),
        F.col("last_event_time"),
        F.col("state"),
        F.col("vibration_mm_s"),
        F.col("temp_c"),
        F.col("throughput_cpm"),
        F.col("rpm"),
        F.col("current_amps"),
        F.col("humidity_pct"),
        F.col("load_pct"),
        F.col("power_kw"),
        F.col("power_factor"),
        F.col("voltage_v"),
        F.col("pressure_bar"),
        F.col("flow_rate_lpm"),
        F.col("fault_code"),
        F.col("iothub_device_id"),
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("last_event_time")))
        .cast("int")
        .alias("telemetry_lag_seconds"),
        (
            (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("last_event_time"))) * F.lit(1000)
        )
        .cast("bigint")
        .alias("telemetry_lag_ms"),
    )
