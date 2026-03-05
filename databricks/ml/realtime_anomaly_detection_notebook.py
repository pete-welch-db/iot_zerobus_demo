# Databricks notebook source
# MAGIC %md
# MAGIC # Real-Time Anomaly Detection
# MAGIC Scans recent streaming data for anomalies using the trained ML model.

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "iot_telemetry")
dbutils.widgets.text("lookback_hours", "24")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
lookback_hours = int(dbutils.widgets.get("lookback_hours"))

# COMMAND ----------
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import mlflow

# Load the latest delivery risk model
model_name = f"{catalog}.{schema}.delivery_risk_model"
model = mlflow.sklearn.load_model(f"models:/{model_name}/latest")

# COMMAND ----------
# Get recent streaming data
cutoff_time = datetime.utcnow() - timedelta(hours=lookback_hours)

streaming_df = spark.table(f"{catalog}.{schema}.machine_events").filter(
    F.col("ts") >= F.lit(cutoff_time.strftime('%Y-%m-%d %H:%M:%S'))
)

recent_count = streaming_df.count()
print(f"Analyzing {recent_count:,} streaming records from last {lookback_hours} hours")

# COMMAND ----------
# Detect anomalies (example: high vibration with high temp)
anomalies = streaming_df.filter(
    (F.col("vibration_mm_s") > 8.0)
    & (F.col("temp_c") > 85.0)
    & (F.col("state") == "running")
).select(
    "machine_id",
    "ts",
    "vibration_mm_s",
    "temp_c",
    "rpm",
    "state"
).orderBy(F.desc("ts"))

anomaly_count = anomalies.count()

if anomaly_count > 0:
    print(f"⚠️  Found {anomaly_count} anomalies in streaming data")
    anomalies.show(10, truncate=False)

    # Write anomalies to alerts table
    anomalies.withColumn("alert_type", F.lit("streaming_anomaly")) \
        .withColumn("detected_at", F.current_timestamp()) \
        .write.format("delta").mode("append") \
        .saveAsTable(f"{catalog}.{schema}.realtime_alerts")
else:
    print("✅ No anomalies detected in recent streaming data")
