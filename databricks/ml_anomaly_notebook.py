import mlflow
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from pyspark.sql import functions as F


catalog = spark.conf.get("iot.catalog", "main")
schema = spark.conf.get("iot.schema", "iot_demo")
silver_table = f"{catalog}.{schema}.silver_machine_telemetry"
output_table = f"{catalog}.{schema}.ml_anomaly_scores"
experiment_name = spark.conf.get("iot.mlflow.experiment", "/Shared/iot_zerobus_demo")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
mlflow.set_experiment(experiment_name)

# Limit to a practical sample for fast retrains during demos.
source_df = (
    spark.table(silver_table)
    .where("event_time >= current_timestamp() - INTERVAL 2 DAYS")
    .select("machine_id", "event_time", "vibration_mm_s", "temp_c", "throughput_cpm", "state")
    .dropna()
)

pdf = source_df.toPandas()
if pdf.empty:
    raise ValueError("No telemetry found in silver table for anomaly training.")

feature_cols = ["vibration_mm_s", "temp_c", "throughput_cpm"]
X = pdf[feature_cols].astype(float).values

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Rule baseline helps maintain interpretability for the demo.
rule_flag = (
    (pdf["vibration_mm_s"] > 9.5)
    | (pdf["temp_c"] > 85.0)
    | ((pdf["state"] == "RUN") & (pdf["throughput_cpm"] < 15))
).astype(float)

with mlflow.start_run(run_name="iot_anomaly_isolation_forest"):
    model = IsolationForest(
        n_estimators=150,
        contamination=0.05,
        random_state=42,
    )
    model.fit(X_scaled)

    decision = model.decision_function(X_scaled)
    model_score = 1.0 / (1.0 + np.exp(4.0 * decision))

    # Blend rule and model score for explainability.
    blended_score = np.clip((model_score * 0.7) + (rule_flag * 0.3), 0.0, 1.0)
    is_anomaly = blended_score >= 0.70

    pdf["anomaly_score"] = blended_score
    pdf["is_anomaly"] = is_anomaly

    mlflow.log_params(
        {
            "feature_cols": ",".join(feature_cols),
            "contamination": 0.05,
            "n_estimators": 150,
        }
    )
    mlflow.log_metric("anomaly_rate", float(np.mean(is_anomaly)))
    mlflow.sklearn.log_model(model, artifact_path="isolation_forest_model")

result_df = spark.createDataFrame(
    pdf[["machine_id", "event_time", "anomaly_score", "is_anomaly"]]
).withColumn("scored_at", F.current_timestamp())

(
    result_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable(output_table)
)
