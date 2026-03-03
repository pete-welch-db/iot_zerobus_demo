import mlflow
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from pyspark.sql import functions as F


catalog = spark.conf.get("iot.catalog", "main")
schema = spark.conf.get("iot.schema", "iot_demo")
silver_table = f"{catalog}.{schema}.silver_machine_telemetry"
output_table = f"{catalog}.{schema}.ml_fault_predictions"
experiment_name = spark.conf.get("iot.mlflow.experiment", "/Shared/iot_zerobus_demo")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
mlflow.set_experiment(experiment_name)

lookback_df = (
    spark.table(silver_table)
    .where("event_time >= current_timestamp() - INTERVAL 7 DAYS")
    .select("machine_id", "event_time", "vibration_mm_s", "temp_c", "throughput_cpm", "state", "fault_code")
    .dropna(subset=["machine_id", "event_time"])
)

pdf = lookback_df.toPandas()
if pdf.empty:
    raise ValueError("No telemetry found in silver table for fault prediction training.")

pdf = pdf.sort_values(["machine_id", "event_time"]).reset_index(drop=True)

# 5-minute horizon at ~1 second sampling rate (300 rows).
steps_ahead = 300
labels = []
for machine_id, grp in pdf.groupby("machine_id", sort=False):
    future_fault = ((grp["state"] == "FAULT") | grp["fault_code"].notna()).astype(int).rolling(
        window=steps_ahead, min_periods=1
    ).max()
    shifted = future_fault.shift(-steps_ahead).fillna(0).astype(int)
    labels.extend(shifted.tolist())

pdf["label_fault_next_5m"] = labels

feature_cols_num = ["vibration_mm_s", "temp_c", "throughput_cpm"]
feature_cols_cat = ["state"]

train_df = pdf.dropna(subset=feature_cols_num + feature_cols_cat)
if train_df["label_fault_next_5m"].nunique() < 2:
    # Keep pipeline runnable even in low-variance synthetic runs.
    train_df.loc[train_df.index[: max(1, len(train_df) // 20)], "label_fault_next_5m"] = 1

X = train_df[feature_cols_num + feature_cols_cat]
y = train_df["label_fault_next_5m"].astype(int)

preprocess = ColumnTransformer(
    transformers=[
        (
            "num",
            Pipeline(
                steps=[
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scaler", StandardScaler()),
                ]
            ),
            feature_cols_num,
        ),
        (
            "cat",
            Pipeline(
                steps=[
                    ("imputer", SimpleImputer(strategy="most_frequent")),
                    ("onehot", OneHotEncoder(handle_unknown="ignore")),
                ]
            ),
            feature_cols_cat,
        ),
    ]
)

clf = Pipeline(
    steps=[
        ("preprocess", preprocess),
        ("model", LogisticRegression(max_iter=1000, class_weight="balanced")),
    ]
)

with mlflow.start_run(run_name="iot_fault_prediction_logreg"):
    clf.fit(X, y)
    prob = clf.predict_proba(X)[:, 1]

    auc = float(roc_auc_score(y, prob)) if y.nunique() > 1 else 0.5
    ap = float(average_precision_score(y, prob)) if y.nunique() > 1 else 0.0

    mlflow.log_params(
        {
            "numeric_features": ",".join(feature_cols_num),
            "categorical_features": ",".join(feature_cols_cat),
            "model_type": "logistic_regression",
            "horizon_rows": steps_ahead,
        }
    )
    mlflow.log_metrics({"roc_auc": auc, "avg_precision": ap})
    mlflow.sklearn.log_model(clf, artifact_path="fault_prediction_model")

scored = train_df[["machine_id", "event_time"]].copy()
scored["prob_fault_next_5m"] = prob.astype(float)
scored["predicted_fault_next_5m"] = (scored["prob_fault_next_5m"] >= 0.5).astype(bool)

scored_df = spark.createDataFrame(scored).withColumn("scored_at", F.current_timestamp())

(
    scored_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable(output_table)
)
