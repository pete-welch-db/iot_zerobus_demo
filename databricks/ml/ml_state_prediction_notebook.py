import argparse
import mlflow
import pandas as pd
from pyspark.sql import functions as F, Window as W
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and score fault prediction model for IoT demo.")
    parser.add_argument("--catalog", default="main")
    parser.add_argument("--schema", default="iot_demo")
    parser.add_argument("--mlflow-experiment", default="/Shared/iot_zerobus_demo")
    parser.add_argument("--inference-mode", choices=["batch", "realtime", "both"], default="both")
    parser.add_argument("--train-model", choices=["true", "false"], default="true")
    parser.add_argument("--batch-lookback-hours", type=int, default=24)
    parser.add_argument("--realtime-lookback-minutes", type=int, default=10)
    return parser.parse_args()


args = parse_args()
catalog = args.catalog
schema = args.schema
silver_table = f"{catalog}.{schema}.silver_machine_telemetry"
output_table = f"{catalog}.{schema}.ml_fault_predictions"
history_table = f"{catalog}.{schema}.ml_fault_predictions_history"
experiment_name = args.mlflow_experiment
train_model = args.train_model.lower() == "true"
inference_mode = args.inference_mode
batch_lookback_hours = args.batch_lookback_hours
realtime_lookback_minutes = args.realtime_lookback_minutes

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
mlflow.set_experiment(experiment_name)
artifact_path = "fault_prediction_pipeline_model"

feature_cols_num = ["vibration_mm_s", "temp_c", "throughput_cpm", "rpm", "current_amps", "humidity_pct"]
feature_cols_cat = ["state"]
feature_cols_all = feature_cols_num + feature_cols_cat

_SELECT_COLS = ["machine_id", "event_time"] + feature_cols_all + ["fault_code"]

HORIZON_SECONDS = 300
HORIZON_SPECS = (
    ("5m", 300),
    ("1h", 3600),
    ("24h", 86400),
    ("7d", 604800),
)


def _feature_struct():
    # Preserve feature names for sklearn ColumnTransformer inside MLflow pyfunc.
    return F.struct(*[F.col(c).alias(c) for c in feature_cols_all])


def _build_labeled_spark_df(source_df):
    """Label rows with whether a FAULT occurs within the next 5 minutes using Spark window functions."""
    w_future = (
        W.partitionBy("machine_id")
        .orderBy(F.col("event_time").cast("long"))
        .rangeBetween(1, HORIZON_SECONDS)
    )
    is_fault_flag = F.when(
        (F.col("state") == "FAULT") | F.col("fault_code").isNotNull(), F.lit(1)
    ).otherwise(F.lit(0))

    labeled = source_df.withColumn(
        "label_fault_next_5m",
        F.when(F.max(is_fault_flag).over(w_future) >= 1, F.lit(1)).otherwise(F.lit(0)),
    )
    return labeled


def _expand_horizons_from_5m(prob_col):
    """Project short-horizon fault probability into planning horizons."""
    prob = F.greatest(F.lit(0.0), F.least(F.lit(1.0), prob_col))
    result = {}
    for label, seconds in HORIZON_SPECS:
        if label == "5m":
            result[label] = prob
            continue
        ratio = float(seconds) / float(HORIZON_SECONDS)
        projected = F.lit(1.0) - F.pow(F.lit(1.0) - prob, F.lit(ratio))
        result[label] = F.greatest(F.lit(0.0), F.least(F.lit(1.0), projected))
    return result


def _latest_training_run_model_uri() -> str:
    exp = mlflow.get_experiment_by_name(experiment_name)
    if exp is None:
        raise ValueError(f"MLflow experiment not found: {experiment_name}")
    runs = mlflow.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string="tags.task = 'fault_training'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    if runs.empty:
        raise ValueError("No prior fault training run found. Run once with --train-model true.")
    run_id = runs.iloc[0]["run_id"]
    return f"runs:/{run_id}/{artifact_path}"


with mlflow.start_run(run_name=f"iot_fault_pipeline_{inference_mode}"):
    mlflow.set_tags(
        {
            "pipeline": "iot_zerobus_demo",
            "use_case": "predictive_maintenance",
            "task": "fault_pipeline",
            "inference_mode": inference_mode,
        }
    )
    mlflow.log_params(
        {
            "catalog": catalog,
            "schema": schema,
            "batch_lookback_hours": batch_lookback_hours,
            "realtime_lookback_minutes": realtime_lookback_minutes,
            "train_model": train_model,
        }
    )

    if train_model:
        training_sdf = (
            spark.table(silver_table)
            .where("event_time >= current_timestamp() - INTERVAL 7 DAYS")
            .select(*_SELECT_COLS)
            .dropna(subset=["machine_id", "event_time"])
        )
        if training_sdf.count() == 0:
            raise ValueError("No telemetry found in silver table for fault prediction training.")

        labeled_sdf = _build_labeled_spark_df(training_sdf)
        train_df = labeled_sdf.dropna(subset=feature_cols_all).toPandas()
        if train_df.empty:
            raise ValueError("No rows available for fault model training after preprocessing.")
        if train_df["label_fault_next_5m"].nunique() < 2:
            train_df.loc[train_df.index[: max(1, len(train_df) // 20)], "label_fault_next_5m"] = 1

        X = train_df[feature_cols_all]
        y = train_df["label_fault_next_5m"].astype(int)

        preprocess = ColumnTransformer(
            transformers=[
                ("num", Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler())]), feature_cols_num),
                ("cat", Pipeline([("imputer", SimpleImputer(strategy="most_frequent")), ("onehot", OneHotEncoder(handle_unknown="ignore"))]), feature_cols_cat),
            ]
        )
        clf = Pipeline([("preprocess", preprocess), ("model", LogisticRegression(max_iter=1000, class_weight="balanced"))])

        with mlflow.start_run(run_name="iot_fault_training", nested=True) as train_run:
            mlflow.set_tags({"task": "fault_training"})
            clf.fit(X, y)
            train_prob = clf.predict_proba(X)[:, 1]
            auc = float(roc_auc_score(y, train_prob)) if y.nunique() > 1 else 0.5
            ap = float(average_precision_score(y, train_prob)) if y.nunique() > 1 else 0.0
            mlflow.log_params(
                {
                    "numeric_features": ",".join(feature_cols_num),
                    "categorical_features": ",".join(feature_cols_cat),
                    "model_type": "logistic_regression",
                    "horizon_seconds": HORIZON_SECONDS,
                    "train_rows": int(len(train_df)),
                }
            )
            mlflow.log_metrics({"roc_auc_train": auc, "avg_precision_train": ap})
            mlflow.sklearn.log_model(clf, artifact_path=artifact_path)
            model_run_id = train_run.info.run_id
            model_uri = f"runs:/{model_run_id}/{artifact_path}"
    else:
        model_uri = _latest_training_run_model_uri()
        model_run_id = model_uri.split("/")[1]

    predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="double")

    scored_segments = []
    requested_modes = ["batch", "realtime"] if inference_mode == "both" else [inference_mode]

    for mode in requested_modes:
        if mode == "batch":
            source_sdf = (
                spark.table(silver_table)
                .where(f"event_time >= current_timestamp() - INTERVAL {batch_lookback_hours} HOURS")
                .select(*_SELECT_COLS)
                .dropna(subset=["machine_id", "event_time"])
            )
        else:
            source_sdf = (
                spark.table(silver_table)
                .where(f"event_time >= current_timestamp() - INTERVAL {realtime_lookback_minutes} MINUTES")
                .select(*_SELECT_COLS)
                .dropna(subset=["machine_id", "event_time"])
            )

        row_count = source_sdf.count()
        if row_count == 0:
            continue

        scored_df = (
            source_sdf
            .withColumn("prob_fault_next_5m", predict_udf(_feature_struct()))
            .withColumn("prob_fault_next_5m", F.greatest(F.lit(0.0), F.least(F.lit(1.0), F.col("prob_fault_next_5m"))))
        )
        horizon_cols = _expand_horizons_from_5m(F.col("prob_fault_next_5m"))
        scored_df = (
            scored_df
            .withColumn("prob_fault_next_1h", horizon_cols["1h"])
            .withColumn("prob_fault_next_24h", horizon_cols["24h"])
            .withColumn("prob_fault_next_7d", horizon_cols["7d"])
            .withColumn("predicted_fault_next_5m", F.col("prob_fault_next_5m") >= 0.5)
            .withColumn("predicted_fault_next_1h", F.col("prob_fault_next_1h") >= 0.5)
            .withColumn("predicted_fault_next_24h", F.col("prob_fault_next_24h") >= 0.5)
            .withColumn("predicted_fault_next_7d", F.col("prob_fault_next_7d") >= 0.5)
            .withColumn("inference_type", F.lit(mode))
            .withColumn("model_run_id", F.lit(model_run_id))
            .withColumn("scored_at", F.current_timestamp())
            .select(
                "machine_id",
                "event_time",
                "prob_fault_next_5m",
                "predicted_fault_next_5m",
                "prob_fault_next_1h",
                "predicted_fault_next_1h",
                "prob_fault_next_24h",
                "predicted_fault_next_24h",
                "prob_fault_next_7d",
                "predicted_fault_next_7d",
                "inference_type",
                "model_run_id",
                "scored_at",
            )
        )

        scored_df.write.mode("append").format("delta").saveAsTable(history_table)

        with mlflow.start_run(run_name=f"iot_fault_inference_{mode}", nested=True):
            mlflow.set_tags({"task": "fault_inference", "inference_type": mode})
            machine_count = scored_df.select("machine_id").distinct().count()
            stats = scored_df.agg(
                F.avg("prob_fault_next_5m").alias("mean_prob"),
                F.avg(F.col("predicted_fault_next_5m").cast("double")).alias("high_risk_rate"),
                F.avg("prob_fault_next_24h").alias("mean_prob_24h"),
                F.avg("prob_fault_next_7d").alias("mean_prob_7d"),
            ).first()
            mlflow.log_params(
                {"inference_type": mode, "rows_scored": row_count, "machines_scored_mode": machine_count}
            )
            mlflow.log_metrics(
                {
                    "high_risk_rate": float(stats["high_risk_rate"] or 0),
                    "mean_fault_probability": float(stats["mean_prob"] or 0),
                    "mean_fault_probability_24h": float(stats["mean_prob_24h"] or 0),
                    "mean_fault_probability_7d": float(stats["mean_prob_7d"] or 0),
                }
            )
            print(f"[fault:{mode}] rows_scored={row_count} machines_scored={machine_count}")
        scored_segments.append(mode)

    if not scored_segments:
        mlflow.log_metrics({"rows_scored_total": 0.0, "machines_scored": 0.0})
        print("No rows available for fault inference in selected mode(s); skipping table updates.")
    else:
        w = W.partitionBy("machine_id").orderBy(F.desc("event_time"))
        latest_df = (
            spark.table(history_table)
            .withColumn("_rn", F.row_number().over(w))
            .where("_rn = 1")
            .drop("_rn")
        )
        (
            latest_df.write.mode("overwrite")
            .option("overwriteSchema", "true")
            .format("delta")
            .saveAsTable(output_table)
        )
        total_count = latest_df.count()
        machine_count = latest_df.select("machine_id").distinct().count()
        mlflow.log_metrics(
            {"rows_scored_total": float(total_count), "machines_scored": float(machine_count)}
        )
