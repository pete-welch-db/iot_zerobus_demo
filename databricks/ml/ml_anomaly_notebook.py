import argparse
import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import functions as F, Window
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and score anomaly model for IoT demo.")
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
output_table = f"{catalog}.{schema}.ml_anomaly_scores"
history_table = f"{catalog}.{schema}.ml_anomaly_scores_history"
experiment_name = args.mlflow_experiment
train_model = args.train_model.lower() == "true"
inference_mode = args.inference_mode
batch_lookback_hours = args.batch_lookback_hours
realtime_lookback_minutes = args.realtime_lookback_minutes

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
mlflow.set_experiment(experiment_name)

feature_cols = ["vibration_mm_s", "temp_c", "throughput_cpm", "rpm", "current_amps", "humidity_pct"]
artifact_path = "anomaly_pipeline_model"

_SELECT_COLS = ["machine_id", "event_time"] + feature_cols + ["state"]


def _feature_struct():
    # Keep feature names stable when invoking MLflow pyfunc model.
    return F.struct(*[F.col(c).alias(c) for c in feature_cols])


def _training_pdf() -> pd.DataFrame:
    source_df = (
        spark.table(silver_table)
        .where("event_time >= current_timestamp() - INTERVAL 2 DAYS")
        .select(*_SELECT_COLS)
        .dropna()
    )
    return source_df.toPandas()


def _latest_training_run_model_uri() -> str:
    exp = mlflow.get_experiment_by_name(experiment_name)
    if exp is None:
        raise ValueError(f"MLflow experiment not found: {experiment_name}")
    runs = mlflow.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string="tags.task = 'anomaly_training'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    if runs.empty:
        raise ValueError("No prior anomaly training run found. Run once with --train-model true.")
    run_id = runs.iloc[0]["run_id"]
    return f"runs:/{run_id}/{artifact_path}"


def _last_scored_ts() -> str:
    try:
        row = spark.table(history_table).select(F.max("scored_at").alias("m")).first()
        if row and row["m"]:
            return row["m"].isoformat()
    except Exception:
        pass
    return None


with mlflow.start_run(run_name=f"iot_anomaly_pipeline_{inference_mode}"):
    mlflow.set_tags(
        {
            "pipeline": "iot_zerobus_demo",
            "use_case": "anomaly_detection",
            "task": "anomaly_pipeline",
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
        train_pdf = _training_pdf()
        if train_pdf.empty:
            raise ValueError("No telemetry found in silver table for anomaly training.")
        X_train = train_pdf[feature_cols].astype(float).values
        model = Pipeline(
            steps=[
                ("scaler", StandardScaler()),
                (
                    "model",
                    IsolationForest(
                        n_estimators=150,
                        contamination=0.05,
                        random_state=42,
                    ),
                ),
            ]
        )
        with mlflow.start_run(run_name="iot_anomaly_training", nested=True) as train_run:
            mlflow.set_tags({"task": "anomaly_training"})
            model.fit(X_train)
            mlflow.log_params(
                {
                    "feature_cols": ",".join(feature_cols),
                    "contamination": 0.05,
                    "n_estimators": 150,
                    "train_rows": int(len(train_pdf)),
                }
            )
            mlflow.sklearn.log_model(model, artifact_path=artifact_path)
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
            score_df = (
                spark.table(silver_table)
                .where(f"event_time >= current_timestamp() - INTERVAL {batch_lookback_hours} HOURS")
                .select(*_SELECT_COLS)
                .dropna()
            )
        else:
            score_df = (
                spark.table(silver_table)
                .where(f"event_time >= current_timestamp() - INTERVAL {realtime_lookback_minutes} MINUTES")
                .select(*_SELECT_COLS)
                .dropna()
            )

        row_count = score_df.count()
        if row_count == 0:
            continue

        scored_df = (
            score_df
            .withColumn("raw_decision", predict_udf(_feature_struct()))
            .withColumn("model_score", F.lit(1.0) / (F.lit(1.0) + F.exp(F.lit(4.0) * F.col("raw_decision"))))
            .withColumn(
                "rule_flag",
                F.when(
                    (F.col("vibration_mm_s") > 9.5)
                    | (F.col("temp_c") > 85.0)
                    | (F.col("current_amps") > 12.0)
                    | ((F.col("state") == "RUN") & (F.col("throughput_cpm") < 15)),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "anomaly_score",
                F.greatest(F.lit(0.0), F.least(F.lit(1.0), F.col("model_score") * 0.7 + F.col("rule_flag") * 0.3)),
            )
            .withColumn("is_anomaly", F.col("anomaly_score") >= 0.70)
            .withColumn("inference_type", F.lit(mode))
            .withColumn("model_run_id", F.lit(model_run_id))
            .withColumn("scored_at", F.current_timestamp())
            .select(
                "machine_id", "event_time", "anomaly_score", "is_anomaly",
                "inference_type", "model_run_id", "scored_at",
            )
        )

        scored_df.write.mode("append").format("delta").saveAsTable(history_table)

        with mlflow.start_run(run_name=f"iot_anomaly_inference_{mode}", nested=True):
            mlflow.set_tags({"task": "anomaly_inference", "inference_type": mode})
            machine_count = scored_df.select("machine_id").distinct().count()
            stats = scored_df.agg(
                F.avg("anomaly_score").alias("mean_score"),
                F.avg(F.col("is_anomaly").cast("double")).alias("anomaly_rate"),
            ).first()
            mlflow.log_params(
                {"inference_type": mode, "rows_scored": row_count, "machines_scored_mode": machine_count}
            )
            mlflow.log_metrics(
                {
                    "anomaly_rate": float(stats["anomaly_rate"] or 0),
                    "mean_anomaly_score": float(stats["mean_score"] or 0),
                }
            )
            print(f"[anomaly:{mode}] rows_scored={row_count} machines_scored={machine_count}")
        scored_segments.append(mode)

    if not scored_segments:
        mlflow.log_metrics({"rows_scored_total": 0.0, "machines_scored": 0.0})
        print("No rows available for anomaly inference in selected mode(s); skipping table updates.")
    else:
        w = Window.partitionBy("machine_id").orderBy(F.desc("event_time"))
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
