import argparse
import mlflow
import numpy as np
import pandas as pd
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

feature_cols = ["vibration_mm_s", "temp_c", "throughput_cpm"]
artifact_path = "anomaly_pipeline_model"
optional_feature_defaults = {
    "load_pct": 0.0,
    "rpm": 0.0,
    "humidity_rh": 50.0,
}


def _training_pdf() -> pd.DataFrame:
    existing_cols = set(spark.table(silver_table).columns)
    select_cols = ["machine_id", "event_time", "vibration_mm_s", "temp_c", "throughput_cpm", "state"]
    for col_name in optional_feature_defaults:
        if col_name in existing_cols:
            select_cols.append(col_name)
    source_df = (
        spark.table(silver_table)
        .where("event_time >= current_timestamp() - INTERVAL 2 DAYS")
        .select(*select_cols)
        .dropna()
    )
    return source_df.toPandas()


def _score_pdf(in_pdf: pd.DataFrame, model: Pipeline) -> pd.DataFrame:
    if in_pdf.empty:
        return in_pdf
    score_pdf = in_pdf.copy()
    for col_name, default_value in optional_feature_defaults.items():
        if col_name not in score_pdf.columns:
            score_pdf[col_name] = default_value
    X = score_pdf[feature_cols].astype(float).values
    decision = model.decision_function(X)
    model_score = 1.0 / (1.0 + np.exp(4.0 * decision))
    rule_flag = (
        (score_pdf["vibration_mm_s"] > 9.5)
        | (score_pdf["temp_c"] > 85.0)
        | ((score_pdf["state"] == "RUN") & (score_pdf["throughput_cpm"] < 15))
    ).astype(float)
    blended_score = np.clip((model_score * 0.7) + (rule_flag * 0.3), 0.0, 1.0)
    score_pdf["anomaly_score"] = blended_score
    score_pdf["is_anomaly"] = blended_score >= 0.70
    return score_pdf


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


def _ensure_output_tables() -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {history_table} (
          machine_id STRING,
          event_time TIMESTAMP,
          anomaly_score DOUBLE,
          is_anomaly BOOLEAN,
          inference_type STRING,
          model_run_id STRING,
          scored_at TIMESTAMP
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {output_table} (
          machine_id STRING,
          event_time TIMESTAMP,
          anomaly_score DOUBLE,
          is_anomaly BOOLEAN,
          inference_type STRING,
          model_run_id STRING,
          scored_at TIMESTAMP
        )
        """
    )


def _merge_latest_scores(result_df) -> None:
    result_df.createOrReplaceTempView("tmp_ml_anomaly_scores_latest")
    spark.sql(
        f"""
        MERGE INTO {output_table} AS tgt
        USING tmp_ml_anomaly_scores_latest AS src
          ON tgt.machine_id = src.machine_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


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
            train_scored_pdf = _score_pdf(train_pdf, model)
            anomaly_rate = float(train_scored_pdf["is_anomaly"].mean())
            mlflow.log_params(
                {
                    "feature_cols": ",".join(feature_cols),
                    "contamination": 0.05,
                    "n_estimators": 150,
                    "train_rows": int(len(train_pdf)),
                }
            )
            mlflow.log_metric("anomaly_rate_train", anomaly_rate)
            mlflow.sklearn.log_model(model, artifact_path=artifact_path)
            model_run_id = train_run.info.run_id
    else:
        model_uri = _latest_training_run_model_uri()
        model = mlflow.sklearn.load_model(model_uri)
        model_run_id = model_uri.split("/")[1]

    scored_segments = []
    _ensure_output_tables()
    requested_modes = ["batch", "realtime"] if inference_mode == "both" else [inference_mode]

    for mode in requested_modes:
        if mode == "batch":
            existing_cols = set(spark.table(silver_table).columns)
            select_cols = ["machine_id", "event_time", "vibration_mm_s", "temp_c", "throughput_cpm", "state"]
            for col_name in optional_feature_defaults:
                if col_name in existing_cols:
                    select_cols.append(col_name)
            score_df = (
                spark.table(silver_table)
                .where(f"event_time >= current_timestamp() - INTERVAL {batch_lookback_hours} HOURS")
                .select(*select_cols)
                .dropna()
            )
        else:
            existing_cols = set(spark.table(silver_table).columns)
            select_cols = ["machine_id", "event_time", "vibration_mm_s", "temp_c", "throughput_cpm", "state"]
            for col_name in optional_feature_defaults:
                if col_name in existing_cols:
                    select_cols.append(col_name)
            score_df = (
                spark.table(silver_table)
                .where(f"event_time >= current_timestamp() - INTERVAL {realtime_lookback_minutes} MINUTES")
                .select(*select_cols)
                .dropna()
            )

        score_pdf = score_df.toPandas()
        if score_pdf.empty:
            continue

        scored_pdf = _score_pdf(score_pdf, model)
        scored_pdf["inference_type"] = mode
        scored_pdf["model_run_id"] = model_run_id
        scored_segments.append(scored_pdf)

        with mlflow.start_run(run_name=f"iot_anomaly_inference_{mode}", nested=True):
            mlflow.set_tags({"task": "anomaly_inference", "inference_type": mode})
            mlflow.log_params(
                {
                    "inference_type": mode,
                    "rows_scored": int(len(scored_pdf)),
                }
            )
            mlflow.log_metrics(
                {
                    "anomaly_rate": float(scored_pdf["is_anomaly"].mean()),
                    "mean_anomaly_score": float(scored_pdf["anomaly_score"].mean()),
                }
            )

    if not scored_segments:
        mlflow.log_metrics({"rows_scored_total": 0.0, "machines_scored": 0.0})
        print("No rows available for anomaly inference in selected mode(s); skipping table updates.")
    else:
        combined_pdf = pd.concat(scored_segments, ignore_index=True)
        combined_pdf["scored_at"] = pd.Timestamp.utcnow()

        history_df = spark.createDataFrame(
            combined_pdf[
                [
                    "machine_id",
                    "event_time",
                    "anomaly_score",
                    "is_anomaly",
                    "inference_type",
                    "model_run_id",
                    "scored_at",
                ]
            ]
        )
        history_df.write.mode("append").format("delta").saveAsTable(history_table)

        latest_pdf = (
            combined_pdf.sort_values(["machine_id", "event_time"])
            .groupby("machine_id", as_index=False)
            .tail(1)
        )
        result_df = spark.createDataFrame(
            latest_pdf[
                [
                    "machine_id",
                    "event_time",
                    "anomaly_score",
                    "is_anomaly",
                    "inference_type",
                    "model_run_id",
                    "scored_at",
                ]
            ]
        )
        _merge_latest_scores(result_df)

        mlflow.log_metrics(
            {
                "rows_scored_total": float(len(combined_pdf)),
                "machines_scored": float(combined_pdf["machine_id"].nunique()),
            }
        )
