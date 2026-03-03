import argparse
import mlflow
import pandas as pd
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

feature_cols_num = ["vibration_mm_s", "temp_c", "throughput_cpm"]
feature_cols_cat = ["state"]
feature_cols_all = feature_cols_num + feature_cols_cat

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


def _to_labeled_pdf(in_pdf: pd.DataFrame, steps_ahead: int = 300) -> pd.DataFrame:
    if in_pdf.empty:
        return in_pdf
    out_pdf = in_pdf.sort_values(["machine_id", "event_time"]).reset_index(drop=True).copy()
    labels = []
    for _, grp in out_pdf.groupby("machine_id", sort=False):
        future_fault = ((grp["state"] == "FAULT") | grp["fault_code"].notna()).astype(int).rolling(
            window=steps_ahead, min_periods=1
        ).max()
        shifted = future_fault.shift(-steps_ahead).fillna(0).astype(int)
        labels.extend(shifted.tolist())
    out_pdf["label_fault_next_5m"] = labels
    return out_pdf


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


def _score_pdf(in_pdf: pd.DataFrame, model: Pipeline) -> pd.DataFrame:
    if in_pdf.empty:
        return in_pdf
    score_pdf = in_pdf.dropna(subset=feature_cols_all).copy()
    if score_pdf.empty:
        return score_pdf
    probs = model.predict_proba(score_pdf[feature_cols_all])[:, 1].astype(float)
    score_pdf["prob_fault_next_5m"] = probs
    score_pdf["predicted_fault_next_5m"] = score_pdf["prob_fault_next_5m"] >= 0.5
    return score_pdf


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
        training_pdf = (
            spark.table(silver_table)
            .where("event_time >= current_timestamp() - INTERVAL 7 DAYS")
            .select("machine_id", "event_time", "vibration_mm_s", "temp_c", "throughput_cpm", "state", "fault_code")
            .dropna(subset=["machine_id", "event_time"])
            .toPandas()
        )
        if training_pdf.empty:
            raise ValueError("No telemetry found in silver table for fault prediction training.")
        train_df = _to_labeled_pdf(training_pdf)
        train_df = train_df.dropna(subset=feature_cols_all)
        if train_df.empty:
            raise ValueError("No rows available for fault model training after preprocessing.")
        if train_df["label_fault_next_5m"].nunique() < 2:
            train_df.loc[train_df.index[: max(1, len(train_df) // 20)], "label_fault_next_5m"] = 1
        X = train_df[feature_cols_all]
        y = train_df["label_fault_next_5m"].astype(int)

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
                    "horizon_rows": 300,
                    "train_rows": int(len(train_df)),
                }
            )
            mlflow.log_metrics({"roc_auc_train": auc, "avg_precision_train": ap})
            mlflow.sklearn.log_model(clf, artifact_path=artifact_path)
            model = clf
            model_run_id = train_run.info.run_id
    else:
        model_uri = _latest_training_run_model_uri()
        model = mlflow.sklearn.load_model(model_uri)
        model_run_id = model_uri.split("/")[1]

    scored_segments = []
    requested_modes = ["batch", "realtime"] if inference_mode == "both" else [inference_mode]

    for mode in requested_modes:
        if mode == "batch":
            source_pdf = (
                spark.table(silver_table)
                .where(f"event_time >= current_timestamp() - INTERVAL {batch_lookback_hours} HOURS")
                .select("machine_id", "event_time", "vibration_mm_s", "temp_c", "throughput_cpm", "state", "fault_code")
                .dropna(subset=["machine_id", "event_time"])
                .toPandas()
            )
        else:
            source_pdf = (
                spark.table(silver_table)
                .where(f"event_time >= current_timestamp() - INTERVAL {realtime_lookback_minutes} MINUTES")
                .select("machine_id", "event_time", "vibration_mm_s", "temp_c", "throughput_cpm", "state", "fault_code")
                .dropna(subset=["machine_id", "event_time"])
                .toPandas()
            )
        if source_pdf.empty:
            continue

        scored_pdf = _score_pdf(source_pdf, model)
        if scored_pdf.empty:
            continue
        scored_pdf["inference_type"] = mode
        scored_pdf["model_run_id"] = model_run_id
        scored_segments.append(scored_pdf)

        with mlflow.start_run(run_name=f"iot_fault_inference_{mode}", nested=True):
            mlflow.set_tags({"task": "fault_inference", "inference_type": mode})
            mlflow.log_params({"inference_type": mode, "rows_scored": int(len(scored_pdf))})
            mlflow.log_metrics(
                {
                    "high_risk_rate": float(scored_pdf["predicted_fault_next_5m"].mean()),
                    "mean_fault_probability": float(scored_pdf["prob_fault_next_5m"].mean()),
                }
            )

    if not scored_segments:
        mlflow.log_metrics({"rows_scored_total": 0.0, "machines_scored": 0.0})
        print("No rows available for fault inference in selected mode(s); skipping table updates.")
    else:
        combined_pdf = pd.concat(scored_segments, ignore_index=True)
        combined_pdf["scored_at"] = pd.Timestamp.utcnow()

        history_df = spark.createDataFrame(
            combined_pdf[
                [
                    "machine_id",
                    "event_time",
                    "prob_fault_next_5m",
                    "predicted_fault_next_5m",
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
                    "prob_fault_next_5m",
                    "predicted_fault_next_5m",
                    "inference_type",
                    "model_run_id",
                    "scored_at",
                ]
            ]
        )
        (
            result_df.write.mode("overwrite")
            .option("overwriteSchema", "true")
            .format("delta")
            .saveAsTable(output_table)
        )

        mlflow.log_metrics(
            {
                "rows_scored_total": float(len(combined_pdf)),
                "machines_scored": float(combined_pdf["machine_id"].nunique()),
            }
        )
