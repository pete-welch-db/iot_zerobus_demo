# Orchestration Reconciliation Inventory

This inventory captures the remote deployed bundle layout versus the local reorganized source-of-truth layout.

## Remote bundle folder from Databricks URL

- Workspace folder id `3745697525442532` resolves to:
  - `/Users/pete.welch@databricks.com/.bundle/iot_zerobus_demo/dev/files`

## Path migration map (old flat -> new functional)

- `databricks/iothub_to_zerobus_bridge.py` -> `databricks/ingestion/iothub_to_zerobus_bridge.py`
- `databricks/lakeflow_zerobus_config.json` -> `databricks/ingestion/lakeflow_zerobus_config.json`
- `databricks/dlt_pipeline.py` -> `databricks/pipelines/dlt_pipeline.py`
- `databricks/ml_anomaly_notebook.py` -> `databricks/ml/ml_anomaly_notebook.py`
- `databricks/ml_state_prediction_notebook.py` -> `databricks/ml/ml_state_prediction_notebook.py`
- `databricks/apply_semantic_views.py` -> `databricks/semantic/apply_semantic_views.py`
- `databricks/create_uc_metric_views.py` -> `databricks/semantic/create_uc_metric_views.py`
- `databricks/sql_views.sql` -> `databricks/semantic/sql_views.sql`
- `databricks/manufacturing_command_center_dashboard.sql` -> `databricks/dashboard/manufacturing_command_center_dashboard.sql`
- `databricks/manufacturing_command_center.lakeview.json` -> `databricks/dashboard/manufacturing_command_center.lvdash.json`
- `databricks/refresh_dashboard_after_dlt.py` -> `databricks/dashboard/refresh_dashboard_after_dlt.py`
- `databricks/dashboard_notes.md` -> `databricks/dashboard/dashboard_notes.md`
- `databricks/deploy_genie_space.py` -> `databricks/genie/deploy_genie_space.py`
- `databricks/genie_benchmarks.md` -> `databricks/genie/genie_benchmarks.md`
- `databricks/genie_manufacturing_command_center.md` -> `databricks/genie/genie_manufacturing_command_center.md`
- `databricks/genie_space_notes.md` -> `databricks/genie/genie_space_notes.md`
- `databricks/lakebase_oltp_mirror.py` -> `databricks/lakebase/lakebase_oltp_mirror.py`
- `databricks/lakebase_parity_validation.py` -> `databricks/lakebase/lakebase_parity_validation.py`
- `databricks/preflight_checks.py` -> `databricks/ops/preflight_checks.py`
- `databricks/validate_demo_outputs.py` -> `databricks/ops/validate_demo_outputs.py`
- `databricks/latency_proof_realtime_ms_notebook.py` -> `databricks/ops/latency_proof_realtime_ms_notebook.py`

## Remote-only stale artifacts (cleanup candidates)

These files are present in the remote bundle workspace but do not belong to the current local structure:

- `databricks/COMPLETE_ORCHESTRATION_ARCHITECTURE.md`
- `databricks/job4_lakebase_mirror_event_driven.yml`
- `databricks/alert_nonrun_to_slack.py` (feature archived locally)

## Notes

- The remote workspace currently still reflects the old flat layout because it has not yet been refreshed with a full deploy from the reorganized source tree.
- Cleanup in Databricks workspace should be done after deploy and successful runtime verification.
