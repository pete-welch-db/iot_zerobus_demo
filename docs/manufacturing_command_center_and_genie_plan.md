# Manufacturing Command Center + Genie Plan

## Goal
Deliver a demo-ready, near-real-time analytics experience in Databricks with:
- An AI/BI dashboard: **Manufacturing Command Center**
- A Genie Space for natural-language exploration on curated metrics
- A single automation workflow that keeps ingest, medallion, ML outputs, and semantic views current

This is a planning document only (no implementation in this step).

## Scope
- Build dashboard and Genie on top of curated views, not raw tables.
- Use existing medallion and ML outputs in `welch.iot_demo_dev`.
- Add orchestration so demo operator runs one workflow command.

## Existing Assets To Reuse
- Pipeline: `iot_telemetry_medallion_${bundle.target}`
- Jobs:
  - `zerobus_connector_setup_${bundle.target}`
  - `iothub_to_zerobus_bridge_${bundle.target}`
  - `iot_pipeline_refresh_${bundle.target}`
  - `iot_ml_scoring_manual_${bundle.target}`
- SQL/view templates:
  - `databricks/semantic/sql_views.sql`
  - `databricks/dashboard/dashboard_notes.md`
  - `databricks/genie/genie_space_notes.md`

## Phase 1: Semantic Layer Hardening
1. Validate and update SQL views in `databricks/semantic/sql_views.sql` to use `welch.iot_demo_dev` consistently.
2. Add/confirm a `current_status` style view to expose latest KPI/risk snapshot per machine.
3. Add freshness fields (`last_event_time`, `last_ml_score_time`) for dashboard/Genie trust signals.
4. Ensure KPI definitions are explicit and stable for both dashboard and Genie.

## Phase 2: Manufacturing Command Center Dashboard
1. Create AI/BI dashboard pages:
   - **Live Operations**: temp, vibration, throughput, machine state over time
   - **Health + Risk**: anomaly score, fault probability, OEE cards
   - **Loss Analysis**: run/stopped/fault time breakdown, downtime trends
   - **Machine Ranking**: top at-risk machines, worst OEE machines
2. Add filters:
   - Time window
   - Machine/line
   - State
3. Add dashboard-level KPI cards:
   - Availability, Performance, Quality, OEE
   - Active anomaly count
   - High-risk machine count
4. Validate each tile query against serverless SQL warehouse before publishing.

## Phase 3: Genie Space Setup
1. Create Genie Space bound to the same warehouse and schema.
2. Include only curated objects:
   - `vw_machine_telemetry_live`
   - `vw_machine_health`
   - `dim_machine`
   - (optional) `vw_machine_current_status`
3. Add Genie instructions:
   - Map business terms (`downtime`, `risk`, `OEE`, `fault likelihood`) to canonical columns.
   - Default to latest window unless user specifies timeframe.
   - Prefer trusted curated views; avoid raw/bronze tables.
4. Add prompt test suite:
   - “Which machine is most likely to fault next?”
   - “Show last hour downtime by line.”
   - “What changed in OEE over the past 30 minutes?”

## Phase 4: One-Click Demo Automation
1. Add a top-level orchestrator job:
   - Preflight checks (secrets/data freshness)
   - Zerobus setup
   - IoT Hub -> Zerobus bridge run
   - DLT refresh
   - ML scoring
   - Post-run validation (row counts + freshness thresholds)
2. Add run modes:
   - `demo_realtime` (fast cadence)
   - `demo_recovery` (checkpoint reset + backfill path)
3. Add a concise runbook in `README.md`:
   - Deploy command
   - Single run command
   - Verification query snippets

## Phase 5: Demo Validation and Sign-Off
1. End-to-end validation checklist:
   - Azure IoT events visible
   - Raw/Bronze/Silver growth observed
   - Gold + ML outputs update within demo SLA
   - Dashboard tiles render with fresh timestamps
   - Genie answers align with dashboard numbers
2. Define fallback actions for each failure point (bridge, DLT, ML, dashboard/Genie query).

## Data Quality and SLA Targets (Demo)
- Data arrival to `raw_iothub_messages`: < 1-2 minutes
- Silver visibility after pipeline refresh: < 2-4 minutes
- ML refresh after scoring run: < 5 minutes
- Dashboard/Genie freshness indicator always shown

## Deliverables
- Updated semantic SQL definitions
- Published AI/BI dashboard: **Manufacturing Command Center**
- Published Genie Space with curated instructions and prompt pack
- Orchestrator workflow job + demo runbook

## Implementation Order Recommendation
1. Semantic layer (views)
2. Dashboard
3. Genie
4. Orchestration
5. Full rehearsal and hardening

## References (Official Skills/Patterns)
- Databricks Skills catalog (source of implementation patterns):  
  https://github.com/databricks-solutions/ai-dev-kit/tree/main/databricks-skills
- Particularly relevant skill categories to follow during implementation:
  - `databricks-aibi-dashboards`
  - `databricks-genie`
  - `databricks-jobs`
  - `databricks-spark-declarative-pipelines`
  - `databricks-asset-bundles`
