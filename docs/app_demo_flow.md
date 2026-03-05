# IoT App Demo Flow (20 Minutes)

## Objective

Run an app-first demo centered on **Flow Break Prediction** using live IoT telemetry, UC metric views, Genie Q&A, and Lakebase operational reads.
This document is the presenter runbook source of truth; in-app sidebar notes are intentionally minimal.

## Recommended Sequence

1. **Landing narrative (3 minutes)**
   - Open the Streamlit app.
   - Explain the manufacturing problem: delayed detection of line-flow disruptions.
   - Highlight why Databricks + Zerobus: low-latency ingestion, governed metrics, AI + app integration.

2. **Embedded AI/BI dashboard (4 minutes)**
   - Show the embedded Manufacturing Command Center.
   - Anchor baseline KPIs: OEE, anomaly score, telemetry freshness, ML freshness.
   - Use this view as “operations context.”

3. **Flow Break Risk Command Center (5 minutes)**
   - Switch to app-native risk panel.
   - Walk through top-risk machines (`prob_fault_next_5m`) and sensor context.
   - Show risk bands (NORMAL/WATCH/CRITICAL) and lag/freshness fields.

4. **Genie in-app investigation (4 minutes)**
   - Ask: “Which machine has the highest flow-break risk right now and why?”
   - Follow up with a change-over-time question for the top machine.
   - Demonstrate natural-language triage without leaving the app.

5. **Lakebase live operational view (2 minutes)**
   - Show mirrored `machine_current_status` rows from Lakebase.
   - Confirm operational read path is live and synchronized.

6. **Backend proof jump (2 minutes)**
   - Briefly show Databricks jobs/pipeline state for continuous ingest + scoring.
   - Return to app to reinforce end-user operational workflow.

## Backup / Recovery Tips

- If dashboard embed is blocked by browser policy, use the in-app fallback link.
- If Genie space id is not configured in app env, use dashboard + risk tables for the rest of the demo.
- If Lakebase credentials are missing, continue with UC metric and semantic views only.

## Talk Track Anchors

- “One platform from signal to decision.”
- “The same governed metrics power AI/BI, Genie, and the app.”
- “Flow-break risk is operationalized, not just reported.”
