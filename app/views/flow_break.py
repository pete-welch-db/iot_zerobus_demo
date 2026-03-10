import pandas as pd
import plotly.express as px
import streamlit as st

from views import freshness


def _fmt_ts(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "--"
    try:
        ts = pd.Timestamp(val)
        return ts.strftime("%-m/%-d %I:%M:%S %p") if not pd.isna(ts) else "--"
    except Exception:
        return str(val)


def _risk_band(prob: float) -> str:
    if prob >= 0.8:
        return "CRITICAL"
    if prob >= 0.5:
        return "WATCH"
    return "NORMAL"


def render() -> None:
    freshness.render_freshness_bar()
    clients = st.session_state.clients
    st.subheader("Flow Break Risk Command Center")
    try:
        df = clients.query_flow_break_signals()
    except Exception as exc:
        st.error(f"Failed to query flow-break signals: {exc}")
        return

    if df.empty:
        st.info("No data returned.")
        return

    df["risk_band"] = df["prob_fault_next_5m"].fillna(0.0).apply(_risk_band)
    critical = int((df["risk_band"] == "CRITICAL").sum())
    watch = int((df["risk_band"] == "WATCH").sum())
    normal = int((df["risk_band"] == "NORMAL").sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Critical Machines", critical)
    c2.metric("Watch Machines", watch)
    c3.metric("Normal Machines", normal)
    c4.metric("Avg Risk", f"{df['prob_fault_next_5m'].fillna(0.0).mean():.3f}")

    chart_df = df.sort_values("prob_fault_next_5m", ascending=False).head(25)
    fig = px.bar(
        chart_df,
        x="machine_id",
        y="prob_fault_next_5m",
        color="risk_band",
        color_discrete_map={"CRITICAL": "#E53E3E", "WATCH": "#FF7033", "NORMAL": "#38A169"},
        title="Top Flow-Break Risk Machines",
        labels={"prob_fault_next_5m": "Risk of flow break in next 5m"},
    )
    st.plotly_chart(fig, use_container_width=True)

    display = df[
        [
            "machine_id",
            "state",
            "prob_fault_next_5m",
            "anomaly_score",
            "throughput_cpm",
            "vibration_mm_s",
            "temp_c",
            "current_amps",
            "humidity_pct",
            "telemetry_lag_ms",
            "last_event_time",
            "risk_band",
        ]
    ].copy()
    if "last_event_time" in display.columns:
        display["last_event_time"] = display["last_event_time"].apply(_fmt_ts)
    st.dataframe(display, use_container_width=True, hide_index=True)
