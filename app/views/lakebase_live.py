"""Machine Explorer — interactive Lakebase-backed device dashboard."""
from __future__ import annotations

import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data_access import DataClients
from views import freshness


@st.cache_data(ttl=5, show_spinner=False)
def _fetch_machines(_clients) -> pd.DataFrame:
    return _clients.query_lakebase_machines()


@st.cache_data(ttl=15, show_spinner=False)
def _fetch_status_summary(_clients) -> pd.DataFrame:
    return _clients.query_lakebase_status()


@st.cache_data(ttl=10, show_spinner=False)
def _fetch_latency_stats(_clients, machine_ids, states, line_names, minutes: int) -> pd.DataFrame:
    return _clients.query_latency_stats(
        machine_ids=list(machine_ids) if machine_ids else None,
        states=list(states) if states else None,
        line_names=list(line_names) if line_names else None,
        minutes=minutes,
    )


def _fmt_ts(val) -> str:
    """Format a timestamp value for compact display (America/Detroit already applied upstream)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "--"
    try:
        ts = pd.Timestamp(val)
        if pd.isna(ts):
            return "--"
        return ts.strftime("%-m/%-d %I:%M:%S %p")
    except Exception:
        return str(val)

# ── Threshold constants ───────────────────────────────────────────────
_STATE_COLORS = {"RUN": "#38A169", "STOPPED": "#DD6B20", "FAULT": "#E53E3E"}
_STATE_ICONS = {"RUN": "check_circle", "STOPPED": "pause_circle", "FAULT": "error"}


def _risk_band(prob: float) -> str:
    if prob >= 0.8:
        return "CRITICAL"
    if prob >= 0.5:
        return "WATCH"
    return "NORMAL"


_RISK_COLORS = {"CRITICAL": "#E53E3E", "WATCH": "#DD6B20", "NORMAL": "#38A169"}


def _gauge(value, title, min_val, max_val, thresholds, unit="", height=140):
    """Build a compact Plotly indicator gauge."""
    green_hi, amber_hi = thresholds
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value if value is not None else 0,
        number={"suffix": f" {unit}" if unit else "", "font": {"size": 18}},
        title={"text": title, "font": {"size": 12}},
        gauge={
            "axis": {"range": [min_val, max_val], "tickfont": {"size": 9}},
            "bar": {"color": "#2D3748", "thickness": 0.3},
            "steps": [
                {"range": [min_val, green_hi], "color": "rgba(56,161,105,0.18)"},
                {"range": [green_hi, amber_hi], "color": "rgba(221,107,32,0.18)"},
                {"range": [amber_hi, max_val], "color": "rgba(229,62,62,0.18)"},
            ],
            "threshold": {
                "line": {"color": "#E53E3E", "width": 2},
                "thickness": 0.8,
                "value": amber_hi,
            },
        },
    ))
    fig.update_layout(
        height=height,
        margin={"t": 30, "b": 5, "l": 20, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def _oee_gauge(value, height=140):
    """OEE gauge with inverted thresholds (higher is better)."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value if value is not None else 0,
        number={"suffix": "%", "font": {"size": 18}},
        title={"text": "OEE", "font": {"size": 12}},
        gauge={
            "axis": {"range": [0, 100], "tickfont": {"size": 9}},
            "bar": {"color": "#2D3748", "thickness": 0.3},
            "steps": [
                {"range": [0, 50], "color": "rgba(229,62,62,0.18)"},
                {"range": [50, 80], "color": "rgba(221,107,32,0.18)"},
                {"range": [80, 100], "color": "rgba(56,161,105,0.18)"},
            ],
            "threshold": {
                "line": {"color": "#38A169", "width": 2},
                "thickness": 0.8,
                "value": 80,
            },
        },
    ))
    fig.update_layout(
        height=height,
        margin={"t": 30, "b": 5, "l": 20, "r": 20},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def _state_badge_html(state: str) -> str:
    color = _STATE_COLORS.get(state, "#718096")
    return (
        f'<span style="display:inline-block;padding:2px 10px;border-radius:12px;'
        f'font-size:0.72rem;font-weight:700;letter-spacing:0.5px;'
        f'color:#fff;background:{color};">{state}</span>'
    )


def _risk_badge_html(band: str) -> str:
    color = _RISK_COLORS.get(band, "#718096")
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:12px;'
        f'font-size:0.7rem;font-weight:600;color:#fff;background:{color};">'
        f'{band}</span>'
    )


def _fmt(val, decimals=1, fallback="--"):
    if val is None:
        return fallback
    return f"{float(val):,.{decimals}f}"


_CARD_CSS = """
<style>
.mx-card {
    border: 1px solid #e8eaed;
    border-radius: 12px;
    padding: 1rem 1.1rem 0.8rem;
    margin-bottom: 0.8rem;
    background: #fff;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    border-top: 4px solid var(--border-color);
}
.mx-card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 0.5rem;
}
.mx-card-title {
    font-size: 1.05rem;
    font-weight: 700;
    color: #1a202c;
}
.mx-card-subtitle {
    font-size: 0.78rem;
    color: #718096;
}
.mx-metrics-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-top: 0.4rem;
}
.mx-mini-metric {
    font-size: 0.73rem;
    color: #4a5568;
    background: #f7f8fa;
    border-radius: 6px;
    padding: 3px 8px;
}
.mx-mini-metric b {
    color: #1a202c;
}
.mx-kpi-card {
    border: 1px solid #e8eaed;
    border-radius: 10px;
    padding: 0.7rem 1rem;
    background: #fff;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    text-align: center;
}
.mx-kpi-value {
    font-size: 1.5rem;
    font-weight: 800;
    color: #1a202c;
}
.mx-kpi-label {
    font-size: 0.75rem;
    color: #718096;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
</style>
"""


def _render_machine_card(row):
    """Render a single machine card with gauges and metrics."""
    state = str(row.get("state", "UNKNOWN"))
    border_color = _STATE_COLORS.get(state, "#718096")
    machine_id = row.get("machine_id", "?")
    line_name = row.get("line_name") or ""
    prob = float(row.get("prob_fault_next_5m") or 0)
    band = _risk_band(prob)

    st.markdown(
        f'<div class="mx-card" style="--border-color:{border_color};">'
        f'<div class="mx-card-header">'
        f'<div><span class="mx-card-title">{machine_id}</span>'
        f'<br><span class="mx-card-subtitle">{line_name}</span></div>'
        f'<div>{_state_badge_html(state)} {_risk_badge_html(band)}</div>'
        f'</div></div>',
        unsafe_allow_html=True,
    )

    g1, g2, g3, g4 = st.columns(4)
    with g1:
        st.plotly_chart(
            _gauge(row.get("vibration_mm_s"), "Vibration", 0, 15, (8, 9.5), "mm/s"),
            use_container_width=True, key=f"vib_{machine_id}",
        )
    with g2:
        st.plotly_chart(
            _gauge(row.get("temp_c"), "Temp", 0, 120, (75, 85), "°C"),
            use_container_width=True, key=f"tmp_{machine_id}",
        )
    with g3:
        st.plotly_chart(
            _gauge(row.get("current_amps"), "Current", 0, 16, (10, 12), "A"),
            use_container_width=True, key=f"amp_{machine_id}",
        )
    with g4:
        st.plotly_chart(
            _oee_gauge(row.get("oee_pct")),
            use_container_width=True, key=f"oee_{machine_id}",
        )

    st.markdown(
        '<div class="mx-metrics-row">'
        f'<span class="mx-mini-metric">RPM <b>{_fmt(row.get("rpm"), 0)}</b></span>'
        f'<span class="mx-mini-metric">Throughput <b>{_fmt(row.get("throughput_cpm"), 0)} cpm</b></span>'
        f'<span class="mx-mini-metric">Humidity <b>{_fmt(row.get("humidity_pct"), 0)}%</b></span>'
        f'<span class="mx-mini-metric">Power <b>{_fmt(row.get("power_kw"), 2)} kW</b></span>'
        f'<span class="mx-mini-metric">Pressure <b>{_fmt(row.get("pressure_bar"), 1)} bar</b></span>'
        f'<span class="mx-mini-metric">Flow <b>{_fmt(row.get("flow_rate_lpm"), 1)} L/m</b></span>'
        f'<span class="mx-mini-metric">Anomaly <b>{_fmt(row.get("anomaly_score"), 3)}</b></span>'
        f'<span class="mx-mini-metric">Fault Risk <b>{_fmt(prob, 3)}</b></span>'
        f'<span class="mx-mini-metric">Last Event <b>{_fmt_ts(row.get("last_event_time"))}</b></span>'
        '</div>',
        unsafe_allow_html=True,
    )


# ── Public API ────────────────────────────────────────────────────────

def render_summary(clients: DataClients) -> None:
    """Compact summary for embedding on the Metrics page."""
    st.markdown("#### Lakebase Live Snapshot")
    if not clients.lakebase_available():
        st.info("Lakebase is not configured for this runtime.")
        return
    try:
        df = _fetch_status_summary(clients)
    except Exception as exc:
        st.warning(f"Lakebase summary unavailable: {exc}")
        return
    if df.empty:
        st.warning("Lakebase summary unavailable: no rows returned.")
        return
    row_count = len(df)
    machine_count = (
        int(df["machine_id"].nunique()) if "machine_id" in df.columns else 0
    )
    avg_risk = (
        float(df["prob_fault_next_5m"].astype(float).mean())
        if "prob_fault_next_5m" in df.columns else None
    )
    latest = _fmt_ts(df["last_event_time"].max()) if "last_event_time" in df.columns else "n/a"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rows", row_count)
    c2.metric("Machines", machine_count)
    c3.metric("Avg Risk", f"{avg_risk:.3f}" if avg_risk is not None else "n/a")
    c4.metric("Latest Update", latest)


def render(clients: DataClients | None = None) -> None:
    """Main Machine Explorer page."""
    freshness.render_freshness_bar()
    st.markdown(_CARD_CSS, unsafe_allow_html=True)

    if clients is None:
        clients = st.session_state.clients

    # ── Header with auto-refresh toggle ─────────────────────────────
    h1, h2 = st.columns([4, 1])
    with h1:
        st.markdown(
            '<h2 style="margin-bottom:0;">Machine Explorer</h2>'
            '<p style="color:#718096;margin-top:2px;">Real-time device status powered by Lakebase OLTP</p>',
            unsafe_allow_html=True,
        )
    with h2:
        auto_refresh = st.toggle("Live refresh", value=st.session_state.get("mx_auto_refresh", False), key="mx_auto_refresh")
        if auto_refresh:
            interval = st.select_slider(
                "Interval",
                options=[5, 10, 15, 30, 60],
                value=st.session_state.get("mx_interval", 15),
                key="mx_interval",
                format_func=lambda x: f"{x}s",
            )

    if not clients.lakebase_available():
        st.info(
            "Lakebase credentials not configured. "
            "Set LAKEBASE_DB_HOST / PORT / NAME / USER and LAKEBASE_INSTANCE_NAME in your .env file."
        )
        return

    try:
        df = _fetch_machines(clients)
    except Exception as exc:
        st.error(f"Failed to query Lakebase: {exc}")
        return

    if df.empty:
        st.warning("No machine data in Lakebase yet. Run the Lakebase mirror job first.")
        return

    for col in ["oee_pct", "prob_fault_next_5m", "anomaly_score", "vibration_mm_s",
                "temp_c", "current_amps", "telemetry_lag_ms"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: float(v) if v is not None else None)

    df["risk_band"] = df["prob_fault_next_5m"].fillna(0.0).apply(_risk_band)
    if "line_name" not in df.columns:
        df["line_name"] = ""
    df["line_name"] = df["line_name"].fillna("")

    # ── Sidebar filters ───────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### Filters")

        all_states = sorted(df["state"].dropna().unique().tolist())
        sel_states = st.multiselect("Status", all_states, default=all_states, key="mx_state")

        all_lines = sorted([l for l in df["line_name"].unique().tolist() if l])
        sel_lines = st.multiselect("Production Line", all_lines, default=[], key="mx_line")

        all_ids = sorted(df["machine_id"].unique().tolist())
        sel_ids = st.multiselect("Machine ID", all_ids, default=[], key="mx_id")

    # Apply filters
    filtered = df.copy()
    if sel_states:
        filtered = filtered[filtered["state"].isin(sel_states)]
    if sel_lines:
        filtered = filtered[filtered["line_name"].isin(sel_lines)]
    if sel_ids:
        filtered = filtered[filtered["machine_id"].isin(sel_ids)]

    total = len(df)
    shown = len(filtered)
    running = int((filtered["state"] == "RUN").sum())
    stopped = int((filtered["state"] == "STOPPED").sum())
    faulted = int((filtered["state"] == "FAULT").sum())
    avg_oee = filtered["oee_pct"].mean() if not filtered.empty else None
    avg_risk = filtered["prob_fault_next_5m"].mean() if not filtered.empty else None

    # ── KPI row ───────────────────────────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.markdown(
            '<div class="mx-kpi-card">'
            f'<div class="mx-kpi-value">{shown}<span style="font-size:0.8rem;color:#718096;font-weight:400;">/{total}</span></div>'
            '<div class="mx-kpi-label">Machines</div></div>',
            unsafe_allow_html=True,
        )
    with k2:
        st.markdown(
            '<div class="mx-kpi-card">'
            f'<div class="mx-kpi-value" style="color:#38A169;">{running}</div>'
            '<div class="mx-kpi-label">Running</div></div>',
            unsafe_allow_html=True,
        )
    with k3:
        st.markdown(
            '<div class="mx-kpi-card">'
            f'<div class="mx-kpi-value" style="color:#DD6B20;">{stopped}</div>'
            '<div class="mx-kpi-label">Stopped</div></div>',
            unsafe_allow_html=True,
        )
    with k4:
        oee_color = "#38A169" if avg_oee and avg_oee >= 80 else "#DD6B20" if avg_oee and avg_oee >= 50 else "#E53E3E"
        st.markdown(
            '<div class="mx-kpi-card">'
            f'<div class="mx-kpi-value" style="color:{oee_color};">{_fmt(avg_oee, 1)}%</div>'
            '<div class="mx-kpi-label">Avg OEE</div></div>',
            unsafe_allow_html=True,
        )
    with k5:
        risk_color = "#E53E3E" if avg_risk and avg_risk >= 0.5 else "#DD6B20" if avg_risk and avg_risk >= 0.2 else "#38A169"
        st.markdown(
            '<div class="mx-kpi-card">'
            f'<div class="mx-kpi-value" style="color:{risk_color};">{_fmt(avg_risk, 3)}</div>'
            '<div class="mx-kpi-label">Avg Fault Risk</div></div>',
            unsafe_allow_html=True,
        )

    # ── Pipeline latency stats (from vw_pipeline_latency) ───────────
    try:
        machine_filter = list(filtered["machine_id"].unique()) if shown <= 20 else None
        lat_df = _fetch_latency_stats(
            clients,
            tuple(machine_filter) if machine_filter else None,
            tuple(sel_states) if sel_states else None,
            tuple(sel_lines) if sel_lines else None,
            10,
        )
        if not lat_df.empty:
            fleet_d2h = lat_df["avg_d2h_ms"].mean()
            fleet_h2z = lat_df["avg_h2z_ms"].mean()
            fleet_total = lat_df["avg_total_ms"].mean()
            p50_total = lat_df["p50_total_ms"].mean()
            p95_total = lat_df["p95_total_ms"].mean()
            samples = int(lat_df["sample_count"].sum())

            def _lat_color(ms):
                if ms is None:
                    return "#718096"
                if ms < 1000:
                    return "#38A169"
                if ms < 5000:
                    return "#DD6B20"
                return "#E53E3E"

            st.markdown(
                '<div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin:0.3rem 0 0.7rem;">'
                '<div class="mx-kpi-card" style="flex:1;min-width:110px;">'
                f'<div class="mx-kpi-value" style="font-size:1.15rem;color:{_lat_color(fleet_d2h)};">{_fmt(fleet_d2h, 0)} ms</div>'
                '<div class="mx-kpi-label">Avg Device → IoT Hub</div></div>'
                '<div class="mx-kpi-card" style="flex:1;min-width:110px;">'
                f'<div class="mx-kpi-value" style="font-size:1.15rem;color:{_lat_color(fleet_h2z)};">{_fmt(fleet_h2z, 0)} ms</div>'
                '<div class="mx-kpi-label">Avg IoT Hub → ZeroBus</div></div>'
                '<div class="mx-kpi-card" style="flex:1;min-width:110px;">'
                f'<div class="mx-kpi-value" style="font-size:1.15rem;color:{_lat_color(fleet_total)};">{_fmt(fleet_total, 0)} ms</div>'
                '<div class="mx-kpi-label">Avg End-to-End</div></div>'
                '<div class="mx-kpi-card" style="flex:1;min-width:110px;">'
                f'<div class="mx-kpi-value" style="font-size:1.15rem;">{_fmt(p50_total, 0)} / {_fmt(p95_total, 0)}</div>'
                '<div class="mx-kpi-label">P50 / P95 Total</div></div>'
                '<div class="mx-kpi-card" style="flex:1;min-width:90px;">'
                f'<div class="mx-kpi-value" style="font-size:1.15rem;">{samples:,}</div>'
                '<div class="mx-kpi-label">Samples (10m)</div></div>'
                '</div>',
                unsafe_allow_html=True,
            )
    except Exception:
        pass

    # ── Detail section ────────────────────────────────────────────────
    if shown == 0:
        st.info("No machines match the current filters.")
        return

    if shown <= 12:
        cols = st.columns(2)
        for idx, (_, row) in enumerate(filtered.iterrows()):
            with cols[idx % 2]:
                _render_machine_card(row)
    else:
        display_cols = [
            "machine_id", "line_name", "state", "risk_band",
            "oee_pct", "prob_fault_next_5m", "anomaly_score",
            "vibration_mm_s", "temp_c", "current_amps",
            "rpm", "throughput_cpm", "humidity_pct",
            "power_kw", "telemetry_lag_ms", "last_event_time",
        ]
        display_cols = [c for c in display_cols if c in filtered.columns]
        styled = filtered[display_cols].copy()
        styled = styled.sort_values("prob_fault_next_5m", ascending=False)
        if "last_event_time" in styled.columns:
            styled["last_event_time"] = styled["last_event_time"].apply(_fmt_ts)

        rename_map = {
            "machine_id": "Machine",
            "line_name": "Line",
            "state": "State",
            "risk_band": "Risk",
            "oee_pct": "OEE %",
            "prob_fault_next_5m": "Fault Risk",
            "anomaly_score": "Anomaly",
            "vibration_mm_s": "Vibration",
            "temp_c": "Temp °C",
            "current_amps": "Current A",
            "rpm": "RPM",
            "throughput_cpm": "Throughput",
            "humidity_pct": "Humidity %",
            "power_kw": "Power kW",
            "telemetry_lag_ms": "Lag ms",
            "last_event_time": "Last Event",
        }
        styled = styled.rename(columns={k: v for k, v in rename_map.items() if k in styled.columns})

        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            height=min(700, 40 + 35 * len(styled)),
        )

    # ── Quick-create service request ─────────────────────────────────
    st.markdown("---")
    with st.expander("Create Service Request", icon=":material/build:"):
        available_machines = sorted(filtered["machine_id"].unique().tolist())
        default_machines = sel_ids if sel_ids else []

        with st.form("sr_quick_form"):
            sr_machines = st.multiselect(
                "Machine(s)",
                available_machines,
                default=default_machines,
                key="sr_machines",
            )
            sr_c1, sr_c2 = st.columns(2)
            with sr_c1:
                sr_priority = st.selectbox("Priority", ["CRITICAL", "HIGH", "MEDIUM", "LOW"], index=2, key="sr_priority")
            with sr_c2:
                sr_type = st.selectbox(
                    "Type", ["PREVENTIVE", "CORRECTIVE", "INSPECTION", "CALIBRATION"], key="sr_type",
                )
            # Pre-fill from AI generation if available
            if "sr_ai_desc" in st.session_state:
                st.session_state["sr_desc_field"] = st.session_state.pop("sr_ai_desc")
            sr_desc = st.text_area(
                "Description",
                key="sr_desc_field",
                placeholder="Describe the issue or maintenance needed...",
            )
            sr_requestor = st.text_input("Requestor", placeholder="Your name or email")

            btn_c1, btn_c2 = st.columns(2)
            with btn_c1:
                try:
                    ai_generate = st.form_submit_button(
                        "Generate Description with AI",
                        icon=":material/auto_awesome:",
                    )
                except TypeError:
                    ai_generate = st.form_submit_button(
                        "Generate Description with AI",
                    )
            with btn_c2:
                submitted = st.form_submit_button(
                    "Submit Service Request",
                    type="primary",
                )

        if ai_generate and sr_machines:
            with st.spinner("Querying ML scores and generating description via ai_query()..."):
                try:
                    st.session_state["sr_ai_desc"] = clients.generate_sr_description(sr_machines, sr_type)
                    st.rerun()
                except Exception as exc:
                    st.warning(f"AI generation failed — fill in manually. ({exc})")
        elif ai_generate:
            st.warning("Select at least one machine before generating a description.")

        if submitted and sr_machines:
            try:
                batch = clients.create_service_request(
                    machine_ids=sr_machines,
                    priority=sr_priority,
                    request_type=sr_type,
                    description=sr_desc,
                    requestor=sr_requestor,
                )
                st.success(f"Service request **{batch}** created for {len(sr_machines)} machine(s).")
                # Clear form fields after successful submit
                for key in ["sr_machines", "sr_priority", "sr_type", "sr_desc_field"]:
                    st.session_state.pop(key, None)
                st.rerun()
            except Exception as exc:
                st.error(f"Failed to create service request: {exc}")

    # ── Auto-refresh trigger ──────────────────────────────────────────
    if st.session_state.get("mx_auto_refresh"):
        secs = st.session_state.get("mx_interval", 15)
        time.sleep(secs)
        st.rerun()
