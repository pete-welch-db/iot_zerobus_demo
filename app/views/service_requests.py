"""Service Requests — view, create, and manage maintenance requests backed by Lakebase."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from data_access import DataClients
from views import freshness

_STATUS_ORDER = ["OPEN", "IN_PROGRESS", "RESOLVED", "CANCELLED"]
_STATUS_COLORS = {
    "OPEN": "#3182CE",
    "IN_PROGRESS": "#DD6B20",
    "RESOLVED": "#38A169",
    "CANCELLED": "#718096",
}
_PRIORITY_COLORS = {
    "CRITICAL": "#E53E3E",
    "HIGH": "#DD6B20",
    "MEDIUM": "#3182CE",
    "LOW": "#38A169",
}
_PRIORITY_ORDER = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

_PAGE_CSS = """
<style>
.sr-kpi-row {
    display: flex;
    gap: 0.6rem;
    flex-wrap: wrap;
    margin-bottom: 1rem;
}
.sr-kpi-card {
    flex: 1;
    min-width: 120px;
    border: 1px solid #e8eaed;
    border-radius: 10px;
    padding: 0.7rem 1rem;
    background: #fff;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    text-align: center;
    cursor: pointer;
    transition: border-color 0.15s;
}
.sr-kpi-card:hover {
    border-color: #FF3621;
}
.sr-kpi-card.active {
    border-color: #FF3621;
    border-width: 2px;
}
.sr-kpi-value {
    font-size: 1.5rem;
    font-weight: 800;
    color: #1a202c;
}
.sr-kpi-label {
    font-size: 0.75rem;
    color: #718096;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.sr-badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.5px;
    color: #fff;
}
.sr-card {
    border: 1px solid #E8ECF1;
    border-radius: 10px;
    padding: 14px 16px;
    margin-bottom: 8px;
    background: #fff;
    transition: box-shadow 0.15s;
}
.sr-card:hover {
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}
.sr-card-header {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 6px;
    flex-wrap: wrap;
}
.sr-card-id {
    font-weight: 700;
    font-size: 0.9rem;
    color: #1a202c;
}
.sr-card-machine {
    font-weight: 600;
    font-size: 0.82rem;
    color: #4A5568;
    background: #F5F7FA;
    padding: 1px 8px;
    border-radius: 4px;
}
.sr-card-type {
    font-size: 0.78rem;
    color: #718096;
}
.sr-card-desc {
    font-size: 0.88rem;
    color: #2D3748;
    line-height: 1.4;
    margin: 6px 0;
    padding: 8px 10px;
    background: #FAFBFC;
    border-radius: 6px;
    border-left: 3px solid #E2E8F0;
}
.sr-card-meta {
    font-size: 0.75rem;
    color: #A0AEC0;
}
</style>
"""


def _fmt_ts(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "--"
    try:
        ts = pd.Timestamp(val)
        return ts.strftime("%-m/%-d %I:%M:%S %p") if not pd.isna(ts) else "--"
    except Exception:
        return str(val)


def _badge(text: str, color: str) -> str:
    return f'<span class="sr-badge" style="background:{color};">{text}</span>'


def _priority_badge(priority: str) -> str:
    return _badge(priority, _PRIORITY_COLORS.get(priority, "#718096"))


def _status_badge(status: str) -> str:
    return _badge(status.replace("_", " "), _STATUS_COLORS.get(status, "#718096"))


def render(clients: DataClients | None = None) -> None:
    freshness.render_freshness_bar()
    st.markdown(_PAGE_CSS, unsafe_allow_html=True)

    if clients is None:
        clients = st.session_state.clients

    st.markdown(
        '<h2 style="margin-bottom:0;">Service Requests</h2>'
        '<p style="color:#718096;margin-top:2px;">Maintenance work orders backed by Lakebase OLTP</p>',
        unsafe_allow_html=True,
    )

    if not clients.lakebase_available():
        st.info(
            "Lakebase credentials not configured. "
            "Set LAKEBASE_DB_HOST / PORT / NAME / USER and LAKEBASE_INSTANCE_NAME in your .env file."
        )
        return

    # ── Fetch all requests (filter client-side for instant interaction) ─
    try:
        df = clients.query_service_requests()
    except Exception as exc:
        st.error(f"Failed to query service requests: {exc}")
        return

    # ── KPI row ─────────────────────────────────────────────────────────
    total = len(df)
    counts = {s: int((df["status"] == s).sum()) if not df.empty else 0 for s in _STATUS_ORDER}

    st.markdown(
        '<div class="sr-kpi-row">'
        '<div class="sr-kpi-card">'
        f'<div class="sr-kpi-value">{total}</div>'
        '<div class="sr-kpi-label">Total</div></div>'
        + "".join(
            f'<div class="sr-kpi-card">'
            f'<div class="sr-kpi-value" style="color:{_STATUS_COLORS[s]};">{counts[s]}</div>'
            f'<div class="sr-kpi-label">{s.replace("_", " ")}</div></div>'
            for s in _STATUS_ORDER
        )
        + "</div>",
        unsafe_allow_html=True,
    )

    # ── Inline filters ──────────────────────────────────────────────────
    with st.container():
        fc1, fc2, fc3, fc4, fc5 = st.columns([1.5, 1.5, 1.5, 1.5, 1])
        with fc1:
            sel_statuses = st.multiselect(
                "Status",
                _STATUS_ORDER,
                default=["OPEN", "IN_PROGRESS"],
                key="sr_f_status",
            )
        with fc2:
            sel_priorities = st.multiselect(
                "Priority",
                _PRIORITY_ORDER,
                default=[],
                key="sr_f_priority",
            )
        with fc3:
            type_options = sorted(df["request_type"].unique().tolist()) if not df.empty else []
            sel_types = st.multiselect("Type", type_options, default=[], key="sr_f_type")
        with fc4:
            machine_options = sorted(df["machine_id"].unique().tolist()) if not df.empty else []
            sel_machines = st.multiselect("Machine", machine_options, default=[], key="sr_f_machine")
        with fc5:
            sort_options = {"Newest first": ("created_at", False), "Oldest first": ("created_at", True), "Priority": ("priority", True)}
            sort_choice = st.selectbox("Sort", list(sort_options.keys()), key="sr_f_sort")

    # ── Apply filters ───────────────────────────────────────────────────
    filtered = df.copy()
    if sel_statuses:
        filtered = filtered[filtered["status"].isin(sel_statuses)]
    if sel_priorities:
        filtered = filtered[filtered["priority"].isin(sel_priorities)]
    if sel_types:
        filtered = filtered[filtered["request_type"].isin(sel_types)]
    if sel_machines:
        filtered = filtered[filtered["machine_id"].isin(sel_machines)]

    sort_col, sort_asc = sort_options[sort_choice]
    if sort_col == "priority":
        filtered["_pri_ord"] = filtered["priority"].map(
            {p: i for i, p in enumerate(_PRIORITY_ORDER)}
        )
        filtered = filtered.sort_values("_pri_ord").drop(columns=["_pri_ord"])
    else:
        filtered = filtered.sort_values(sort_col, ascending=sort_asc)

    st.caption(f"Showing {len(filtered)} of {total} requests")

    # ── Request cards ───────────────────────────────────────────────────
    if filtered.empty:
        st.info("No service requests match the current filters.")
    else:
        for _, row in filtered.iterrows():
            desc = row.get("description") or ""
            desc_html = (
                f'<div class="sr-card-desc">{desc}</div>' if desc else ""
            )

            st.markdown(
                f'<div class="sr-card">'
                f'  <div class="sr-card-header">'
                f'    <span class="sr-card-id">{row["id"]}</span>'
                f"    {_status_badge(row['status'])}"
                f"    {_priority_badge(row['priority'])}"
                f'    <span class="sr-card-machine">{row["machine_id"]}</span>'
                f'    <span class="sr-card-type">{row["request_type"]}</span>'
                f"  </div>"
                f"  {desc_html}"
                f'  <div class="sr-card-meta">'
                f"    Requested by {row.get('requestor') or 'unknown'}"
                f"    &middot; Created {_fmt_ts(row.get('created_at'))}"
                f"    &middot; Updated {_fmt_ts(row.get('updated_at'))}"
                f"  </div>"
                f"</div>",
                unsafe_allow_html=True,
            )

            # Inline status update
            col_spacer, col_action = st.columns([4, 1])
            with col_action:
                current_status = row["status"]
                new_status = st.selectbox(
                    "Update status",
                    _STATUS_ORDER,
                    index=_STATUS_ORDER.index(current_status) if current_status in _STATUS_ORDER else 0,
                    key=f"sr_status_{row['id']}",
                    label_visibility="collapsed",
                )
                if new_status != current_status:
                    try:
                        clients.update_service_request_status(row["id"], new_status)
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Update failed: {exc}")

    # ── Create new service request ──────────────────────────────────────
    st.markdown("---")
    with st.expander("Create New Service Request", icon=":material/add_circle:"):
        try:
            machines_df = clients.query_lakebase_machines()
            available_machines = sorted(machines_df["machine_id"].unique().tolist()) if not machines_df.empty else []
        except Exception:
            available_machines = []

        with st.form("sr_new_form"):
            sr_machines = st.multiselect("Machine(s)", available_machines, key="sr_new_machines")
            c1, c2 = st.columns(2)
            with c1:
                sr_priority = st.selectbox(
                    "Priority", _PRIORITY_ORDER, index=2, key="sr_new_priority",
                )
            with c2:
                sr_type = st.selectbox(
                    "Type", ["PREVENTIVE", "CORRECTIVE", "INSPECTION", "CALIBRATION"], key="sr_new_type",
                )
            # Pre-fill from AI generation if available
            if "sr_new_ai_desc" in st.session_state:
                st.session_state["sr_new_desc"] = st.session_state.pop("sr_new_ai_desc")
            sr_desc = st.text_area(
                "Description",
                key="sr_new_desc",
                placeholder="Describe the issue or maintenance needed...",
            )
            sr_requestor = st.text_input("Requestor", placeholder="Your name or email")

            btn_c1, btn_c2 = st.columns(2)
            with btn_c1:
                ai_generate = st.form_submit_button(
                    "Generate Description with AI",
                    icon=":material/auto_awesome:",
                )
            with btn_c2:
                submitted = st.form_submit_button(
                    "Submit Service Request",
                    type="primary",
                )

        if ai_generate and sr_machines:
            with st.spinner("Querying ML scores and generating description via ai_query()..."):
                try:
                    st.session_state["sr_new_ai_desc"] = clients.generate_sr_description(sr_machines, sr_type)
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
                st.rerun()
            except Exception as exc:
                st.error(f"Failed to create service request: {exc}")
