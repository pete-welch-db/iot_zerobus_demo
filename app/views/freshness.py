"""Reusable data-freshness widget for the sidebar."""
from __future__ import annotations

import streamlit as st


@st.cache_data(ttl=30, show_spinner=False)
def _fetch_freshness(_clients) -> dict:
    """Cached freshness query — runs at most once every 30 seconds."""
    return _clients.query_pipeline_freshness()


def _age_label(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 60:
        return f"{seconds:.0f}s ago"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m ago"
    return f"{seconds / 3600:.1f}h ago"


def _freshness_color(seconds: float | None) -> str:
    if seconds is None:
        return "#718096"
    if seconds < 120:
        return "#38A169"
    if seconds < 600:
        return "#DD6B20"
    return "#E53E3E"


def _dot(color: str) -> str:
    return f"<span style='color:{color};font-weight:700;'>&#9679;</span>"


def render_freshness_sidebar() -> None:
    """Render freshness indicators in the sidebar.

    Pulls ``clients`` from ``st.session_state``.  Safe to call on any page;
    degrades gracefully if data is unavailable.
    """
    clients = st.session_state.get("clients")
    if clients is None:
        return

    try:
        f = _fetch_freshness(clients)
    except Exception:
        return

    if not f:
        return

    with st.sidebar:
        st.markdown(
            "<hr style='margin:8px 0 6px 0;border:none;border-top:1px solid #E8ECF1;'>",
            unsafe_allow_html=True,
        )
        st.markdown(
            "<p style='font-size:0.7rem;color:#718096;text-transform:uppercase;"
            "letter-spacing:0.8px;margin-bottom:4px;font-weight:600;'>Data Freshness</p>",
            unsafe_allow_html=True,
        )

        sql_age = f.get("sql_age_seconds")
        sql_color = _freshness_color(sql_age)
        st.markdown(
            f"{_dot(sql_color)} <span style='font-size:0.78rem;'>"
            f"SQL Warehouse: <b>{_age_label(sql_age)}</b></span>",
            unsafe_allow_html=True,
        )

        lb_age = f.get("lb_age_seconds")
        if lb_age is not None:
            lb_color = _freshness_color(lb_age)
            st.markdown(
                f"{_dot(lb_color)} <span style='font-size:0.78rem;'>"
                f"Lakebase OLTP: <b>{_age_label(lb_age)}</b></span>",
                unsafe_allow_html=True,
            )
        elif f.get("lb_error"):
            st.markdown(
                f"{_dot('#E53E3E')} <span style='font-size:0.78rem;'>Lakebase: offline</span>",
                unsafe_allow_html=True,
            )

        machines = f.get("sql_machine_count")
        if machines is not None:
            st.markdown(
                f"<span style='font-size:0.78rem;'>Machines: <b>{machines}</b></span>",
                unsafe_allow_html=True,
            )

        avg_lag = f.get("sql_avg_lag_ms")
        if avg_lag is not None:
            st.markdown(
                f"<span style='font-size:0.78rem;'>Avg Lag: <b>{avg_lag:,.0f} ms</b></span>",
                unsafe_allow_html=True,
            )

        sql_error = f.get("sql_error")
        if sql_error:
            st.markdown(
                f"{_dot('#E53E3E')} <span style='font-size:0.78rem;'>SQL: error</span>",
                unsafe_allow_html=True,
            )


def render_freshness_bar() -> None:
    """Deprecated -- now a no-op. Freshness is rendered in the sidebar by app.py."""
    pass
