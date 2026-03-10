"""Metrics & Dashboard page — combines Lakebase snapshot with embedded AI/BI dashboard."""
import streamlit as st

from views import lakebase_live, dashboard_embed, freshness


def render() -> None:
    freshness.render_freshness_bar()
    clients = st.session_state.clients
    cfg = st.session_state.cfg
    lakebase_live.render_summary(clients)
    dashboard_embed.render(cfg.dashboard_url)
