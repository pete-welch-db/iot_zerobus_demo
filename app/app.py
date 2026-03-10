"""
IoT Manufacturing Flow Break Command Center — Application Entry Point
Routing only: delegates to view modules via st.navigation.
"""
from pathlib import Path

import streamlit as st

from config import load_config
from data_access import DataClients

_ASSETS_DIR = Path(__file__).resolve().parent / "assets"
_DATABRICKS_LOGO = _ASSETS_DIR / "Databricks_Logo.png"

st.set_page_config(
    page_title="IoT Manufacturing Flow Break Command Center",
    page_icon=":material/precision_manufacturing:",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.logo(str(_DATABRICKS_LOGO), size="large")

st.markdown(
    """
    <style>
    section.main > div.block-container {
        padding-top: 1rem !important;
    }
    [data-testid="stLogo"] {
        height: auto !important;
        max-height: none !important;
        padding-bottom: 1rem;
    }
    [data-testid="stLogo"] img {
        max-height: 80px !important;
        width: auto !important;
    }
    [data-testid="stSidebarNav"] {
        min-height: auto;
    }
    [data-testid="stSidebarNavLink"][aria-selected="true"] {
        background-color: rgba(255,54,33,0.08);
        border-left: 3px solid #FF3621;
    }
    [data-testid="stMetric"] {
        background: #fff;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 0.5rem 0.75rem;
    }
    [data-testid="stExpander"] {
        border: 1px solid #E2E8F0;
        border-radius: 10px;
    }
    a { color: #FF3621; }
    button[kind="primary"] {
        background-color: #FF3621 !important;
        border-color: #FF3621 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "cfg" not in st.session_state:
    cfg = load_config()
    st.session_state.cfg = cfg
    st.session_state.clients = DataClients(cfg)

from views import landing_v2, metrics, flow_break, genie_chat, lakebase_live, service_requests, settings, freshness

page = st.navigation(
    [
        st.Page(landing_v2.render, title="Overview", url_path="overview", icon=":material/home:", default=True),
        st.Page(metrics.render, title="Metrics & Dashboard", url_path="metrics", icon=":material/monitoring:"),
        st.Page(flow_break.render, title="Flow Break Risk", url_path="flow-break-risk", icon=":material/warning:"),
        st.Page(genie_chat.render, title="Genie Assistant", url_path="genie", icon=":material/smart_toy:"),
        st.Page(lakebase_live.render, title="Machine Explorer", url_path="machine-explorer", icon=":material/precision_manufacturing:"),
        st.Page(service_requests.render, title="Service Requests", url_path="service-requests", icon=":material/build:"),
        st.Page(settings.render, title="Settings", url_path="settings", icon=":material/settings:"),
    ]
)

freshness.render_freshness_sidebar()
page.run()
