import streamlit as st

from config import load_config
from data_access import DataClients
from genie_client import GenieClient
from views import dashboard_embed, flow_break, genie_chat, lakebase_live, landing


def _apply_databricks_light_theme() -> None:
    st.markdown(
        """
        <style>
          :root {
            --db-orange: #ff5f15;
            --db-orange-dark: #ff3621;
            --db-bg: #ffffff;
            --db-surface: #f7f8fa;
            --db-text: #1f2328;
            --db-muted: #5f6b7a;
          }
          .stApp {
            background: var(--db-bg);
            color: var(--db-text);
          }
          [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #fff7f2 0%, #ffffff 100%);
            border-right: 1px solid #f1f2f4;
          }
          h1, h2, h3, .stMarkdown, .stCaption, label, p, div {
            color: var(--db-text);
          }
          [data-testid="stSidebar"] .stRadio > div {
            background: var(--db-surface);
            border-radius: 10px;
            padding: 0.4rem;
          }
          [data-testid="stSidebar"] .stRadio label {
            border-radius: 8px;
            padding: 0.35rem 0.5rem;
          }
          [data-testid="stSidebar"] .stRadio label:hover {
            background: #ffe9df;
          }
          [data-testid="stSidebar"] .stRadio [aria-checked="true"] {
            background: #ffd8c8;
            border-left: 3px solid var(--db-orange);
          }
          [data-testid="stVerticalBlock"] > [data-testid="element-container"] div[data-testid="stMetric"] {
            background: #fff;
            border: 1px solid #f1f2f4;
            border-radius: 10px;
            padding: 0.4rem 0.6rem;
          }
          [data-testid="stMarkdownContainer"] h2 {
            letter-spacing: 0.01em;
          }
          [data-testid="stExpander"] {
            border: 1px solid #f1f2f4;
            border-radius: 10px;
          }
          .db-sidebar-header {
            border-radius: 10px;
            background: #fff;
            padding: 0.5rem 0.6rem 0.2rem 0.6rem;
            margin-bottom: 0.5rem;
            border: 1px solid #f1f2f4;
          }
          .db-sidebar-subtitle {
            color: var(--db-muted);
            font-size: 0.86rem;
            margin-top: 0.2rem;
          }
          a {
            color: var(--db-orange-dark);
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    cfg = load_config()
    st.set_page_config(
        page_title=cfg.app_title,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _apply_databricks_light_theme()
    clients = DataClients(cfg)
    genie = GenieClient(cfg)

    st.sidebar.markdown(
        """
        <div class="db-sidebar-header">
          <img src="https://www.vectorlogo.zone/logos/databricks/databricks-ar21.svg" width="230" />
          <div class="db-sidebar-subtitle">IoT Manufacturing Flow Break Command Center</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    page = st.sidebar.radio(
        "Navigation",
        ["Landing", "Metrics", "Flow Break Risk", "Genie", "Lakebase Live"],
        label_visibility="visible",
    )

    if page == "Landing":
        landing.render()
    elif page == "Metrics":
        lakebase_live.render_summary(clients)
        dashboard_embed.render(cfg.dashboard_url)
    elif page == "Flow Break Risk":
        flow_break.render(clients)
    elif page == "Genie":
        genie_chat.render(genie)
    else:
        lakebase_live.render(clients)


if __name__ == "__main__":
    main()
