import streamlit as st
import streamlit.components.v1 as components
from urllib.parse import parse_qs, urlparse


def _to_embed_url(dashboard_url: str) -> str:
    if not dashboard_url:
        return ""
    parsed = urlparse(dashboard_url)
    if "/embed/dashboardsv3/" in parsed.path:
        return dashboard_url
    if "/dashboardsv3/" not in parsed.path:
        return dashboard_url

    # Convert published/view URL to embed URL expected for iframe rendering.
    dashboard_id = parsed.path.split("/dashboardsv3/", 1)[1].split("/", 1)[0]
    params = parse_qs(parsed.query)
    workspace_id = (params.get("o") or [""])[0]
    if workspace_id:
        return f"{parsed.scheme}://{parsed.netloc}/embed/dashboardsv3/{dashboard_id}?o={workspace_id}"
    return f"{parsed.scheme}://{parsed.netloc}/embed/dashboardsv3/{dashboard_id}"


def render(dashboard_url: str) -> None:
    st.subheader("Metrics And Embedded AI/BI Dashboard")
    if not dashboard_url:
        st.warning("Dashboard URL is not configured. Set APP_DASHBOARD_URL.")
        return

    embed_url = _to_embed_url(dashboard_url)
    with st.container(border=True):
        components.iframe(embed_url, height=900, scrolling=True)

    st.caption(
        "If browser embedding policy blocks rendering in your environment, open the dashboard in a new tab."
    )
    st.link_button("Open dashboard in new tab", embed_url)
