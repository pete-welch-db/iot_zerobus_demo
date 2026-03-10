"""
IoT Manufacturing — Settings
Connection status, Genie connectivity diagnostics, and debug logs.
"""
import socket
from urllib.parse import urlparse

import requests
import streamlit as st


def _genie_headers() -> dict:
    cfg = st.session_state.cfg
    return {
        "Authorization": f"Bearer {cfg.token}",
        "Content-Type": "application/json",
    }


def _genie_base_url() -> str:
    cfg = st.session_state.cfg
    return f"{cfg.workspace_host.rstrip('/')}/api/2.0/genie/spaces/{cfg.genie_space_id}"


def _append_debug(message: str):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    if "debug_logs" not in st.session_state:
        st.session_state.debug_logs = []
    timestamp = datetime.now(ZoneInfo("America/Detroit")).strftime("%H:%M:%S")
    st.session_state.debug_logs.append(f"[{timestamp}] {message}")
    if len(st.session_state.debug_logs) > 200:
        st.session_state.debug_logs = st.session_state.debug_logs[-200:]


def _run_connectivity_check() -> tuple[bool, dict]:
    cfg = st.session_state.cfg
    diagnostics: dict = {
        "workspace_host": cfg.workspace_host,
        "genie_space_id": cfg.genie_space_id,
        "catalog": cfg.catalog,
        "schema": cfg.schema,
    }

    parsed = urlparse(cfg.workspace_host)
    hostname = parsed.hostname or ""
    diagnostics["hostname"] = hostname

    try:
        ip = socket.gethostbyname(hostname) if hostname else ""
        diagnostics["dns_ip"] = ip
        _append_debug(f"DNS resolved {hostname} -> {ip}")
    except Exception as exc:
        diagnostics["dns_error"] = str(exc)
        _append_debug(f"DNS resolution failed: {exc}")

    if cfg.genie_space_id not in {"", "__AUTO__"}:
        try:
            resp = requests.get(
                _genie_base_url(),
                headers=_genie_headers(),
                timeout=15,
            )
            resp.raise_for_status()
            space = resp.json()
            diagnostics["genie_space_title"] = space.get("title")
            diagnostics["genie_warehouse_id"] = space.get("warehouse_id")
            _append_debug("Genie connectivity check passed")
        except Exception as exc:
            diagnostics["genie_api_error"] = str(exc)
            _append_debug(f"Genie API check failed: {exc}")
            return False, diagnostics
    else:
        diagnostics["genie_status"] = "not configured"

    if cfg.lakebase_host:
        try:
            lb_ip = socket.gethostbyname(cfg.lakebase_host)
            diagnostics["lakebase_dns_ip"] = lb_ip
            _append_debug(f"Lakebase DNS resolved {cfg.lakebase_host} -> {lb_ip}")
        except Exception as exc:
            diagnostics["lakebase_dns_error"] = str(exc)
    else:
        diagnostics["lakebase_status"] = "not configured"

    all_ok = "dns_error" not in diagnostics and "genie_api_error" not in diagnostics
    return all_ok, diagnostics


def render() -> None:
    cfg = st.session_state.cfg
    st.title("Settings")

    st.subheader("Connection Status")
    c1, c2, c3 = st.columns(3)
    c1.metric("Workspace", "Connected" if cfg.workspace_host else "Not set")
    c2.metric("Genie Space", cfg.genie_space_id[:12] + "..." if len(cfg.genie_space_id) > 12 else cfg.genie_space_id or "Not set")
    c3.metric("Lakebase", "Configured" if cfg.lakebase_host else "Not configured")

    st.markdown("---")

    st.subheader("Connectivity Check")
    if st.button("Run connectivity check", use_container_width=False):
        with st.spinner("Running connectivity checks..."):
            ok, diagnostics = _run_connectivity_check()
        if ok:
            st.success("Connectivity check passed")
        else:
            st.error("Connectivity check failed")
        st.json(diagnostics)

    st.markdown("---")

    st.subheader("Debug Logs")
    if "debug_logs" not in st.session_state:
        st.session_state.debug_logs = []

    if st.session_state.debug_logs:
        st.code("\n".join(st.session_state.debug_logs[-80:]))
    else:
        st.caption("No debug logs yet. Interact with Genie or run a connectivity check to generate logs.")

    if st.button("Clear debug logs"):
        st.session_state.debug_logs = []
        st.rerun()

    st.markdown("---")

    st.subheader("Environment")
    with st.expander("Current configuration", expanded=False):
        st.json({
            "workspace_host": cfg.workspace_host,
            "catalog": cfg.catalog,
            "schema": cfg.schema,
            "warehouse_id": cfg.warehouse_id,
            "genie_space_id": cfg.genie_space_id,
            "dashboard_url": cfg.dashboard_url[:60] + "..." if len(cfg.dashboard_url) > 60 else cfg.dashboard_url,
            "refresh_seconds": cfg.refresh_seconds,
            "lakebase_host": cfg.lakebase_host or "(not set)",
            "lakebase_db": cfg.lakebase_db or "(not set)",
        })
