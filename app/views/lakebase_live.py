import streamlit as st

from data_access import DataClients


def render_summary(clients: DataClients) -> None:
    st.markdown("#### Lakebase Live Snapshot")
    if not clients.lakebase_available():
        st.info("Lakebase is not configured for this runtime.")
        return

    try:
        df = clients.query_lakebase_status()
    except Exception as exc:
        st.warning(f"Lakebase summary unavailable: {exc}")
        return

    if df.empty:
        st.warning("Lakebase summary unavailable: no rows returned.")
        return

    if "source_kind" in df.columns:
        st.caption("Showing `mirror_metadata` fallback because `machine_current_status` is not available yet.")

    row_count = len(df)
    machine_count = (
        int(df["machine_id"].nunique())
        if "machine_id" in df.columns
        else int(df["instance_id"].nunique()) if "instance_id" in df.columns else 0
    )
    avg_risk = (
        float(df["prob_fault_next_5m"].astype(float).mean())
        if "prob_fault_next_5m" in df.columns
        else None
    )
    freshness_field = (
        "updated_at"
        if "updated_at" in df.columns
        else "last_run_at" if "last_run_at" in df.columns else "source_watermark"
    )
    latest_freshness = (
        str(df[freshness_field].max()) if freshness_field and freshness_field in df.columns else "n/a"
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Rows (sample)", row_count)
    c2.metric("Entities", machine_count)
    c3.metric("Avg Risk", f"{avg_risk:.3f}" if avg_risk is not None else "n/a")
    c4.metric("Latest Update", latest_freshness)


def render(clients: DataClients) -> None:
    st.subheader("Lakebase Live Operational View")
    if not clients.lakebase_available():
        st.info(
            "Lakebase credentials not configured for app runtime. "
            "Set LAKEBASE_DB_HOST/PORT/NAME/USER/PASSWORD in app environment."
        )
        return

    try:
        df = clients.query_lakebase_status()
    except Exception as exc:
        st.error(f"Failed to query Lakebase: {exc}")
        return

    if df.empty:
        st.warning("Lakebase query returned no rows.")
        return

    if "source_kind" in df.columns:
        st.warning(
            "Lakebase status table not found yet. Showing mirror metadata as a fallback freshness signal."
        )
    st.metric("Lakebase rows (sample)", len(df))
    st.dataframe(df, use_container_width=True, hide_index=True)
