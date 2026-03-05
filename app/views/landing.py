import streamlit as st


def render() -> None:
    st.title("IoT Manufacturing Flow Break Prediction")
    st.subheader("From raw signals to real-time decisions with Databricks")
    st.markdown(
        """
        Manufacturing operations increasingly depend on low-latency telemetry and predictive intelligence.
        This app demonstrates how Databricks helps teams ingest IoT signals at scale, predict flow breaks,
        and take action through a unified Data + AI + App platform.
        """
    )

    st.markdown("### Why Databricks + Zerobus for IoT")
    st.markdown(
        """
        - **Low-latency ingestion:** Zerobus continuously lands IoT telemetry into governed Unity Catalog tables.
        - **Unified analytics and AI:** SQL, ML, Genie, and Lakebase-powered app experiences run on one platform.
        - **Operational + analytical convergence:** Lakebase mirrors current-state operational records for responsive app reads.
        - **Governed metrics:** UC metric views standardize OEE, risk, and freshness KPIs across dashboard, Genie, and app.
        """
    )

    st.markdown("### Evidence-Based Context")
    st.markdown(
        """
        - Predictive maintenance and condition monitoring can materially reduce unplanned downtime and maintenance cost when deployed with reliable data pipelines.  
          Source: [McKinsey - The value of predictive maintenance](https://www.mckinsey.com/).
        - Smart factory programs consistently highlight real-time visibility + AI-assisted decisions as top drivers of throughput and quality gains.  
          Source: [Deloitte Smart Factory study](https://www2.deloitte.com/).
        - Streaming architectures and unified governance are foundational for production-grade industrial AI systems.  
          Source: [Databricks Product Documentation](https://docs.databricks.com/).
        """
    )
