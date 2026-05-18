import os
import streamlit as st

from views import freshness

SLIDES_DIR = os.path.join(os.path.dirname(__file__), "..", "assets", "slides")

TAB_NAMES = [
    "The Challenge",
    "ZeroBus Ingest",
    "Lakebase",
    "Platform Architecture",
    "Evidence & Sources",
]


def _slide_image(filename: str, caption: str | None = None):
    """Render a slide image with optional caption, scaled to full width."""
    path = os.path.join(SLIDES_DIR, filename)
    if os.path.exists(path):
        try:
            st.image(path, use_container_width=True, caption=caption)
        except TypeError:
            st.image(path, use_column_width=True, caption=caption)
    else:
        st.warning(f"Image not found: {filename}")


def _nav_bar(current: int):
    """Render bottom navigation: back / section indicator / next."""
    st.markdown(
        "<div style='height:12px'></div><hr style='margin:0 0 12px 0'>",
        unsafe_allow_html=True,
    )
    col_back, col_label, col_next = st.columns([1, 3, 1])

    with col_back:
        if current > 0:
            if st.button(
                f"\u2190  {TAB_NAMES[current - 1]}",
                key="nav_back",
                use_container_width=True,
            ):
                st.session_state["landing_section"] = current - 1
                st.rerun()

    with col_label:
        pips = "  ".join(
            f"**\u25cf**" if i == current else "\u25cb"
            for i in range(len(TAB_NAMES))
        )
        st.markdown(
            f"<p style='text-align:center;color:#718096;margin:8px 0 0 0'>"
            f"<span style='font-size:0.85rem'>{pips}</span><br>"
            f"<span style='font-size:0.78rem'>{current + 1} / {len(TAB_NAMES)}  \u2014  {TAB_NAMES[current]}</span>"
            f"</p>",
            unsafe_allow_html=True,
        )

    with col_next:
        if current < len(TAB_NAMES) - 1:
            if st.button(
                f"{TAB_NAMES[current + 1]}  \u2192",
                key="nav_next",
                use_container_width=True,
            ):
                st.session_state["landing_section"] = current + 1
                st.rerun()


# ── Section renderers ─────────────────────────────────────────────────


def _section_challenge():
    st.subheader("The Manufacturing Problem: Lots of Signals, Expensive Interruptions")

    r1, r2, r3 = st.columns(3)
    r1.metric("Downtime reduction potential", "30\u201350%")
    r2.metric("Maintenance cost reduction", "10\u201340%")
    r3.metric("Machine life extension", "20\u201340%")

    st.caption(
        "Sources: McKinsey predictive maintenance benchmarks. "
        "https://www.mckinsey.com/capabilities/operations/our-insights/manufacturing-analytics-unleashes-productivity-and-profitability"
    )

    r4, r5, r6 = st.columns(3)
    r4.metric("Throughput improvement", "10\u201330%")
    r5.metric("Labor productivity improvement", "15\u201330%")
    r6.metric("Annual cost of unplanned downtime", "$50B")

    st.caption(
        "Sources: McKinsey Industry 4.0 value benchmarks; Deloitte / IndustryWeek downtime estimate."
    )

    st.markdown("---")

    _slide_image(
        "01_unsustainable_trajectory.png",
        "Point-to-point integrations, legacy databases, and proprietary formats create an unsustainable trajectory",
    )
    _slide_image(
        "02_current_limitations.png",
        "IT and OT data silos prevent enterprise-scale decisions",
    )

    st.markdown("---")

    _slide_image(
        "04_factory_challenges.png",
        "Every factory and machine is different \u2014 making standardization a core challenge",
    )


def _section_zerobus():
    st.subheader("ZeroBus: Single-Hop Push Ingestion to Your Lakehouse")
    st.markdown(
        """
        **Your Azure IoT Hub stays in place.** ZeroBus replaces the message bus layer
        (Event Hubs, Kafka) with a direct push to managed Delta tables in Unity Catalog.

        - **< 5 second latency** from source to queryable table
        - **Up to 100 MB/sec** per connection, 10+ GB/sec per table
        - **No middleware** \u2014 no message bus to scale, patch, or monitor
        - **Built-in exactly-once** delivery with Unity Catalog commit resolution
        """
    )

    _slide_image(
        "05_zerobus_overview.png",
        "ZeroBus Ingest: simplify ingestion for IoT, clickstream, and telemetry",
    )

    st.markdown("---")
    col_before, col_after = st.columns(2)
    with col_before:
        _slide_image(
            "06_before_architecture.png",
            "Before: Multi-hop ingestion through message bus + Spark",
        )
    with col_after:
        _slide_image(
            "07_after_architecture.png",
            "After: Single-hop ingestion with ZeroBus Ingest",
        )


def _section_lakebase():
    st.subheader("Lakebase: Fully-Managed Postgres for Real-Time Applications")
    st.markdown(
        """
        Once IoT data lands in your Lakehouse via ZeroBus, **Lakebase** serves it
        to operational applications as a fully-managed, serverless Postgres database
        built into the Databricks platform.

        - **Separates compute & storage** \u2014 scale independently
        - **Autoscaling & scale-to-zero** \u2014 only pay when active
        - **Branching & snapshots** \u2014 git-style workflows for data
        - **Unified governance** \u2014 tables governed in Unity Catalog alongside analytics and AI
        - **No ETL required** \u2014 Lakebase tables sync with the Lakehouse automatically
        """
    )

    _slide_image(
        "08_lakebase_architecture.png",
        "Lakebase separates compute and storage for serverless Postgres",
    )

    st.markdown("---")

    _slide_image(
        "09_lakebase_benefits.png",
        "Lower TCO, integrated platform, great developer experience",
    )

    st.markdown("---")

    _slide_image(
        "10_lakebase_usecases.png",
        "Analyze data in the Lakehouse, build AI and traditional apps with Lakebase",
    )


def _section_platform():
    st.subheader("Built on an Open Foundation")
    st.markdown(
        """
        The complete Denso architecture:
        **Plant Floor \u2192 Azure IoT Hub \u2192 ZeroBus Ingest \u2192 Lakehouse \u2192 Lakebase \u2192 Apps**

        All data governed in Unity Catalog. ML models, dashboards, Genie rooms, and
        this operational application all run on the same platform.
        """
    )

    _slide_image(
        "11_open_foundation.png",
        "Data Intelligence Platform: ingest, govern, enable, and serve",
    )

    st.markdown("---")

    _slide_image(
        "03_smart_factory_iiot.png",
        "Smart Factory IIoT high-level architecture on Databricks",
    )

    st.markdown("---")
    st.subheader("Why Databricks for IoT Flow Break Prediction")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown(
            """
            - **Streaming + batch on one platform**: ingest PLC, sensor, MES, ERP, and quality data into one governed lakehouse.
            - **Unity Catalog governance**: secure, discoverable telemetry, features, models, and KPI definitions.
            - **Lakeflow / pipelines**: continuously refresh signal, event, and machine-state layers without brittle point tooling.
            """
        )

    with c2:
        st.markdown(
            """
            - **ML + SQL together**: train risk models while exposing the same trusted outputs to analysts and engineers.
            - **AI/BI + Genie**: let plant leaders explore downtime, flow breaks, and root-cause trends with natural language.
            - **Databricks Apps**: package an operator-facing workflow with real-time status, guided action, and governed metrics.
            """
        )

    st.markdown(
        """
        <div style="
            background: #F5F7FA;
            border-radius: 12px;
            padding: 18px 20px;
            margin: 22px 0 18px 0;
            border: 1px solid #E8ECF1;
            border-left: 4px solid #FF3621;
        ">
            <p style="color: #1B3139; font-size: 1.0rem; font-weight: 600; margin: 0 0 6px 0;">
                Unity Catalog as the manufacturing control plane
            </p>
            <p style="color: #4A5568; font-size: 0.92rem; margin: 0;">
                Governed telemetry, feature tables, metric definitions, dashboards, Genie spaces,
                and app semantics can all be reused consistently across one platform instead of being
                fragmented across OT, BI, and ML silos.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _section_evidence():
    st.subheader("Evidence-Based Context")
    st.markdown(
        """
        - **Predictive maintenance materially reduces downtime**: McKinsey reports that predictive maintenance typically cuts machine downtime by **30\u201350%**, lowers maintenance costs by **10\u201340%**, and extends machine life by **20\u201340%**.
        - **Smart factory transformations improve plant performance**: McKinsey reports that Industry 4.0 deployments commonly drive **10\u201330% throughput gains** and **15\u201330% labor productivity improvements**.
        - **Downtime is financially massive**: Deloitte cites research that unplanned downtime costs industrial manufacturers an estimated **$50 billion annually**.
        - **The operating model matters as much as the model itself**: for this reason, the winning pattern is not just prediction, but prediction connected to governed data, real-time app experiences, and action workflows.
        """
    )

    with st.expander("Reference notes"):
        st.markdown(
            """
            **Primary sources used in this page**

            1. McKinsey \u2014 *Manufacturing analytics unleashes productivity and profitability*
               https://www.mckinsey.com/capabilities/operations/our-insights/manufacturing-analytics-unleashes-productivity-and-profitability

            2. McKinsey \u2014 *Capturing the true value of Industry 4.0*
               https://www.mckinsey.com/capabilities/operations/our-insights/capturing-the-true-value-of-industry-four-point-zero

            3. Deloitte \u2014 *Asset Optimization: Predictive Maintenance and the Smart Factory*
               https://www.deloitte.com/us/en/services/consulting/services/predictive-maintenance-and-the-smart-factory.html

            4. Siemens / IndustryWeek whitepaper \u2014 *Maintenance 4.0*
               https://storydesign.industryweek.com/Global/FileLib/Siemens/Maintenance_4.0_whitepaper.pdf
            """
        )


_SECTIONS = [
    _section_challenge,
    _section_zerobus,
    _section_lakebase,
    _section_platform,
    _section_evidence,
]


# ── Main render ───────────────────────────────────────────────────────


def render():
    """Landing page narrative for an IoT manufacturing demo with research-backed metrics."""
    freshness.render_freshness_bar()
    st.markdown(
        """
        <style>
        .main .block-container {
            padding-top: 1.1rem;
            padding-bottom: 1.0rem;
        }
        div[data-testid="stMetric"] {
            background: #F5F7FA;
            border: 1px solid #E2E8F0;
            border-radius: 10px;
            padding: 8px 12px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div style="
            background: linear-gradient(135deg, #FFFFFF 0%, #F5F7FA 100%);
            border-radius: 16px;
            padding: 26px 28px;
            margin-bottom: 14px;
            border: 1px solid #E8ECF1;
            position: relative;
            overflow: hidden;
        ">
            <div style="
                position: absolute;
                top: -60px;
                right: -60px;
                width: 220px;
                height: 220px;
                background: radial-gradient(circle, rgba(255,54,33,0.12) 0%, transparent 70%);
                border-radius: 50%;
            "></div>
            <div style="position: relative; z-index: 1;">
                <p style="
                    color: #FF3621;
                    font-size: 0.82rem;
                    font-weight: 600;
                    letter-spacing: 1.8px;
                    text-transform: uppercase;
                    margin-bottom: 10px;
                ">DATABRICKS FOR MANUFACTURING</p>
                <p style="
                    color: #1B3139;
                    font-size: 2.05rem;
                    font-weight: 700;
                    line-height: 1.15;
                    margin: 0 0 8px 0;
                ">IoT Flow Break Prediction</p>
                <p style="
                    color: #4A5568;
                    font-size: 1.0rem;
                    margin: 0;
                    max-width: 760px;
                ">
                    Unify streaming telemetry, governed manufacturing data, predictive models,
                    and operational applications on Databricks to detect flow breaks earlier and
                    help plant teams respond in real time.
                </p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div style="
            background: #FFFFFF;
            border-radius: 12px;
            padding: 14px 18px;
            margin-bottom: 14px;
            border: 1px solid #E8ECF1;
            border-left: 4px solid #FF3621;
        ">
            <p style="
                color: #1B3139;
                font-size: 0.95rem;
                font-style: italic;
                margin: 0 0 4px 0;
                line-height: 1.45;
            ">"The value is not in collecting more machine data. The value is in turning telemetry into
            earlier interventions, less downtime, and better throughput."</p>
            <p style="color: #718096; font-size: 0.9rem; margin: 0;">
                Modern manufacturing operating principle
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Section navigation ────────────────────────────────────────────
    if "landing_section" not in st.session_state:
        st.session_state["landing_section"] = 0

    current = st.session_state["landing_section"]

    # Scroll to top whenever section changes
    if st.session_state.get("_landing_prev") != current:
        st.session_state["_landing_prev"] = current
        st.components.v1.html(
            "<script>window.parent.document.querySelector('section.main').scrollTo(0,0);</script>",
            height=0,
        )

    # Section jump bar (always visible, compact)
    cols = st.columns(len(TAB_NAMES))
    for i, (col, name) in enumerate(zip(cols, TAB_NAMES)):
        with col:
            if i == current:
                st.markdown(
                    f"<div style='text-align:center;padding:6px 0;background:#FF3621;"
                    f"color:white;border-radius:6px;font-size:0.78rem;font-weight:600'>"
                    f"{name}</div>",
                    unsafe_allow_html=True,
                )
            else:
                if st.button(name, key=f"jump_{i}", use_container_width=True):
                    st.session_state["landing_section"] = i
                    st.rerun()

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    # ── Render current section ────────────────────────────────────────
    _SECTIONS[current]()

    # ── Bottom navigation ─────────────────────────────────────────────
    _nav_bar(current)
