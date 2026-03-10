"""
IoT Manufacturing — Genie + Research Agent page.
Chat mode gives direct answers; Research Agent runs multi-step investigation.
"""
import time

import pandas as pd
import requests
import streamlit as st

from views import freshness


PROCESSING_STATUSES = {
    "EXECUTING_QUERY", "PENDING", "FILTERING_RESULTS", "SUBMITTED",
    "RUNNING", "FILTERING_CONTEXT", "ASKING_AI", "PENDING_WAREHOUSE", "QUEUED",
}

CHAT_SAMPLE_QUESTIONS = [
    "Which machine has the highest flow-break risk right now and why?",
    "Show machines with prob_fault_next_5m above 0.5 and summarize primary signals.",
    "What changed in the last 15 minutes for the top risk machine?",
    "What is the average device-to-hub latency across all machines?",
    "Which machines are in FAULT state and what are their anomaly scores?",
    "Show throughput trend for MC-0000 over the last hour.",
]

RESEARCH_SAMPLE_QUESTIONS = [
    "Identify the top 3 machines most likely to experience a flow break in the next 5 minutes. "
    "What sensor signals are driving each prediction, and what is the confidence level?",
    "Analyze end-to-end pipeline latency: device-to-hub, hub-to-bridge, and total telemetry lag. "
    "Which hops are the bottleneck and what is the 95th percentile?",
    "Provide an operations readout: fleet OEE, machines in fault state, average anomaly score, "
    "and the top 3 risks with recommended actions.",
    "Compare vibration and temperature patterns between machines in RUN vs FAULT states. "
    "Are there leading indicators that precede fault transitions?",
]


def _genie_headers() -> dict:
    cfg = st.session_state.cfg
    return {
        "Authorization": f"Bearer {cfg.token}",
        "Content-Type": "application/json",
    }


def _genie_base_url() -> str:
    cfg = st.session_state.cfg
    return f"{cfg.workspace_host.rstrip('/')}/api/2.0/genie/spaces/{cfg.genie_space_id}"


def _api_request(method: str, url: str, payload: dict | None = None) -> dict:
    headers = _genie_headers()
    if method == "GET":
        resp = requests.get(url, headers=headers, timeout=45)
    else:
        resp = requests.post(url, headers=headers, json=payload, timeout=45)
    resp.raise_for_status()
    return resp.json()


def _start_conversation(prompt: str) -> tuple[str, str]:
    resp = _api_request("POST", f"{_genie_base_url()}/start-conversation", {"content": prompt})
    conv = resp.get("conversation") or resp
    msg = resp.get("message") or resp
    return conv.get("id", resp.get("conversation_id", "")), msg.get("id", resp.get("message_id", ""))


def _create_message(conversation_id: str, prompt: str) -> str:
    resp = _api_request(
        "POST",
        f"{_genie_base_url()}/conversations/{conversation_id}/messages",
        {"content": prompt},
    )
    msg = resp.get("message") or resp
    return msg.get("id", resp.get("message_id", ""))


def _get_message(conversation_id: str, message_id: str) -> dict:
    return _api_request(
        "GET",
        f"{_genie_base_url()}/conversations/{conversation_id}/messages/{message_id}",
    )


def _extract_text_and_query(response: dict):
    texts = []
    sql_blocks = []
    table_df = None

    if response.get("content"):
        texts.append(str(response["content"]))

    for att in response.get("attachments") or []:
        if not isinstance(att, dict):
            continue
        text_block = att.get("text")
        if isinstance(text_block, dict) and text_block.get("content"):
            texts.append(text_block["content"])
        elif isinstance(text_block, str) and text_block:
            texts.append(text_block)
        content = att.get("content")
        if content and content not in texts:
            texts.append(str(content))
        query_obj = att.get("query") or {}
        if query_obj.get("query"):
            sql_blocks.append(query_obj["query"])
        if query_obj.get("description") and query_obj["description"] not in texts:
            texts.append(query_obj["description"])

    query_result = response.get("query_result") or {}
    statement_id = query_result.get("statement_id")
    if statement_id:
        try:
            cfg = st.session_state.cfg
            stmt = _api_request(
                "GET",
                f"{cfg.workspace_host.rstrip('/')}/api/2.0/sql/statements/{statement_id}",
            )
            data_array = (
                stmt.get("result", {}).get("data_array")
                or stmt.get("result", {}).get("chunk", {}).get("data_array")
                or []
            )
            columns = [
                c.get("name")
                for c in stmt.get("manifest", {}).get("schema", {}).get("columns", [])
            ]
            if data_array and columns:
                table_df = pd.DataFrame(data_array, columns=columns)
        except Exception:
            pass

    return "\n\n".join(texts).strip(), sql_blocks, table_df


def _wait_for_completion(conversation_id: str, message_id: str, max_attempts: int = 60) -> dict:
    for _ in range(max_attempts):
        response = _get_message(conversation_id, message_id)
        status = str(response.get("status", "")).upper()
        if status == "COMPLETED":
            return response
        if status in ("FAILED", "CANCELLED", "CANCELED"):
            error = response.get("error")
            if isinstance(error, dict):
                error = error.get("message")
            raise RuntimeError(f"Genie request {status}: {error}")
        time.sleep(2.0)
    raise TimeoutError("Genie request timed out.")


def _is_stale_conversation(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "not found" in msg or "404" in msg


def _ask_genie(prompt: str) -> dict:
    response = None

    if st.session_state.genie_conversation_id:
        try:
            message_id = _create_message(st.session_state.genie_conversation_id, prompt)
            if not message_id:
                raise RuntimeError("No message_id returned for follow-up.")
            response = _wait_for_completion(st.session_state.genie_conversation_id, message_id)
        except Exception as exc:
            if not _is_stale_conversation(exc):
                raise
            st.session_state.genie_conversation_id = None

    if response is None:
        conversation_id, message_id = _start_conversation(prompt)
        if not conversation_id or not message_id:
            raise RuntimeError("Genie did not return conversation/message IDs.")
        st.session_state.genie_conversation_id = conversation_id
        response = _wait_for_completion(conversation_id, message_id)

    return response


def _render_chat_mode(prompt: str):
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Genie is generating an answer..."):
            try:
                response = _ask_genie(prompt)
                text, sql_blocks, data = _extract_text_and_query(response)
                st.markdown(text or "No text response returned.")
                if data is not None:
                    st.dataframe(data, use_container_width=True)
                for idx, sql in enumerate(sql_blocks):
                    with st.expander(f"Generated SQL {idx + 1}"):
                        st.code(sql, language="sql", wrap_lines=True)
            except TimeoutError:
                st.error("Genie timed out. Try a simpler question or reset conversation.")
            except Exception as exc:
                st.error(f"Genie request failed: {exc}")


def _render_research_mode(user_prompt: str):
    with st.chat_message("user"):
        st.markdown(user_prompt)

    with st.chat_message("assistant"):
        status = st.status("Research Agent running...", expanded=True)
        step_outputs = []
        steps = [
            (
                "Step 1: scope",
                "You are an IoT manufacturing research planner. For the question below, produce "
                "3 concise sub-questions that would validate the answer from different angles.\n\n"
                f"Question: {user_prompt}",
            ),
            (
                "Step 2: evidence",
                "Answer the original question with key evidence from the telemetry data. "
                "Return concise bullets first, then include any supporting tabular output.\n\n"
                f"Question: {user_prompt}",
            ),
            (
                "Step 3: challenge and risks",
                "Identify assumptions, potential data caveats, and what could change this answer. "
                "Consider sensor drift, latency spikes, and model confidence.\n\n"
                f"Question: {user_prompt}",
            ),
            (
                "Step 4: operations synthesis",
                "Provide an operations-ready synthesis in 5 bullets: answer, key drivers, "
                "confidence level, risk flags, and recommended next action.\n\n"
                f"Question: {user_prompt}",
            ),
        ]

        try:
            with st.spinner("Running multi-step analysis..."):
                for label, prompt in steps:
                    status.update(label=f"{label}...", state="running")
                    response = _ask_genie(prompt)
                    text, sql_blocks, data = _extract_text_and_query(response)
                    step_outputs.append({
                        "label": label,
                        "text": text,
                        "sql": sql_blocks,
                        "data": data,
                    })
                    time.sleep(0.15)
        except TimeoutError:
            status.update(label="Research Agent timeout", state="error")
            st.error("Research Agent timed out. Try resetting and asking a narrower question.")
            return
        except Exception as exc:
            status.update(label="Research Agent error", state="error")
            st.error(f"Research Agent failed: {exc}")
            return

        status.update(label="Research Agent complete", state="complete")
        st.markdown("### Research output")
        for item in step_outputs:
            with st.expander(item["label"], expanded=item["label"].endswith("synthesis")):
                st.markdown(item["text"] or "No text response returned.")
                if item["data"] is not None:
                    st.dataframe(item["data"], use_container_width=True)
                for sql in item["sql"]:
                    st.code(sql, language="sql", wrap_lines=True)

        st.success("Reasoning steps completed: 4/4")


def _render_question_chips(mode: str):
    questions = CHAT_SAMPLE_QUESTIONS if mode == "Chat" else RESEARCH_SAMPLE_QUESTIONS
    cols = st.columns(2)
    for idx, question in enumerate(questions):
        with cols[idx % 2]:
            if st.button(question, use_container_width=True, key=f"sample_q_{mode}_{idx}"):
                st.session_state.genie_prefill_prompt = question
                st.rerun()


def render() -> None:
    freshness.render_freshness_bar()
    cfg = st.session_state.cfg
    st.title("Genie Space")

    if "genie_conversation_id" not in st.session_state:
        st.session_state.genie_conversation_id = None
    if "genie_prefill_prompt" not in st.session_state:
        st.session_state.genie_prefill_prompt = ""

    if cfg.genie_space_id in {"", "__AUTO__"}:
        st.warning("GENIE_SPACE_ID is not configured. Set `APP_GENIE_SPACE_ID` to enable this page.")
        return

    if not cfg.workspace_host or not cfg.token:
        st.error("Workspace host and a valid token must be configured for Genie.")
        return

    mode_col, reset_col = st.columns([4, 1])
    with mode_col:
        mode = st.radio(
            "Mode",
            options=["Chat", "Research Agent"],
            horizontal=True,
            label_visibility="collapsed",
            help="Chat: direct answer. Research Agent: multi-step investigation and synthesis.",
        )
    with reset_col:
        if st.button("Reset conversation"):
            st.session_state.genie_conversation_id = None
            st.session_state.genie_prefill_prompt = ""
            st.rerun()

    _render_question_chips(mode)
    st.markdown("---")

    prompt = st.chat_input("Ask about flow-break risk, anomalies, latency, or machine health...")

    effective_prompt = prompt or st.session_state.genie_prefill_prompt
    if effective_prompt:
        st.session_state.genie_prefill_prompt = ""
        if mode == "Chat":
            _render_chat_mode(effective_prompt)
        else:
            _render_research_mode(effective_prompt)
