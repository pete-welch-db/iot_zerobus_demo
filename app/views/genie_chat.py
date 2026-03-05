import streamlit as st

from genie_client import GenieClient


DEFAULT_PROMPTS = [
    "Which machine has the highest flow-break risk right now and why?",
    "Show machines with prob_fault_next_5m above 0.5 and summarize primary signals.",
    "What changed in the last 15 minutes for the top risk machine?",
]


def render(genie: GenieClient) -> None:
    st.subheader("Genie Assistant")
    configured = (
        bool(genie.config.workspace_host)
        and bool(genie.config.token)
        and genie.config.genie_space_id not in {"", "__AUTO__"}
    )
    if configured:
        st.caption(f"Connected to Genie space `{genie.config.genie_space_id}`.")
    else:
        st.warning(
            "Genie is not fully configured. Set `APP_GENIE_SPACE_ID` for this app environment."
        )
    if "genie_messages" not in st.session_state:
        st.session_state.genie_messages = []
    if "genie_conversation_id" not in st.session_state:
        st.session_state.genie_conversation_id = None

    for msg in st.session_state.genie_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    with st.expander("Suggested prompts", expanded=False):
        for prompt in DEFAULT_PROMPTS:
            if st.button(prompt, key=f"p_{prompt}"):
                st.session_state.genie_messages.append({"role": "user", "content": prompt})
                result = genie.ask(prompt, conversation_id=st.session_state.genie_conversation_id)
                if result.get("conversation_id"):
                    st.session_state.genie_conversation_id = result.get("conversation_id")
                content = result.get("answer") if result.get("ok") else f"Genie error: {result.get('error')}"
                st.session_state.genie_messages.append({"role": "assistant", "content": str(content)})
                st.rerun()

    user_input = st.chat_input("Ask Genie about flow-break risk, anomalies, or freshness...")
    if not user_input:
        return

    st.session_state.genie_messages.append({"role": "user", "content": user_input})
    result = genie.ask(user_input, conversation_id=st.session_state.genie_conversation_id)
    if result.get("conversation_id"):
        st.session_state.genie_conversation_id = result.get("conversation_id")
    content = result.get("answer") if result.get("ok") else f"Genie error: {result.get('error')}"
    st.session_state.genie_messages.append({"role": "assistant", "content": str(content)})
    st.rerun()
