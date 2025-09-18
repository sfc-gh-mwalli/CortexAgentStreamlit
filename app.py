import time
import json
from typing import Dict, Any, List
from datetime import datetime

import streamlit as st

from snowflake_cortex_agent_client import SnowflakeCortexAgentClient


st.set_page_config(page_title="Snowflake Cortex Agent Chat", page_icon="❄️", layout="wide")


def ensure_session_state() -> None:
    if "thread_id" not in st.session_state:
        st.session_state.thread_id = None
    if "parent_message_id" not in st.session_state:
        st.session_state.parent_message_id = None
    if "messages" not in st.session_state:
        st.session_state.messages: List[Dict[str, Any]] = []
    if "loaded_thread_id" not in st.session_state:
        st.session_state.loaded_thread_id = None
    # Always show detailed events now


def sidebar_threads(client) -> None:
    st.sidebar.header("Threads")
    # List all threads across applications so user can discover app names
    threads = client.list_threads(limit=50)
    thread_options: List[Dict[str, str]] = []
    for t in threads:
        if not isinstance(t, dict):
            continue
        created_on = t.get('created_on')
        origin_app = t.get('origin_application') or ''
        created_str = ""
        if isinstance(created_on, (int, float)):
            try:
                created_str = datetime.fromtimestamp(created_on / 1000).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                created_str = str(created_on)
        label_core = created_str if created_str else ""
        label = f"{label_core} — {origin_app}".strip(" — ") if origin_app else label_core
        thread_options.append({
            "label": label,
            "value": str(t.get('thread_id')),
        })
    if not threads:
        st.sidebar.info("No threads found. Create one below.")

    selected = st.sidebar.selectbox(
        "Select a thread",
        options=[opt["value"] for opt in thread_options] if thread_options else [""],
        format_func=lambda v: next((o["label"] for o in thread_options if o["value"] == v), "(none)"),
        index=0 if thread_options else 0,
    )
    if selected and selected != st.session_state.loaded_thread_id:
        # Auto-load when selection changes
        st.session_state.thread_id = selected
        st.session_state.parent_message_id = "0"  # default; update from history below
        desc = client.describe_thread(selected, page_size=50)
        rendered: List[Dict[str, Any]] = []
        if isinstance(desc, dict):
            msgs = desc.get('messages')
            if isinstance(msgs, list):
                msgs_sorted = list(reversed(msgs))
                for m in msgs_sorted:
                    if not isinstance(m, dict):
                        continue
                    role = m.get('role', 'assistant')
                    payload = m.get('message_payload')
                    text_parts: List[str] = []
                    kept_blocks: List[Dict[str, Any]] = []
                    if isinstance(payload, str):
                        parsed = None
                        if payload.lstrip().startswith(('{', '[')):
                            try:
                                import json  # local import to avoid top-level changes
                                parsed = json.loads(payload)
                            except Exception:
                                parsed = None
                        if isinstance(parsed, dict):
                            role = parsed.get('role', role)
                            content_blocks = parsed.get('content')
                            if isinstance(content_blocks, list):
                                for block in content_blocks:
                                    if not isinstance(block, dict):
                                        continue
                                    btype = block.get('type')
                                    if btype == 'thinking':
                                        continue
                                    if btype == 'text' and isinstance(block.get('text'), str):
                                        text_parts.append(block['text'])
                                        kept_blocks.append({"type": "text", "text": block['text']})
                                    elif btype == 'chart':
                                        # Normalize chart block to carry chart_spec string if nested
                                        norm_chart = {"type": "chart"}
                                        if isinstance(block.get('chart_spec'), str):
                                            norm_chart['chart_spec'] = block['chart_spec']
                                        elif isinstance(block.get('chart'), dict) and isinstance(block['chart'].get('chart_spec'), str):
                                            norm_chart['chart_spec'] = block['chart']['chart_spec']
                                        elif isinstance(block.get('json'), dict) and isinstance(block['json'].get('chart_spec'), str):
                                            norm_chart['chart_spec'] = block['json']['chart_spec']
                                        kept_blocks.append(norm_chart)
                                    elif btype == 'table':
                                        kept_blocks.append(block)
                            if not text_parts and not kept_blocks:
                                text_parts.append(payload)
                        else:
                            text_parts.append(payload)
                        if kept_blocks:
                            rendered.append({"role": role, "content": kept_blocks})
                        else:
                            rendered.append({
                                "role": role,
                                "content": [{"type": "text", "text": "".join(text_parts)}],
                            })
        st.session_state.messages = rendered
        # Set parent_message_id to latest message id to maintain context
        try:
            if isinstance(desc, dict) and isinstance(desc.get('messages'), list) and desc['messages']:
                latest = desc['messages'][0]  # original order is newest-first per docs
                mid = latest.get('message_id')
                if mid is not None:
                    st.session_state.parent_message_id = str(mid)
        except Exception:
            pass
        st.session_state.loaded_thread_id = selected

    # Inline thread actions under dropdown
    col_new, col_del = st.sidebar.columns(2)
    with col_new:
        if st.button("New thread"):
            # Use origin app name from session (set in Connection section)
            current_origin = st.session_state.get("origin_application", "demo")
            new_id = client.create_thread(current_origin)
            if new_id:
                st.success(f"Created thread {new_id}")
                st.session_state.thread_id = str(new_id)
                st.session_state.parent_message_id = "0"
                st.session_state.messages = []
                st.session_state.loaded_thread_id = str(new_id)
            else:
                st.error("Failed to create thread")
    with col_del:
        if st.session_state.thread_id and st.button("Delete current thread"):
            ok = client.delete_thread(st.session_state.thread_id)
            if ok:
                st.success("Thread deleted")
                st.session_state.thread_id = None
                st.session_state.parent_message_id = None
                st.session_state.messages = []
                st.session_state.loaded_thread_id = None
                # Refresh sidebar threads list
                st.rerun()
            else:
                st.error("Failed to delete thread")

    # View options removed; detailed events always displayed


def _get_secret(key: str, fallback: str = "") -> str:
    try:
        # Try top-level secrets
        if key in st.secrets:
            return str(st.secrets.get(key, fallback)).strip()
        # Try nested under 'snowflake'
        sf = st.secrets.get("snowflake", {})
        if isinstance(sf, dict) and key.lower() in sf:
            return str(sf.get(key.lower(), fallback)).strip()
    except Exception:
        pass
    return fallback


def main() -> None:
    ensure_session_state()

    st.title("Cortex HCLS Agent")
    st.caption("Snowflake Agent REST API")
    # Light styling for a less boring UI
    st.markdown(
        """
        <style>
        .thinking { color: #a855f7; }
        .status-line { color: #0ea5e9; font-size: 0.9rem; }
        .notice { color: #64748b; font-size: 0.9rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Read connection strictly from secrets
    account_url = _get_secret("SNOWFLAKE_ACCOUNT_URL", "")
    auth_token = _get_secret("SNOWFLAKE_AUTH_TOKEN", "")
    db = _get_secret("SNOWFLAKE_AGENT_DATABASE", "SNOWFLAKE_INTELLIGENCE")
    schema = _get_secret("SNOWFLAKE_AGENT_SCHEMA", "AGENTS")
    agent = _get_secret("SNOWFLAKE_AGENT_NAME", "HCLS_AGENT")
    # Fixed origin application name for thread creation (from secrets or default)
    st.session_state["origin_application"] = _get_secret("SNOWFLAKE_ORIGIN_APPLICATION", "hcls_agent_st")

    # Build client
    if not account_url or not auth_token:
        st.error("Missing connection secrets. Set SNOWFLAKE_ACCOUNT_URL and SNOWFLAKE_AUTH_TOKEN in st.secrets.")
        return
    client = SnowflakeCortexAgentClient(account_url=account_url, auth_token=auth_token)

    sidebar_threads(client)

    # Welcome message (first load or after new thread creation when no messages yet)
    if not st.session_state.messages:
        st.markdown(
            """
Hello! I'm a healthcare data analyst with access to comprehensive healthcare data and several powerful tools to help you analyze patient information, clinical outcomes, and healthcare operations.

How can I help you?
            """
        )

    # Chat input
    user_text = st.chat_input("Type your message...")
    # Render existing history if loaded
    for msg in st.session_state.messages:
        role = msg.get("role", "assistant")
        with st.chat_message("user" if role == "user" else "assistant", avatar=("👤" if role == "user" else "❄️")):
            # Extract text blocks
            parts = []
            for c in msg.get("content", []):
                if not isinstance(c, dict):
                    continue
                btype = c.get("type")
                if btype == "text" and isinstance(c.get("text"), str):
                    parts.append(c["text"])
                elif btype == "chart":
                    spec = None
                    # Accept chart_spec string directly
                    if isinstance(c.get("chart_spec"), str):
                        try:
                            spec = json.loads(c["chart_spec"])  # type: ignore[name-defined]
                        except Exception:
                            spec = None
                    # Or nested objects
                    if spec is None and isinstance(c.get("chart"), dict):
                        chart_dict = c.get("chart")
                        if chart_dict.get('chart_spec') and isinstance(chart_dict.get('chart_spec'), str):
                            try:
                                spec = json.loads(chart_dict['chart_spec'])
                            except Exception:
                                spec = None
                        elif chart_dict.get('mark') or chart_dict.get('encoding') or chart_dict.get('$schema'):
                            spec = chart_dict
                    if spec is None and isinstance(c.get("json"), dict) and isinstance(c['json'].get('chart_spec'), str):
                        try:
                            spec = json.loads(c['json']['chart_spec'])
                        except Exception:
                            spec = None
                    if isinstance(spec, dict):
                        data_values = []
                        d = spec.get('data')
                        if isinstance(d, dict):
                            data_values = d.get('values', [])
                        st.vega_lite_chart(data_values, spec=spec, use_container_width=True)
                elif btype == "table":
                    rows = c.get('rows') or c.get('data')
                    try:
                        if isinstance(rows, list):
                            st.dataframe(rows, use_container_width=True)
                        else:
                            st.json(c)
                    except Exception:
                        st.json(c)
            if parts:
                st.write("".join(parts))
    if user_text:
        st.session_state.messages.append({
            "role": "user",
            "content": [{"type": "text", "text": user_text}],
        })

        # Render user bubble
        with st.chat_message("user", avatar="👤"):
            st.write(user_text)

        # Decide payload messages per Snowflake API rules
        if st.session_state.thread_id is not None and st.session_state.parent_message_id is not None:
            sending_messages = [st.session_state.messages[-1]]  # exactly one user message
        else:
            sending_messages = st.session_state.messages

        # Stream assistant response
        assistant_container = st.empty()
        status_container = st.empty()
        thinking_container = st.empty()
        details_container = st.container()
        visuals_container = st.container()
        last_status = None
        steps: List[str] = []
        start_time = time.time()
        last_event_time = start_time
        last_render_update = 0
        # Heartbeat counters (UI-driven, independent of server events)
        heartbeat_ticks = 0
        last_heartbeat_tick = start_time
        thinking_buffer: List[str] = []
        full_text = []
        thinking_text = None
        for chunk in client.run_agent_stream(
            database=db,
            schema=schema,
            agent_name=agent,
            messages=sending_messages,
            thread_id=st.session_state.thread_id,
            parent_message_id=st.session_state.parent_message_id,
            tool_choice={"type": "auto"},
            yield_events=True,
        ):
            if chunk.get("type") == "event":
                ev = chunk["event"]
                if isinstance(ev, dict):
                    etype = ev.get("event")
                    if etype == "response.thinking":
                        # Final consolidated thinking text
                        thinking_text = ev.get("text")
                        if isinstance(thinking_text, str):
                            thinking_buffer = [thinking_text]
                        if thinking_buffer:
                            thinking_container.markdown(
                                f"<div class='notice'><strong>Thinking</strong></div><div class='status-line'>{''.join(thinking_buffer)}</div>",
                                unsafe_allow_html=True,
                            )
                        last_event_time = time.time()
                    elif etype == "response.status":
                        msg = ev.get("message")
                        if isinstance(msg, str):
                            last_status = msg
                            last_event_time = time.time()
                    elif etype == "response.text.annotation":
                        # Render annotations as they arrive
                        with details_container:
                            st.markdown("<div class='notice'><strong>Annotations</strong></div>", unsafe_allow_html=True)
                            st.json(ev)
                    elif etype == "response.thinking.delta":
                        # Streaming thinking deltas per docs
                        delta = ev.get("text")
                        if isinstance(delta, str):
                            thinking_buffer.append(delta)
                            thinking_container.markdown(
                                f"<div class='notice'><strong>Thinking</strong></div><div class='status-line'>{''.join(thinking_buffer)}</div>",
                                unsafe_allow_html=True,
                            )
                            last_event_time = time.time()
                    elif etype == "response.tool_use":
                         # Show which tool is being used
                        ttype = ev.get("type") or ev.get("tool_type")
                        tname = ev.get("name")
                        if isinstance(ttype, str) or isinstance(tname, str):
                            steps.append(f"Using tool: {ttype or ''} {tname or ''}".strip())
                            last_event_time = time.time()
                    elif etype == "response.tool_result.status":
                        status = ev.get("status") or ev.get("message")
                        if isinstance(status, str):
                            steps.append(f"Tool result: {status}")
                            last_event_time = time.time()
                    elif etype in ("response.table", "response.chart"):
                        steps.append("Rendering " + ("table" if etype.endswith("table") else "chart"))
                        last_event_time = time.time()
                        with visuals_container:
                            if etype.endswith("table"):
                                st.markdown("<div class='notice'><strong>Table</strong></div>", unsafe_allow_html=True)
                                data_obj = ev.get('table') or ev.get('json') or ev.get('content') or ev
                                try:
                                    if isinstance(data_obj, dict) and isinstance(data_obj.get('rows'), list):
                                        st.dataframe(data_obj.get('rows'), use_container_width=True)
                                    elif isinstance(data_obj, list):
                                        st.dataframe(data_obj, use_container_width=True)
                                    else:
                                        st.json(ev)
                                except Exception:
                                    st.json(ev)
                            else:  # chart
                                st.markdown("<div class='notice'><strong>Chart</strong></div>", unsafe_allow_html=True)
                                # Best-effort Vega-Lite rendering if possible
                                try:
                                    spec = None
                                    chart_obj = ev.get('chart')
                                    # Case 1: top-level chart_spec string
                                    if isinstance(ev.get('chart_spec'), str):
                                        try:
                                            spec = json.loads(ev['chart_spec'])
                                        except Exception:
                                            spec = None
                                    # Case 2: nested chart with chart_spec string or spec dict
                                    if spec is None and isinstance(chart_obj, dict):
                                        if isinstance(chart_obj.get('chart_spec'), str):
                                            try:
                                                spec = json.loads(chart_obj['chart_spec'])
                                            except Exception:
                                                spec = None
                                        elif (chart_obj.get('mark') or chart_obj.get('encoding') or chart_obj.get('$schema')):
                                            spec = chart_obj
                                    # Case 3: raw spec dict provided directly in event json
                                    if spec is None and isinstance(ev.get('json'), dict):
                                        j = ev['json']
                                        if j.get('mark') or j.get('encoding') or j.get('$schema'):
                                            spec = j
                                    # Render if we have a plausible spec
                                    if isinstance(spec, dict) and (spec.get('mark') or spec.get('encoding') or spec.get('$schema')):
                                        data_values = []
                                        data_field = spec.get('data')
                                        if isinstance(data_field, dict):
                                            data_values = data_field.get('values', [])
                                        st.vega_lite_chart(data_values, spec=spec, use_container_width=True)
                                    else:
                                        st.json(ev)
                                except Exception:
                                    st.json(ev)

                    # Agent instructions and execution environment (if surfaced)
                    ai = ev.get('agent_instructions') or ev.get('instructions')
                    if ai is not None:
                        with details_container:
                            st.markdown("<div class='notice'><strong>Agent Instructions</strong></div>", unsafe_allow_html=True)
                            st.json(ai)
                    exenv = ev.get('execution_environment')
                    if exenv is None and isinstance(ev.get('json'), dict):
                        exenv = ev['json'].get('execution_environment')
                    if isinstance(exenv, dict):
                        with details_container:
                            st.markdown("<div class='notice'><strong>Execution Environment</strong></div>", unsafe_allow_html=True)
                            st.json(exenv)

                    # Model configuration (if present in event)
                    model_cfg = ev.get('models') or ev.get('model_config')
                    if model_cfg is None and isinstance(ev.get('json'), dict):
                        model_cfg = ev['json'].get('models') or ev['json'].get('model_config')
                    if model_cfg is not None:
                        with details_container:
                            st.markdown("<div class='notice'><strong>Model Config</strong></div>", unsafe_allow_html=True)
                            st.json(model_cfg)

                    # Render compact status with elapsed/idle; throttle UI updates to ~2/s
                    now = time.time()
                    # Increment heartbeat once per second regardless of event flow
                    if now - last_heartbeat_tick >= 1:
                        heartbeat_ticks += int((now - last_heartbeat_tick))
                        last_heartbeat_tick = now
                    if now - last_render_update > 0.5:
                        elapsed = int(now - start_time)
                        lines: List[str] = []
                        if last_status:
                            lines.append(f"Status: {last_status}")
                        if steps:
                            for s in steps[-5:]:
                                lines.append(s)
                        lines.append(f"Elapsed: {elapsed}s (heartbeat {heartbeat_ticks}s)")
                        status_container.markdown("<br>".join([f"<div class='status-line'>{l}</div>" for l in lines]), unsafe_allow_html=True)
                        last_render_update = now
                    # You can add more statuses here (tool_use/tool_result)
            elif chunk.get("type") == "content":
                # Suppress deltas for a clean UI
                pass
            elif chunk.get("type") == "final":
                full_text.append(chunk["content"])
                with assistant_container.container():
                    with st.chat_message("assistant", avatar="❄️"):
                        st.write("".join(full_text))
                status_container.empty()
                # Clear thinking when answer is finalized
                thinking_container.empty()
                # Update parent_message_id to latest message in thread for follow-ups
                try:
                    if st.session_state.thread_id:
                        latest_desc = client.describe_thread(st.session_state.thread_id, page_size=1)
                        if isinstance(latest_desc, dict) and isinstance(latest_desc.get('messages'), list) and latest_desc['messages']:
                            latest = latest_desc['messages'][0]
                            mid = latest.get('message_id')
                            if mid is not None:
                                st.session_state.parent_message_id = str(mid)
                except Exception:
                    pass
            elif chunk.get("type") == "error":
                with assistant_container.container():
                    st.error(chunk.get("error"))
                break

            # Detect stalled stream: no events for N seconds
            now2 = time.time()
            idle_gap = now2 - last_event_time
            if idle_gap > 30 and idle_gap <= 60:
                status_container.markdown("<div class='status-line'>Warning: no updates for 30s, stream may be stalled…</div>", unsafe_allow_html=True)
            if idle_gap > 60:
                # Auto-refresh the currently selected thread to recover from a stalled stream
                status_container.markdown("<div class='status-line'>No updates for 60s. Reloading thread…</div>", unsafe_allow_html=True)
                try:
                    # Force the sidebar auto-load logic to reload this thread on rerun
                    st.session_state.loaded_thread_id = None
                except Exception:
                    pass
                st.rerun()


if __name__ == "__main__":
    main()
