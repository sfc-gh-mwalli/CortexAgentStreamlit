# Snowflake Cortex Agent Streamlit UI

## Screenshot

![UI Screenshot](assets/ui-screenshot.png)

Run locally:

```bash
cd /Users/mwalli/Documents/Projects/Cursor/CortexAgentStreamlit
python -m pip install -r requirements.txt
streamlit run app.py
```

Configuration (via `st.secrets`):
Create `.streamlit/secrets.toml` with your connection details:

```toml
# .streamlit/secrets.toml
SNOWFLAKE_ACCOUNT_URL = "https://<account>.snowflakecomputing.com"
SNOWFLAKE_AUTH_TOKEN = "<bearer_token>"
[snowflake]
account_url = "https://<account>.snowflakecomputing.com"
auth_token = "<bearer_token>"
```

Threads:
- Use the sidebar to list, load, or create threads. Conversations will use the selected thread when both `thread_id` and `parent_message_id` are provided.
