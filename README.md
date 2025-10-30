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

```toml
# .streamlit/secrets.toml
SNOWFLAKE_ACCOUNT_URL = "https://<account>.snowflakecomputing.com"
```

## Parent-managed OAuth (Track B branch: feat/oauth-parent)

In this branch, the Streamlit app does not handle OAuth or PAT. A parent page performs OAuth, stores tokens in localStorage, and posts an access token to the Streamlit child via postMessage.

- Parent page: `parent/index.html`
  - Prototype issuer: Snowflake OAuth (PUBLIC + PKCE)
  - Stores `{ access_token, refresh_token, expires_at }` in localStorage
  - Posts `{ type: 'auth:token', access_token, expires_at }` to the child origin
  - Refreshes token via `grant_type=refresh_token` and re-posts

- Child (Streamlit):
  - Requires token from parent; shows “Waiting for token from parent…” until received
  - Uses the token for Agents API calls
  - Sidebar displays token expiry

Secrets for the child:

```toml
# .streamlit/secrets.toml
SNOWFLAKE_ACCOUNT_URL = "https://<account>.snowflakecomputing.com"
ALLOWED_PARENT_ORIGINS = ["http://localhost:8000"]
```

Running the parent page locally:

```bash
# any simple static server from the project root
python -m http.server 8000
# open http://localhost:8000/parent/index.html
```

Notes:
- Swap the parent issuer from Snowflake OAuth to Entra/Okta later without changing the child.
- Avoid logging tokens. The prototype uses localStorage for simplicity.

Threads:
- Use the sidebar to list, load, or create threads. Conversations will use the selected thread when both `thread_id` and `parent_message_id` are provided.
