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

## Parent-managed OAuth (Track B: `feat/oauth-parent`)

This branch removes PAT and in-app OAuth. A tiny parent page performs OAuth and hands the authorization code to the Streamlit child. The child exchanges the code server‑side and manages refresh. Defaults are tailored for Snowflake OAuth; External OAuth examples (Entra/Okta) are below.

- Parent page: `parent/index.html`
  - PUBLIC + PKCE client (Snowflake OAuth for proto; Entra/Okta for External OAuth later)
  - After callback, loads the iframe with query params: `p_code`, `p_cv`, `p_cid`, `p_ruri`, `p_acc`
  - One‑time silent reconnect attempt with `prompt=none` (if the IdP permits). Falls back to normal “Sign in”.
  - “Sign out” also signs out the child (loads iframe with `p_signout=1`).

- Child (Streamlit `app.py`):
  - On first load with code: server‑side POST to `<account>/oauth/token-request` using `grant_type=authorization_code` and PKCE verifier
  - Stores `access_token`, `refresh_token`, `expires_at`, and `scope`
  - Auto‑refresh: at T−120s and on 401 once (`grant_type=refresh_token`)
  - Sidebar shows: “Authenticated via Parent OAuth”, `Scope: …`, and “Token expires in …”

Child secrets — Snowflake OAuth (defaults):

```toml
# .streamlit/secrets.toml
SNOWFLAKE_ACCOUNT_URL = "https://<account>.snowflakecomputing.com"
# Optional (explicit) — the child will default to this if omitted
OAUTH_TOKEN_ENDPOINT = "https://<account>.snowflakecomputing.com/oauth/token-request"
# Optional
# OAUTH_CLIENT_ID = "<OAUTH_CLIENT_ID>"
# OAUTH_SCOPE = "SESSION:ROLE:<ROLE_NAME>"
```

Run the parent locally:

```bash
python -m http.server 8000
# then open http://localhost:8000/parent/index.html
```

Snowflake OAuth setup (PUBLIC + PKCE, dev):

```sql
use role accountadmin;
create or replace security integration OAUTH_PUB_PKCE
  type = oauth
  enabled = true
  oauth_client = custom
  oauth_client_type = 'PUBLIC'
  oauth_redirect_uri = 'http://localhost:8000/parent/index.html'
  oauth_allow_non_tls_redirect_uri = true
  oauth_enforce_pkce = true
  oauth_issue_refresh_tokens = true
  oauth_refresh_token_validity = 86400;

desc security integration OAUTH_PUB_PKCE; -- copy OAUTH_CLIENT_ID
```

Parent config (top of `parent/index.html`) — Snowflake OAuth defaults:

```js
const cfg = {
  accountUrl: 'https://<account>.snowflakecomputing.com',
  clientId: '<OAUTH_CLIENT_ID>',
  redirectUri: 'http://localhost:8000/parent/index.html',
  scope: 'SESSION:ROLE:<ROLE_NAME>'
};
```

External OAuth (Entra/Okta):
- Replace authorize and token endpoints per your IdP. The parent supports a one‑time silent reconnect (`prompt=none`); if your tenant disallows it, it falls back to interactive login.

Entra ID example:

Parent (`parent/index.html`):
```js
const cfg = {
  authEndpoint: 'https://login.microsoftonline.com/<tenant>/oauth2/v2.0/authorize',
  tokenEndpoint: 'https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token',
  clientId: '<CLIENT_ID>',
  redirectUri: 'http://localhost:8000/parent/index.html',
  scope: 'api://<external-oauth-audience>/.default' // or custom scopes
};
```

Child (`.streamlit/secrets.toml`):
```toml
SNOWFLAKE_ACCOUNT_URL = "https://<account>.snowflakecomputing.com"
OAUTH_TOKEN_ENDPOINT = "https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token"
OAUTH_CLIENT_ID = "<CLIENT_ID>"
OAUTH_SCOPE = "api://<external-oauth-audience>/.default"
```

Okta example:

Parent (`parent/index.html`):
```js
const cfg = {
  authEndpoint: 'https://<yourOktaDomain>/oauth2/<authServerId>/v1/authorize',
  tokenEndpoint: 'https://<yourOktaDomain>/oauth2/<authServerId>/v1/token',
  clientId: '<CLIENT_ID>',
  redirectUri: 'http://localhost:8000/parent/index.html',
  scope: 'api://<external-oauth-audience>/.default' // or okta-specific scopes
};
```

Child (`.streamlit/secrets.toml`):
```toml
SNOWFLAKE_ACCOUNT_URL = "https://<account>.snowflakecomputing.com"
OAUTH_TOKEN_ENDPOINT = "https://<yourOktaDomain>/oauth2/<authServerId>/v1/token"
OAUTH_CLIENT_ID = "<CLIENT_ID>"
OAUTH_SCOPE = "api://<external-oauth-audience>/.default"
```

Threads:
- Use the sidebar to list, load, or create threads. Conversations will use the selected thread when both `thread_id` and `parent_message_id` are provided.
