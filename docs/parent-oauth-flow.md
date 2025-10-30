# Parent-managed OAuth flow (Track B)

This document explains how the Parent-managed OAuth flow works in the `feat/oauth-parent` branch and how to configure it for Snowflake OAuth and for External OAuth providers (Azure Entra ID / Okta).

- Parent: a minimal HTML page (`parent/index.html`) that performs the Authorization Code + PKCE redirect and hands the authorization code and PKCE verifier to the child via iframe URL parameters.
- Child: the Streamlit app (`app.py`) that exchanges the code server-side, stores the token in session, auto-refreshes it, and calls the Snowflake Cortex Agent API using the access token.

## Why Parent-managed?
- Browser CORS prevents a static parent from calling many IdP token endpoints. The server-side exchange is performed in the Streamlit child to avoid CORS.
- The Streamlit app is embedded, so all identity UX (login) is owned by the parent app. The child only consumes tokens.

## High-level sequence

```mermaid
sequenceDiagram
  autonumber
  participant U as User
  participant P as Parent (index.html)
  participant IdP as IdP (Snowflake/Entra/Okta)
  participant C as Child (Streamlit)

  U->>P: Click parent Sign in or child Connect to Snowflake
  P->>P: Generate PKCE verifier and challenge
  P->>P: Save verifier in localStorage
  P->>IdP: GET /authorize (with PKCE)
  IdP-->>P: Redirect back with code and state
  P->>C: Load iframe URL with p_code, p_cv (verifier), p_cid, p_ruri, p_acc
  C->>IdP: POST token_endpoint { grant_type=authorization_code, code, code_verifier, client_id, redirect_uri }
  IdP-->>C: { access_token, refresh_token, expires_in, scope }
  C->>C: Save { token, refresh_token, expires_at, scope, last_refresh }
  C-->>U: Child authenticated; scope and expiry shown
  C->>IdP: POST token_endpoint { grant_type=refresh_token, refresh_token }
  IdP-->>C: { access_token, refresh_token?, expires_in, scope? }
  C->>C: Update session; rerun

  U->>C: Click child Sign out
  C->>C: Clear token state and rerun (no parent sign-out)
```

## Parent behavior (parent/index.html)
- Generates PKCE values, builds the authorize URL and redirects the top window.
- On callback (`?code=...`), loads the child iframe with query parameters so the child can exchange the code server-side.
- Default Snowflake endpoints are used when `authEndpoint`/`tokenEndpoint` are not provided.
- Silent reconnect on refresh is disabled by default because many Snowflake PUBLIC clients re‑prompt consent on each refresh; use the explicit Sign in button instead.
- Listens for child events via `postMessage`:
  - `parent:reconnect`: start a normal interactive authorize flow (same as Sign in).

### Optional: enable silent reconnect (prompt=none)
If your IdP supports silent SSO, you can opt in to a single `prompt=none` attempt on page load. Add `enableSilentReconnect: true` to `cfg` and insert a once-per-load block that builds the authorize URL with `prompt=none`. If the IdP returns an interaction-required error, the page falls back to the normal interactive Sign in.

Config fields in `cfg`:
- `accountUrl`: Snowflake account URL (no path, no trailing slash) when using Snowflake OAuth defaults
- `clientId`: OAuth client id (from Snowflake SECURITY INTEGRATION or your IdP app)
- `redirectUri`: the parent callback; must be registered with your IdP
- `scope`: requested scope string
- `authEndpoint?`: explicit authorize endpoint for External OAuth (Entra/Okta)
- `tokenEndpoint?`: explicit token endpoint for External OAuth (Entra/Okta)
- `childUrl`: URL of the Streamlit child (iframe src)

Authorize URL the parent builds (default Snowflake):
```
<accountUrl>/oauth/authorize?response_type=code&client_id=<id>&redirect_uri=<uri>
  &scope=<scope>&code_challenge=<S256>&code_challenge_method=S256&state=<uuid>
```

## Child behavior (Streamlit app.py)
- On first load with `p_code` + `p_cv` (+ optional `p_cid`, `p_ruri`, `p_acc`), performs the server-side token exchange against the configured token endpoint.
- Stores in `st.session_state`:
  - `parent_token`, `parent_refresh_token`, `parent_token_expires`
  - `parent_scope`, `parent_last_refresh`
- Auto refresh:
  - Time-based: if `expires_at - now < 120s`, attempt one refresh
  - Error-based: on a 401 from the Agents API, attempt one refresh
- UI shows:
  - Scope, token expiry countdown, and last refreshed time in the sidebar
  - When unauthenticated, a single centered primary button “Connect to Snowflake” that asks the parent to begin OAuth
- Child “Sign out” clears only the child session state (no parent sign-out).

Server-side token calls (performed by the child):
- Authorization code exchange
```
POST <token_endpoint>
  grant_type=authorization_code
  code=<p_code>
  redirect_uri=<p_ruri>
  client_id=<p_cid or OAUTH_CLIENT_ID>
  code_verifier=<p_cv>
```
- Refresh
```
POST <token_endpoint>
  grant_type=refresh_token
  refresh_token=<child session>
  client_id=<OAUTH_CLIENT_ID>
```

## Query parameter contract (Parent → Child)
- `p_code`: authorization code from IdP callback
- `p_cv`: PKCE code verifier generated by the parent
- `p_cid`: client id to use during exchange (optional if child secrets provide it)
- `p_ruri`: redirect URI used with the IdP (echoed for the exchange)
- `p_acc`: account base URL (Snowflake default path helper when token endpoint not provided)
- `p_signout`: when `1`, instructs the child to clear session

## Configuration

Child (`.streamlit/secrets.toml`):
```
SNOWFLAKE_ACCOUNT_URL = "https://<account>.snowflakecomputing.com"
# For Snowflake OAuth (defaults to <account>/oauth/token-request if omitted)
# OAUTH_TOKEN_ENDPOINT = "https://<account>.snowflakecomputing.com/oauth/token-request"
# For External OAuth (Entra/Okta), set an explicit token endpoint and scope
# OAUTH_TOKEN_ENDPOINT = "https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token"
# OAUTH_CLIENT_ID = "<CLIENT_ID>"
# OAUTH_SCOPE = "api://<audience>/.default"
```

Parent (`parent/index.html`) configuration examples are already in the README for Snowflake, Entra, and Okta. Use explicit `authEndpoint`/`tokenEndpoint` for External OAuth.

## Error handling & troubleshooting
- Repeated login prompts after refresh: Your IdP may not honor `prompt=none` for this client. We disable silent reconnect by default and rely on an explicit Sign in.
- 401 from Agents API: The child attempts a one-time refresh; if it fails, use the child’s “Connect to Snowflake” to re-authenticate.
- 404 when using Cortex Search: Ensure the session role has grants on the referenced service/schema/DB and that the OAuth scope matches a role with those grants.
- Invalid redirect URI: For local dev, Snowflake requires `oauth_allow_non_tls_redirect_uri=true` in the `SECURITY INTEGRATION` when using HTTP redirect URIs.

## Security notes
- Access/refresh tokens never live in parent JavaScript; only the authorization code and PKCE verifier are passed to the child. The child performs the exchange server-side to avoid CORS and protect tokens.
- Use HTTPS in production for parent and child redirect URIs.
- Prefer short-lived access tokens and enable refresh tokens where appropriate.

## Local development quickstart
- Child
```
python -m pip install -r requirements.txt
streamlit run app.py
```
- Parent
```
python -m http.server 8000
# Open http://localhost:8000/parent/index.html
```
