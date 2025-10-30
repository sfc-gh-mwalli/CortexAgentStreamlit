<!-- 7ab789e5-43b2-48f0-9ee9-a4acd339b7ef 34cb6530-927d-4998-a55b-5160a3a437e5 -->
# Parent-Managed OAuth Flow Documentation

## What we’ll add
- A new deep-dive doc explaining the Parent → Child (Streamlit) OAuth flow for Track B, covering Snowflake OAuth defaults and External OAuth (Entra/Okta) variants.
- A short README link pointing to the deep-dive doc.

## Deliverables
- docs/parent-oauth-flow.md
  - Purpose and scope
  - High-level overview of actors and roles
  - Sequence diagram (Mermaid) for the full authorize → code handoff → server-side exchange → refresh → sign-out flows
  - Parent behavior
    - PKCE generation
    - Authorize URL build
    - Callback handling
    - Iframe URL handoff with p_code/p_cv/p_cid/p_ruri/p_acc
    - Disabled silent reconnect (why) and when to enable
    - Sign-in and sign-out effects
  - Child behavior
    - Server-side token exchange (CORS rationale)
    - Session state keys (token, refresh_token, expires, scope, last_refresh)
    - Auto-refresh policy (T-120s and 401)
    - Displayed status (scope, token expiry, last refreshed)
    - Child-only sign out
  - Configuration
    - Parent: cfg fields; Snowflake defaults; how to switch to Entra/Okta (auth/token endpoints, scope, clientId, redirectUri)
    - Child: .streamlit/secrets.toml keys incl OAUTH_TOKEN_ENDPOINT/CLIENT_ID/SCOPE
  - Query parameter contract
    - p_code, p_cv, p_cid, p_ruri, p_acc, p_signout
  - Error handling & troubleshooting
    - prompt=none

## Status
- Completed: Created `docs/parent-oauth-flow.md` with sequence diagram and full details.
- Completed: Linked the doc from `README.md` in the Track B section.
- Note: Track B implementation uses parent-managed OAuth with child server-side exchange, auto refresh (T-120s and 401), centered “Connect to Snowflake” button, and child-only sign out.
