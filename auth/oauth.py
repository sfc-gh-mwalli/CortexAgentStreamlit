from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Dict, Optional

import requests


@dataclass
class OAuthTokens:
    access_token: str
    expires_at: int
    refresh_token: Optional[str] = None
    id_token: Optional[str] = None


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def generate_pkce_pair() -> Dict[str, str]:
    verifier = _b64url(os.urandom(40))
    challenge = _b64url(sha256(verifier.encode()).digest())
    return {"code_verifier": verifier, "code_challenge": challenge}


def build_authorize_url(account_url: str, client_id: str, redirect_uri: str, code_challenge: str, scope: str = "SESSION:ROLE-ANY", state: Optional[str] = None) -> str:
    from urllib.parse import urlencode

    auth = account_url.rstrip("/") + "/oauth/authorize"
    qs = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state or str(int(time.time())),
    }
    return auth + "?" + urlencode(qs)


def exchange_code_for_token(account_url: str, client_id: str, redirect_uri: str, code: str, code_verifier: str) -> OAuthTokens:
    # Snowflake OAuth token endpoint is /oauth/token-request
    token_url = account_url.rstrip("/") + "/oauth/token-request"
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "code_verifier": code_verifier,
    }
    resp = requests.post(token_url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    access_token = payload.get("access_token")
    expires_in = int(payload.get("expires_in", 3000))
    refresh_token = payload.get("refresh_token")
    id_token = payload.get("id_token")
    if not access_token:
        raise RuntimeError("No access_token in token response")
    return OAuthTokens(
        access_token=access_token,
        expires_at=int(time.time()) + expires_in,
        refresh_token=refresh_token,
        id_token=id_token,
    )
