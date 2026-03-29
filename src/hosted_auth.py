"""Hosted Notion MCP OAuth helpers for QuestBoard."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import queue
import secrets
import threading
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
from dotenv import find_dotenv, set_key

from .config import Config, HOSTED_MCP_URL

DEFAULT_CLIENT_NAME = "QuestBoard Hosted Login"
DEFAULT_CLIENT_URI = "https://github.com/bO-05/questboard"


@dataclass
class OAuthMetadata:
    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    registration_endpoint: str | None = None
    code_challenge_methods_supported: list[str] | None = None


@dataclass
class ClientCredentials:
    client_id: str
    client_secret: str = ""
    client_id_issued_at: int | None = None


@dataclass
class TokenResponse:
    access_token: str
    refresh_token: str = ""
    token_type: str = "Bearer"
    expires_in: int = 3600
    scope: str = ""

    @property
    def expires_at(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(seconds=max(self.expires_in, 0))

    @property
    def expires_at_iso(self) -> str:
        return self.expires_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def discover_oauth_metadata(server_url: str = HOSTED_MCP_URL, timeout_s: float = 20.0) -> OAuthMetadata:
    """Discover OAuth metadata for an MCP server."""
    protected_resource: dict[str, Any] | None = None
    errors: list[str] = []

    with httpx.Client(timeout=timeout_s, follow_redirects=True) as client:
        for candidate in _protected_resource_candidates(server_url):
            response = client.get(candidate, headers={"Accept": "application/json"})
            if response.is_success:
                protected_resource = response.json()
                break
            errors.append(f"{candidate} -> {response.status_code}")

        if protected_resource is None:
            raise RuntimeError(
                "OAuth discovery failed while loading protected resource metadata. "
                + " | ".join(errors)
            )

        auth_servers = protected_resource.get("authorization_servers") or []
        if not auth_servers:
            raise RuntimeError("Protected resource metadata did not include any authorization servers.")

        auth_server_url = auth_servers[0]
        metadata_url = _authorization_server_metadata_url(auth_server_url)
        response = client.get(metadata_url, headers={"Accept": "application/json"})
        response.raise_for_status()
        metadata = response.json()

    authorization_endpoint = metadata.get("authorization_endpoint")
    token_endpoint = metadata.get("token_endpoint")
    if not authorization_endpoint or not token_endpoint:
        raise RuntimeError("Authorization server metadata is missing required OAuth endpoints.")

    return OAuthMetadata(
        issuer=metadata.get("issuer", auth_server_url),
        authorization_endpoint=authorization_endpoint,
        token_endpoint=token_endpoint,
        registration_endpoint=metadata.get("registration_endpoint"),
        code_challenge_methods_supported=metadata.get("code_challenge_methods_supported"),
    )


def register_client(
    metadata: OAuthMetadata,
    redirect_uri: str,
    *,
    client_name: str = DEFAULT_CLIENT_NAME,
    client_uri: str = DEFAULT_CLIENT_URI,
    timeout_s: float = 20.0,
) -> ClientCredentials:
    """Register a public OAuth client for the hosted MCP server."""
    if not metadata.registration_endpoint:
        raise RuntimeError("The MCP server does not advertise a dynamic client registration endpoint.")

    payload = {
        "client_name": client_name,
        "client_uri": client_uri,
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
    }
    response = httpx.post(
        metadata.registration_endpoint,
        json=payload,
        headers={"Accept": "application/json"},
        timeout=timeout_s,
    )
    response.raise_for_status()
    body = response.json()
    client_id = body.get("client_id")
    if not client_id:
        raise RuntimeError("Client registration succeeded but did not return a client_id.")
    return ClientCredentials(
        client_id=client_id,
        client_secret=body.get("client_secret", ""),
        client_id_issued_at=body.get("client_id_issued_at"),
    )


def generate_code_verifier() -> str:
    verifier = base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")
    return verifier.rstrip("=")


def generate_code_challenge(code_verifier: str) -> str:
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def generate_state() -> str:
    return secrets.token_hex(32)


def build_authorization_url(
    metadata: OAuthMetadata,
    *,
    client_id: str,
    redirect_uri: str,
    code_challenge: str,
    state: str,
    scope: str = "",
) -> str:
    params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "prompt": "consent",
    }
    if scope:
        params["scope"] = scope

    return str(httpx.URL(metadata.authorization_endpoint, params=params))


def exchange_authorization_code(
    metadata: OAuthMetadata,
    credentials: ClientCredentials,
    *,
    code: str,
    redirect_uri: str,
    code_verifier: str,
    timeout_s: float = 20.0,
) -> TokenResponse:
    """Exchange an authorization code for access and refresh tokens."""
    payload = {
        "grant_type": "authorization_code",
        "client_id": credentials.client_id,
        "code": code,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
    }
    if credentials.client_secret:
        payload["client_secret"] = credentials.client_secret
    return _request_token_response(metadata.token_endpoint, payload, timeout_s=timeout_s)


def refresh_access_token(
    metadata: OAuthMetadata,
    credentials: ClientCredentials,
    *,
    refresh_token: str,
    timeout_s: float = 20.0,
) -> TokenResponse:
    """Refresh a hosted MCP access token."""
    payload = {
        "grant_type": "refresh_token",
        "client_id": credentials.client_id,
        "refresh_token": refresh_token,
    }
    if credentials.client_secret:
        payload["client_secret"] = credentials.client_secret
    return _request_token_response(metadata.token_endpoint, payload, timeout_s=timeout_s)


def maybe_refresh_hosted_access_token(
    config: Config,
    *,
    force: bool = False,
    skew_seconds: int = 600,
    env_path: str | os.PathLike[str] | None = None,
) -> bool:
    """Refresh hosted MCP tokens in-place when they are missing or near expiry."""
    if config.is_self_hosted:
        return False
    parsed = urlparse(config.mcp_server_url or "")
    if parsed.hostname != "mcp.notion.com":
        return False
    if not config.mcp_refresh_token or not config.mcp_client_id:
        return False
    if not force and _token_is_fresh(config.mcp_access_token, config.mcp_token_expires_at, skew_seconds):
        return False

    metadata = discover_oauth_metadata(config.mcp_server_url)
    credentials = ClientCredentials(
        client_id=config.mcp_client_id,
        client_secret=config.mcp_client_secret,
    )
    tokens = refresh_access_token(
        metadata,
        credentials,
        refresh_token=config.mcp_refresh_token,
    )
    persist_hosted_tokens(
        tokens,
        client_id=credentials.client_id,
        client_secret=credentials.client_secret,
        server_url=config.mcp_server_url,
        env_path=env_path,
    )
    apply_hosted_tokens_to_config(
        config,
        tokens,
        client_id=credentials.client_id,
        client_secret=credentials.client_secret,
        server_url=config.mcp_server_url,
    )
    return True


def persist_hosted_tokens(
    tokens: TokenResponse,
    *,
    client_id: str,
    client_secret: str = "",
    server_url: str = HOSTED_MCP_URL,
    env_path: str | os.PathLike[str] | None = None,
) -> Path:
    """Persist hosted MCP credentials into the local .env file."""
    resolved_env_path = resolve_env_path(env_path)
    resolved_env_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_env_path.touch(exist_ok=True)

    updates = {
        "MCP_SERVER_URL": server_url,
        "MCP_ACCESS_TOKEN": tokens.access_token,
        "MCP_REFRESH_TOKEN": tokens.refresh_token,
        "MCP_CLIENT_ID": client_id,
        "MCP_CLIENT_SECRET": client_secret,
        "MCP_TOKEN_EXPIRES_AT": tokens.expires_at_iso,
        "MCP_AUTH_TOKEN": "",
    }
    for key, value in updates.items():
        set_key(str(resolved_env_path), key, value, quote_mode="never")
        os.environ[key] = value

    return resolved_env_path


def apply_hosted_tokens_to_config(
    config: Config,
    tokens: TokenResponse,
    *,
    client_id: str,
    client_secret: str = "",
    server_url: str = HOSTED_MCP_URL,
) -> Config:
    """Apply refreshed hosted credentials to an in-memory Config object."""
    config.mcp_server_url = server_url
    config.mcp_access_token = tokens.access_token
    config.mcp_refresh_token = tokens.refresh_token
    config.mcp_client_id = client_id
    config.mcp_client_secret = client_secret
    config.mcp_token_expires_at = tokens.expires_at_iso
    config.mcp_auth_token = ""
    os.environ["MCP_SERVER_URL"] = server_url
    os.environ["MCP_ACCESS_TOKEN"] = tokens.access_token
    os.environ["MCP_REFRESH_TOKEN"] = tokens.refresh_token
    os.environ["MCP_CLIENT_ID"] = client_id
    os.environ["MCP_CLIENT_SECRET"] = client_secret
    os.environ["MCP_TOKEN_EXPIRES_AT"] = tokens.expires_at_iso
    os.environ["MCP_AUTH_TOKEN"] = ""
    return config


def open_browser(url: str) -> bool:
    """Open the system browser for OAuth authorization."""
    try:
        return bool(webbrowser.open(url, new=2))
    except Exception:
        return False


def resolve_env_path(env_path: str | os.PathLike[str] | None = None) -> Path:
    if env_path:
        return Path(env_path)
    discovered = find_dotenv(usecwd=True)
    if discovered:
        return Path(discovered)
    return Path.cwd() / ".env"


class LoopbackCallbackServer:
    """Temporary local callback server used for OAuth redirects."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0, callback_path: str = "/callback"):
        self.host = host
        self.port = port
        self.callback_path = callback_path
        self._queue: queue.Queue[dict[str, str]] = queue.Queue(maxsize=1)
        self._server: _OAuthHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "LoopbackCallbackServer":
        self._server = _OAuthHTTPServer((self.host, self.port), _OAuthCallbackHandler)
        self._server.callback_path = self.callback_path
        self._server.callback_queue = self._queue
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._server = None
        self._thread = None

    @property
    def redirect_uri(self) -> str:
        if self._server is None:
            raise RuntimeError("Loopback callback server has not been started.")
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}{self.callback_path}"

    def wait_for_callback(self, timeout_s: float = 300.0) -> dict[str, str]:
        try:
            return self._queue.get(timeout=timeout_s)
        except queue.Empty as exc:
            raise TimeoutError("Timed out waiting for the Notion OAuth callback.") from exc


class _OAuthHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True

    def __init__(self, server_address, request_handler_class):
        super().__init__(server_address, request_handler_class)
        self.callback_queue: queue.Queue[dict[str, str]] | None = None
        self.callback_path: str = "/callback"


class _OAuthCallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path != self.server.callback_path:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"QuestBoard OAuth callback not found.")
            return

        params = {key: values[0] for key, values in parse_qs(parsed.query).items() if values}
        if self.server.callback_queue is not None and self.server.callback_queue.empty():
            self.server.callback_queue.put(params)

        if params.get("error"):
            body = (
                "<html><body><h1>QuestBoard login failed</h1>"
                "<p>You can return to the terminal for details.</p></body></html>"
            )
        else:
            body = (
                "<html><body><h1>QuestBoard login complete</h1>"
                "<p>You can close this tab and return to the terminal.</p></body></html>"
            )

        payload = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args):
        return


def _protected_resource_candidates(server_url: str) -> list[str]:
    parsed = urlparse(server_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid MCP server URL: {server_url}")

    origin = f"{parsed.scheme}://{parsed.netloc}"
    candidates = [f"{origin}/.well-known/oauth-protected-resource"]

    normalized_path = parsed.path.rstrip("/")
    if normalized_path:
        candidates.append(f"{origin}{normalized_path}/.well-known/oauth-protected-resource")

    return list(dict.fromkeys(candidates))


def _authorization_server_metadata_url(auth_server_url: str) -> str:
    parsed = urlparse(auth_server_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid authorization server URL: {auth_server_url}")
    origin = f"{parsed.scheme}://{parsed.netloc}"
    return f"{origin}/.well-known/oauth-authorization-server"


def _request_token_response(token_endpoint: str, payload: dict[str, str], *, timeout_s: float = 20.0) -> TokenResponse:
    response = httpx.post(
        token_endpoint,
        data=payload,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "QuestBoard-MCP-Client/0.1.0",
        },
        timeout=timeout_s,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        error_payload: dict[str, Any] = {}
        try:
            error_payload = response.json()
        except json.JSONDecodeError:
            error_payload = {}

        error_code = str(error_payload.get("error", "")).strip()
        error_description = str(error_payload.get("error_description", "")).strip()
        if error_code == "invalid_grant" and payload.get("grant_type") == "refresh_token":
            raise RuntimeError(
                "Hosted Notion MCP refresh token is no longer valid for this repo. "
                "Run `questboard hosted-login` here to refresh the local .env credentials."
            ) from exc
        if error_code or error_description:
            detail = ": ".join(part for part in (error_code, error_description) if part)
            raise RuntimeError(f"Hosted Notion OAuth token request failed: {detail}") from exc
        raise
    body = response.json()
    access_token = body.get("access_token")
    if not access_token:
        raise RuntimeError("OAuth token response did not include an access_token.")
    expires_in = body.get("expires_in", 3600)
    try:
        expires_in = int(expires_in)
    except (TypeError, ValueError):
        expires_in = 3600
    return TokenResponse(
        access_token=access_token,
        refresh_token=body.get("refresh_token", ""),
        token_type=body.get("token_type", "Bearer"),
        expires_in=expires_in,
        scope=body.get("scope", ""),
    )


def _parse_expiry(timestamp: str) -> datetime | None:
    if not timestamp:
        return None
    normalized = timestamp.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _token_is_fresh(access_token: str, expires_at: str, skew_seconds: int) -> bool:
    if not access_token:
        return False
    parsed_expiry = _parse_expiry(expires_at)
    if parsed_expiry is None:
        return True
    return parsed_expiry > datetime.now(timezone.utc) + timedelta(seconds=skew_seconds)
