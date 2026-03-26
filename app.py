#!/usr/bin/env python3
import atexit
import base64
import contextlib
import datetime
import errno
import hashlib
import io
import json
import mimetypes
import os
import re
import secrets
import shutil
import subprocess
import threading
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import uuid
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from codex2gpt.events import extract_event_details, iter_sse_messages
from codex2gpt.protocols.gemini import (
    codex_response_to_gemini,
    parse_model_action,
    stream_gemini_sse_from_codex_events,
    translate_gemini_request,
)
from codex2gpt.protocols.relay import (
    stream_anthropic_to_codex_sse,
    codex_request_to_anthropic,
    codex_request_to_gemini,
    codex_request_to_openai_chat,
    stream_gemini_to_codex_sse,
    stream_openai_chat_to_codex_sse,
)
from codex2gpt.schema_utils import prepare_json_schema
from codex2gpt.state_db import RuntimeStateStore


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUNTIME_DIR = os.path.join(BASE_DIR, "runtime")
AUTH_DIR = os.environ.get("LITE_AUTH_DIR", os.path.join(RUNTIME_DIR, "accounts"))
STATE_ROOT = os.path.abspath(os.environ.get("LITE_RUNTIME_ROOT", os.path.dirname(AUTH_DIR) or RUNTIME_DIR))
LISTEN_HOST = os.environ.get("LITE_HOST", "127.0.0.1")
LISTEN_PORT = int(os.environ.get("LITE_PORT", "18100"))
API_KEY = os.environ.get("LITE_API_KEY", "")
STATE_DB_PATH = os.path.abspath(os.environ.get("LITE_STATE_DB", os.path.join(STATE_ROOT, "state.sqlite3")))
COOKIES_PATH = os.path.abspath(os.environ.get("LITE_COOKIES_PATH", os.path.join(STATE_ROOT, "cookies.json")))
FINGERPRINT_CACHE_PATH = os.path.abspath(
    os.environ.get("LITE_FINGERPRINT_CACHE_PATH", os.path.join(STATE_ROOT, "fingerprint-cache.json"))
)
SETTINGS_PATH = os.path.abspath(os.environ.get("LITE_SETTINGS_PATH", os.path.join(STATE_ROOT, "settings.json")))
CODEX_AUTH_PATH = os.path.abspath(
    os.environ.get("LITE_CODEX_AUTH_PATH", os.path.join(os.path.expanduser("~"), ".codex", "auth.json"))
)
WEB_DIR = os.path.abspath(os.environ.get("LITE_WEB_DIR", os.path.join(BASE_DIR, "web")))
DEFAULT_MODEL = os.environ.get("LITE_MODEL", "gpt-5.4")
DEFAULT_MODELS_RAW = os.environ.get("LITE_MODELS", "")
MODEL_OVERRIDES_JSON = os.environ.get("LITE_MODEL_OVERRIDES_JSON", "").strip()
MODEL_OVERRIDES_PATH = os.path.abspath(os.environ.get("LITE_MODEL_OVERRIDES_PATH", os.path.join(BASE_DIR, "model-overrides.toml")))
DEFAULT_INSTRUCTIONS = os.environ.get("LITE_INSTRUCTIONS", "You are a helpful coding assistant.")
DEFAULT_REASONING_EFFORT = os.environ.get("LITE_REASONING_EFFORT", "medium").strip() or "medium"
DEFAULT_TEXT_VERBOSITY = os.environ.get("LITE_TEXT_VERBOSITY", "high").strip() or "high"
DEFAULT_MODEL_CONTEXT_WINDOW = int(os.environ.get("LITE_MODEL_CONTEXT_WINDOW", "258400") or "258400")
DEFAULT_MODEL_AUTO_COMPACT_TOKEN_LIMIT = int(
    os.environ.get("LITE_MODEL_AUTO_COMPACT_TOKEN_LIMIT", str((DEFAULT_MODEL_CONTEXT_WINDOW * 9) // 10))
    or str((DEFAULT_MODEL_CONTEXT_WINDOW * 9) // 10)
)
TRANSCRIPTS_ENABLED = os.environ.get("LITE_TRANSCRIPT_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
TRANSCRIPTS_DIR = os.path.abspath(os.environ.get("LITE_TRANSCRIPT_DIR", os.path.join(RUNTIME_DIR, "transcripts")))
SESSION_STICKY_TTL = int(os.environ.get("LITE_SESSION_STICKY_TTL", "3600") or "3600")
SESSION_LOCK_TTL = int(os.environ.get("LITE_SESSION_LOCK_TTL", str(max(SESSION_STICKY_TTL, 300))) or str(max(SESSION_STICKY_TTL, 300)))
DEFAULT_BUSINESS_KEY = os.environ.get("LITE_DEFAULT_BUSINESS_KEY", "default").strip() or "default"
DASHBOARD_PASSWORD = os.environ.get("LITE_DASHBOARD_PASSWORD", "").strip()
DASHBOARD_SESSION_COOKIE = os.environ.get("LITE_DASHBOARD_SESSION_COOKIE", "codex2gpt_dashboard_session").strip() or "codex2gpt_dashboard_session"
DASHBOARD_SESSION_TTL = int(os.environ.get("LITE_DASHBOARD_SESSION_TTL", "43200") or "43200")
GLOBAL_PROXY_URL = os.environ.get("LITE_GLOBAL_PROXY_URL", "").strip()
TRANSPORT_BACKEND = os.environ.get("LITE_TRANSPORT_BACKEND", "auto").strip().lower() or "auto"
RESPONSES_TRANSPORT = os.environ.get("LITE_RESPONSES_TRANSPORT", "").strip().lower()
FINGERPRINT_SOURCE_URL = os.environ.get("LITE_FINGERPRINT_SOURCE_URL", "").strip()
TOKEN_REFRESH_MARGIN_SECONDS = int(os.environ.get("LITE_TOKEN_REFRESH_MARGIN_SECONDS", "900") or "900")
OAUTH_CALLBACK_BIND_HOST = os.environ.get("LITE_OAUTH_CALLBACK_BIND_HOST", "127.0.0.1").strip() or "127.0.0.1"
OAUTH_CALLBACK_REDIRECT_HOST = os.environ.get("LITE_OAUTH_CALLBACK_REDIRECT_HOST", "localhost").strip() or "localhost"
OAUTH_CALLBACK_PORT = int(os.environ.get("LITE_OAUTH_CALLBACK_PORT", "1455") or "1455")
OAUTH_CALLBACK_TTL_SECONDS = int(os.environ.get("LITE_OAUTH_CALLBACK_TTL_SECONDS", "300") or "300")
CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
TOKEN_URL = "https://auth.openai.com/oauth/token"
AUTHORIZE_URL = "https://auth.openai.com/oauth/authorize"
UPSTREAM_URL = "https://chatgpt.com/backend-api/codex/responses"
QUOTA_URL = "https://chatgpt.com/backend-api/codex/usage"
IPIFY_URL = "https://api.ipify.org?format=json"
RETRYABLE_STATUS_CODES = {401, 403, 408, 409, 429, 500, 502, 503, 504}
UPSTREAM_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "text/event-stream",
    "OpenAI-Beta": "responses_websockets=2026-02-06",
    "originator": "codex_cli_rs",
    "user-agent": "codex-cli/0.104.0",
    "Origin": "https://chatgpt.com",
    "Referer": "https://chatgpt.com/codex",
}
UNSUPPORTED_TOP_LEVEL_FIELDS = {
    "max_output_tokens",
    "max_tokens",
    "max_completion_tokens",
    "temperature",
    "metadata",
    "service_tier",
    "output_config",
    "thinking",
    "context_management",
    "parallel_tool_calls",
    "stream_options",
    "reasoning_effort",
    "user",
    "n",
    "business_key",
    "client_id",
    "conversation_id",
    "session_id",
}
REASONING_EFFORT_VALUES = {"minimal", "low", "medium", "high", "xhigh"}
OPENAI_RESPONSE_FORMAT_TYPES = {"json_object", "json_schema", "text"}
ANTHROPIC_VERSION_HEADER = "anthropic-version"
ANTHROPIC_MODEL_ALIASES = {
    "claude-opus-4-6": "gpt-5.4-1m",
    "claude-sonnet-4-6": "gpt-5.4",
    "claude-haiku-4-5": "gpt-5.3-codex",
}
ANTHROPIC_SUPPORTED_MODELS = set(ANTHROPIC_MODEL_ALIASES) | {"gpt-5.4", "gpt-5.3-codex"}
GEMINI_ACTIONS = {"generateContent", "streamGenerateContent"}
ANTHROPIC_ERROR_TYPES = {
    400: "invalid_request_error",
    401: "authentication_error",
    404: "not_found_error",
    429: "rate_limit_error",
}


def parse_models(raw: str):
    seen = set()
    models = []
    for item in raw.split(","):
        model = item.strip()
        if not model or model in seen:
            continue
        seen.add(model)
        models.append(model)
    if models:
        return models
    return [DEFAULT_MODEL]


ADVERTISED_MODELS = parse_models(DEFAULT_MODELS_RAW)


def positive_int_or_none(value: Any):
    try:
        current = int(value)
    except (TypeError, ValueError):
        return None
    if current <= 0:
        return None
    return current


def load_model_overrides():
    payload = {}
    if MODEL_OVERRIDES_JSON:
        try:
            payload = json.loads(MODEL_OVERRIDES_JSON)
        except Exception:
            payload = {}
    else:
        try:
            with open(MODEL_OVERRIDES_PATH, "rb") as f:
                payload = tomllib.load(f)
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            payload = payload.get("model_overrides") or {}
    if not isinstance(payload, dict):
        return {}

    overrides = {}
    for model_name, config in payload.items():
        name = str(model_name or "").strip()
        if not name or not isinstance(config, dict):
            continue
        upstream_model = str(config.get("upstream_model") or name).strip() or name
        context_window = positive_int_or_none(config.get("context_window")) or DEFAULT_MODEL_CONTEXT_WINDOW
        auto_compact = positive_int_or_none(config.get("auto_compact_token_limit"))
        if auto_compact is None:
            auto_compact = (context_window * 9) // 10
        auto_compact = min(auto_compact, context_window)
        overrides[name] = {
            "upstream_model": upstream_model,
            "context_window": context_window,
            "auto_compact_token_limit": auto_compact,
            "advertise": bool(config.get("advertise")),
        }
    return overrides


MODEL_OVERRIDES = load_model_overrides()


def resolve_model_spec(model_name: Any):
    requested_model = str(model_name or DEFAULT_MODEL).strip() or DEFAULT_MODEL
    override = MODEL_OVERRIDES.get(requested_model)
    if override:
        return {
            "requested_model": requested_model,
            "effective_model": str(override.get("upstream_model") or requested_model),
            "context_window": int(override.get("context_window") or DEFAULT_MODEL_CONTEXT_WINDOW),
            "auto_compact_token_limit": int(
                override.get("auto_compact_token_limit") or DEFAULT_MODEL_AUTO_COMPACT_TOKEN_LIMIT
            ),
            "advertise": bool(override.get("advertise")),
        }
    return {
        "requested_model": requested_model,
        "effective_model": requested_model,
        "context_window": DEFAULT_MODEL_CONTEXT_WINDOW,
        "auto_compact_token_limit": DEFAULT_MODEL_AUTO_COMPACT_TOKEN_LIMIT,
        "advertise": requested_model in ADVERTISED_MODELS,
    }


def configured_model_overrides_snapshot():
    snapshot = {}
    for model_name, config in MODEL_OVERRIDES.items():
        snapshot[model_name] = {
            "effective_model": str(config.get("upstream_model") or model_name),
            "context_window": int(config.get("context_window") or DEFAULT_MODEL_CONTEXT_WINDOW),
            "auto_compact_token_limit": int(
                config.get("auto_compact_token_limit") or DEFAULT_MODEL_AUTO_COMPACT_TOKEN_LIMIT
            ),
            "advertise": bool(config.get("advertise")),
        }
    return snapshot


def advertised_model_entries():
    entries = []
    seen = set()
    for model_name in ADVERTISED_MODELS:
        entries.append((model_name, resolve_model_spec(model_name)))
        seen.add(model_name)
    for model_name, config in MODEL_OVERRIDES.items():
        if not bool(config.get("advertise")) or model_name in seen:
            continue
        entries.append((model_name, resolve_model_spec(model_name)))
        seen.add(model_name)
    return entries


def ensure_parent_dir(path):
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)


def read_json_file(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except json.JSONDecodeError:
        return default


def write_json_file(path, payload):
    ensure_parent_dir(path)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def format_rfc3339(dt: datetime.datetime) -> str:
    return dt.astimezone(datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def normalize_identifier(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_email_identifier(value: Any) -> str:
    normalized = normalize_identifier(value)
    if "@" in normalized:
        return normalized.lower()
    return normalized


def auth_tokens_from_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    tokens = payload.get("tokens")
    merged = dict(tokens) if isinstance(tokens, dict) else {}
    for key in ("access_token", "refresh_token", "id_token", "account_id"):
        if key not in merged and payload.get(key) is not None:
            merged[key] = payload.get(key)
    return merged


def auth_claims_from_payload(payload: Any) -> dict[str, Any]:
    tokens = auth_tokens_from_payload(payload)
    return decode_jwt_payload(tokens.get("id_token", ""))


def claim_chatgpt_auth(claims: Any) -> dict[str, Any]:
    raw = claims.get("https://api.openai.com/auth") if isinstance(claims, dict) else {}
    return raw if isinstance(raw, dict) else {}


def extract_auth_account_id(payload: Any, claims: dict[str, Any] | None = None) -> str:
    claims = claims if isinstance(claims, dict) else {}
    tokens = auth_tokens_from_payload(payload)
    return normalize_identifier(
        tokens.get("account_id")
        or (payload.get("account_id") if isinstance(payload, dict) else "")
        or (payload.get("workspace_id") if isinstance(payload, dict) else "")
        or claim_chatgpt_auth(claims).get("chatgpt_account_id")
    )


def extract_auth_email(payload: Any, claims: dict[str, Any] | None = None) -> str:
    claims = claims if isinstance(claims, dict) else {}
    profile = claims.get("https://api.openai.com/profile") if isinstance(claims.get("https://api.openai.com/profile"), dict) else {}
    return normalize_identifier(
        (payload.get("email") if isinstance(payload, dict) else "")
        or claims.get("email")
        or profile.get("email")
    )


def extract_auth_user_id(payload: Any, claims: dict[str, Any] | None = None) -> str:
    claims = claims if isinstance(claims, dict) else {}
    auth_claims = claim_chatgpt_auth(claims)
    return normalize_identifier(
        (payload.get("user_id") if isinstance(payload, dict) else "")
        or claims.get("sub")
        or auth_claims.get("user_id")
        or auth_claims.get("chatgpt_user_id")
    )


def extract_auth_plan_type(payload: Any, claims: dict[str, Any] | None = None) -> str:
    claims = claims if isinstance(claims, dict) else {}
    auth_claims = claim_chatgpt_auth(claims)
    return normalize_identifier(
        (payload.get("plan_type") if isinstance(payload, dict) else "")
        or (payload.get("plan") if isinstance(payload, dict) else "")
        or auth_claims.get("chatgpt_plan_type")
    )


def auth_identity_key(account_id: Any = "", *, user_id: Any = "", email: Any = "") -> str:
    normalized_account_id = normalize_identifier(account_id)
    principal = normalize_identifier(user_id) or normalize_email_identifier(email) or normalized_account_id
    if not normalized_account_id:
        return principal
    return f"{principal}|{normalized_account_id}"


def auth_identity_key_from_payload(payload: Any) -> str:
    claims = auth_claims_from_payload(payload)
    return auth_identity_key(
        extract_auth_account_id(payload, claims),
        user_id=extract_auth_user_id(payload, claims),
        email=extract_auth_email(payload, claims),
    )


def normalize_last_refresh(value: Any) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return format_rfc3339(datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc))

    raw = normalize_identifier(value)
    if raw:
        if raw.isdigit():
            return format_rfc3339(datetime.datetime.fromtimestamp(float(raw), tz=datetime.timezone.utc))
        try:
            parsed = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=datetime.timezone.utc)
            return format_rfc3339(parsed)
        except ValueError:
            pass
        try:
            parsed = parsedate_to_datetime(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=datetime.timezone.utc)
            return format_rfc3339(parsed)
        except (TypeError, ValueError, IndexError, OverflowError):
            pass
    return format_rfc3339(datetime.datetime.now(datetime.timezone.utc))


def normalize_codex_tokens(root: Any) -> dict[str, Any]:
    if not isinstance(root, dict):
        raise RuntimeError("auth payload must be a JSON object")
    tokens = auth_tokens_from_payload(root)
    access_token = normalize_identifier(tokens.get("access_token"))
    id_token = normalize_identifier(tokens.get("id_token"))
    if not access_token:
        raise RuntimeError("auth.json missing access_token")
    if not id_token:
        raise RuntimeError("auth.json missing id_token")
    claims = decode_jwt_payload(id_token)
    normalized = dict(tokens)
    normalized["access_token"] = access_token
    normalized["id_token"] = id_token
    refresh_token = normalize_identifier(tokens.get("refresh_token"))
    if refresh_token:
        normalized["refresh_token"] = refresh_token
    account_id = extract_auth_account_id(root, claims)
    if account_id:
        normalized["account_id"] = account_id
    return normalized


def normalize_codex_auth_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise RuntimeError("auth payload must be a JSON object")
    root = dict(payload)
    tokens = normalize_codex_tokens(root)
    claims = decode_jwt_payload(tokens.get("id_token", ""))
    root["auth_mode"] = normalize_identifier(root.get("auth_mode")) or "chatgpt"
    root["OPENAI_API_KEY"] = root.get("OPENAI_API_KEY")
    root["tokens"] = tokens
    root["last_refresh"] = normalize_last_refresh(root.get("last_refresh"))
    email = extract_auth_email(root, claims)
    user_id = extract_auth_user_id(root, claims)
    plan_type = extract_auth_plan_type(root, claims)
    if email:
        root["email"] = email
    if user_id:
        root["user_id"] = user_id
    if plan_type:
        root["plan_type"] = plan_type
    for key in ("access_token", "refresh_token", "id_token", "account_id", "workspace_id"):
        root.pop(key, None)
    return root


def write_codex_auth_file(path: str, payload: Any) -> dict[str, Any]:
    normalized = normalize_codex_auth_payload(payload)
    ensure_parent_dir(path)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return normalized


def default_runtime_settings():
    return {
        "rotation_mode": "least_used",
        "responses_transport": RESPONSES_TRANSPORT or "auto",
        "quota": {"warning_threshold": 0.9, "refresh_interval_seconds": 300, "skip_exhausted": True},
        "dashboard": {"password_required": bool(DASHBOARD_PASSWORD or API_KEY)},
        "codex_app": {"selected_entry_id": "", "selected_identity_key": "", "selected_at": ""},
        "proxy_defaults": {"global_proxy_id": ""},
        "background_jobs": {
            "quota_refresh_seconds": 300,
            "proxy_health_seconds": 300,
            "fingerprint_refresh_seconds": 3600,
            "token_refresh_seconds": 300,
        },
        "plans": {
            "gpt-5.4": [],
            "gpt-5.3-codex": [],
        },
    }


def load_runtime_settings():
    settings = default_runtime_settings()
    loaded = read_json_file(SETTINGS_PATH, {})
    if isinstance(loaded, dict):
        for key, value in loaded.items():
            if isinstance(settings.get(key), dict) and isinstance(value, dict):
                merged = dict(settings[key])
                merged.update(value)
                settings[key] = merged
            else:
                settings[key] = value
    return settings


def save_runtime_settings(settings):
    write_json_file(SETTINGS_PATH, settings)


def load_cookie_store():
    data = read_json_file(COOKIES_PATH, {})
    return data if isinstance(data, dict) else {}


def save_cookie_store(cookies):
    write_json_file(COOKIES_PATH, cookies if isinstance(cookies, dict) else {})


def parse_cookie_expiry(value):
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = parsedate_to_datetime(value.strip())
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat()


def load_fingerprint_cache():
    payload = read_json_file(FINGERPRINT_CACHE_PATH, {})
    if isinstance(payload, dict) and payload:
        return payload
    return {
        "app_version": "0.104.0",
        "build_number": "dev",
        "chromium_version": "131.0.6778.265",
        "platform": "Mac OS X",
        "arch": "arm64",
        "originator": "codex_cli_rs",
        "header_order": [
            "Authorization",
            "ChatGPT-Account-Id",
            "originator",
            "User-Agent",
            "sec-ch-ua",
            "sec-ch-ua-mobile",
            "sec-ch-ua-platform",
            "Accept-Encoding",
            "Accept-Language",
            "sec-fetch-site",
            "sec-fetch-mode",
            "sec-fetch-dest",
            "Content-Type",
            "Accept",
            "OpenAI-Beta",
            "Origin",
            "Referer",
            "Cookie",
        ],
        "default_headers": {
            "Accept-Language": "en-US,en;q=0.9",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"macOS\"",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
        },
        "updated_at": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat(),
        "transport_backend": "direct",
    }


def find_curl_impersonate_binary():
    candidates = [
        "curl-impersonate",
        "curl_chrome",
        "curl-impersonate-chrome",
        "curl",
    ]
    for candidate in candidates:
        path = shutil.which(candidate)
        if path:
            return path
    return ""


def current_transport_backend():
    binary = find_curl_impersonate_binary()
    if TRANSPORT_BACKEND == "direct":
        return "direct"
    if TRANSPORT_BACKEND == "curl_impersonate":
        return "curl_impersonate" if binary else "direct"
    if binary:
        return "curl_impersonate"
    return "direct"


def order_headers(headers):
    order = FINGERPRINT_CACHE.get("header_order")
    if not isinstance(order, list):
        return headers
    ordered = {}
    consumed = set()
    lowered = {str(key).lower(): key for key in headers.keys()}
    for desired in order:
        actual = lowered.get(str(desired).lower())
        if actual is None or actual in consumed:
            continue
        ordered[actual] = headers[actual]
        consumed.add(actual)
    for key, value in headers.items():
        if key not in consumed:
            ordered[key] = value
    return ordered


def build_sec_ch_ua():
    chromium_version = str(FINGERPRINT_CACHE.get("chromium_version") or "131")
    major = chromium_version.split(".", 1)[0] or "131"
    return f'"Chromium";v="{major}", "Not:A-Brand";v="24"'


def build_desktop_user_agent():
    app_version = str(FINGERPRINT_CACHE.get("app_version") or "0.104.0")
    platform = str(FINGERPRINT_CACHE.get("platform") or "Mac OS X")
    arch = str(FINGERPRINT_CACHE.get("arch") or "arm64")
    return f"Codex Desktop/{app_version} ({platform}; {arch})"


def build_default_desktop_headers():
    defaults = dict(FINGERPRINT_CACHE.get("default_headers") or {})
    defaults.setdefault("User-Agent", build_desktop_user_agent())
    defaults.setdefault("sec-ch-ua", build_sec_ch_ua())
    defaults.setdefault("Origin", "https://chatgpt.com")
    defaults.setdefault("Referer", "https://chatgpt.com/codex")
    defaults.setdefault("Accept", "text/event-stream")
    defaults.setdefault("originator", str(FINGERPRINT_CACHE.get("originator") or "codex_cli_rs"))
    return defaults


def build_anonymous_desktop_headers():
    headers = build_default_desktop_headers()
    headers.pop("Content-Type", None)
    headers.pop("Cookie", None)
    return order_headers(headers)


RUNTIME_SETTINGS = load_runtime_settings()
COOKIE_STORE = load_cookie_store()
FINGERPRINT_CACHE = load_fingerprint_cache()
FINGERPRINT_CACHE["transport_backend"] = current_transport_backend()
STATE_DB = RuntimeStateStore(STATE_DB_PATH)
atexit.register(STATE_DB.close)


def codex_app_runtime_settings():
    settings = RUNTIME_SETTINGS.get("codex_app")
    if not isinstance(settings, dict):
        settings = {"selected_entry_id": "", "selected_identity_key": "", "selected_at": ""}
        RUNTIME_SETTINGS["codex_app"] = settings
    return settings


def save_codex_app_selection(entry_id: str, identity_key: str):
    settings = codex_app_runtime_settings()
    settings["selected_entry_id"] = normalize_identifier(entry_id)
    settings["selected_identity_key"] = normalize_identifier(identity_key)
    settings["selected_at"] = now_iso()
    save_runtime_settings(RUNTIME_SETTINGS)
    return dict(settings)


def load_codex_auth_payload():
    payload = read_json_file(CODEX_AUTH_PATH, {})
    return payload if isinstance(payload, dict) else {}


def current_codex_auth_identity():
    payload = load_codex_auth_payload()
    if not payload:
        return {"payload": {}, "identity_key": "", "account_id": "", "email": "", "user_id": ""}
    claims = auth_claims_from_payload(payload)
    account_id = extract_auth_account_id(payload, claims)
    email = extract_auth_email(payload, claims)
    user_id = extract_auth_user_id(payload, claims)
    return {
        "payload": payload,
        "identity_key": auth_identity_key(account_id, user_id=user_id, email=email),
        "account_id": account_id,
        "email": email,
        "user_id": user_id,
    }


def match_entry_id_for_identity(identity_key: str = "", account_id: str = "") -> str:
    identity_key = normalize_identifier(identity_key)
    normalized_account_id = normalize_identifier(account_id)
    for account in STATE_DB.list_accounts():
        metadata = account.get("metadata") if isinstance(account.get("metadata"), dict) else {}
        candidate_key = normalize_identifier(metadata.get("identity_key"))
        if identity_key and candidate_key == identity_key:
            return account["entry_id"]
        if normalized_account_id and normalize_identifier(account.get("account_id")) == normalized_account_id:
            return account["entry_id"]
    return ""


def current_codex_app_state():
    selection = codex_app_runtime_settings()
    current = current_codex_auth_identity()
    current_entry_id = match_entry_id_for_identity(current["identity_key"], current["account_id"])
    current_account = STATE_DB.get_account(current_entry_id) if current_entry_id else None
    selected_identity_key = normalize_identifier(selection.get("selected_identity_key"))
    current_identity_key = normalize_identifier(current.get("identity_key"))
    return {
        "auth_path": CODEX_AUTH_PATH,
        "exists": os.path.isfile(CODEX_AUTH_PATH),
        "current_entry_id": current_entry_id,
        "current_identity_key": current_identity_key,
        "current_account_id": normalize_identifier(current.get("account_id")),
        "current_email": normalize_identifier(current.get("email")),
        "matched": bool(current_entry_id),
        "manual_restart_required": True,
        "selected_entry_id": normalize_identifier(selection.get("selected_entry_id")),
        "selected_identity_key": selected_identity_key,
        "selected_at": normalize_identifier(selection.get("selected_at")),
        "external_override_detected": bool(selected_identity_key and current_identity_key and selected_identity_key != current_identity_key),
        "current_status": normalize_identifier(current_account.get("status")) if current_account else "",
    }


def current_codex_app_reserved_entry_id() -> str:
    state = current_codex_app_state()
    return state["current_entry_id"] if state.get("matched") else ""


def canonical_json_bytes(payload):
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


class OAuthAccount:
    def __init__(self, auth_file: str):
        self.auth_file = auth_file
        self.name = os.path.basename(auth_file)
        self.lock = threading.Lock()

    def _load(self):
        with open(self.auth_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self, data):
        write_codex_auth_file(self.auth_file, data)

    def access_token(self):
        data = self._load()
        token = data.get("tokens", {}).get("access_token", "").strip()
        if token:
            return token
        return self.refresh_access_token()

    def refresh_access_token(self):
        with self.lock:
            data = self._load()
            refresh_token = data.get("tokens", {}).get("refresh_token", "").strip()
            if not refresh_token:
                raise RuntimeError(f"missing refresh_token in {self.auth_file}")
            body = urllib.parse.urlencode(
                {
                    "grant_type": "refresh_token",
                    "client_id": CLIENT_ID,
                    "refresh_token": refresh_token,
                    "scope": "openid profile email",
                }
            ).encode()
            req = urllib.request.Request(
                TOKEN_URL,
                data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                refreshed = json.load(resp)
            tokens = data.setdefault("tokens", {})
            tokens["access_token"] = refreshed["access_token"]
            if refreshed.get("refresh_token"):
                tokens["refresh_token"] = refreshed["refresh_token"]
            if refreshed.get("id_token"):
                tokens["id_token"] = refreshed["id_token"]
            data["last_refresh"] = normalize_last_refresh(time.time())
            self._save(data)
            return tokens["access_token"]


class AccountPool:
    def __init__(self, auth_dir: str):
        self.auth_dir = auth_dir
        self.lock = threading.Lock()
        self.accounts = []
        self.cooldowns = {}
        self.sticky_sessions = {}
        self.preferred_account_name = ""
        self.next_index = 0
        self.usage_counts = {}
        self.reload()

    def reload(self):
        accounts = []
        if os.path.isdir(self.auth_dir):
            for name in sorted(os.listdir(self.auth_dir)):
                if not name.endswith(".json"):
                    continue
                path = os.path.join(self.auth_dir, name)
                if os.path.isfile(path):
                    accounts.append(OAuthAccount(path))
        with self.lock:
            self.accounts = accounts
            self.cooldowns = {k: v for k, v in self.cooldowns.items() if v > time.time()}
            self.sticky_sessions = {
                session_key: binding
                for session_key, binding in self.sticky_sessions.items()
                if binding["expires_at"] > time.time() and any(account.name == binding["account_name"] for account in accounts)
            }
            self.usage_counts = {
                account.name: int(self.usage_counts.get(account.name, 0))
                for account in accounts
            }
            if self.preferred_account_name and not any(account.name == self.preferred_account_name for account in accounts):
                self.preferred_account_name = ""
            if self.accounts:
                self.next_index %= len(self.accounts)
            else:
                self.next_index = 0

    def names(self):
        with self.lock:
            return [account.name for account in self.accounts]

    def size(self):
        with self.lock:
            return len(self.accounts)

    def sticky_size(self):
        with self.lock:
            self._prune_sticky_sessions_locked()
            return len(self.sticky_sessions)

    def preferred_account(self):
        with self.lock:
            return self.preferred_account_name

    def _prune_sticky_sessions_locked(self):
        now = time.time()
        self.sticky_sessions = {
            session_key: binding
            for session_key, binding in self.sticky_sessions.items()
            if binding["expires_at"] > now
        }

    def _prefer_account_locked(self, ordered, preferred_name):
        preferred_name = preferred_name.strip()
        if not preferred_name:
            return ordered
        preferred = None
        others = []
        for account in ordered:
            if account.name == preferred_name and preferred is None:
                preferred = account
            else:
                others.append(account)
        if preferred is None:
            return ordered
        return [preferred] + others

    def _deprioritize_codex_app_account_locked(self, ordered):
        reserved_name = current_codex_app_reserved_entry_id()
        if not reserved_name:
            return ordered
        reserved = None
        others = []
        for account in ordered:
            if account.name == reserved_name and reserved is None:
                reserved = account
            else:
                others.append(account)
        if reserved is None or not others:
            return ordered
        return others + [reserved]

    def _selectable_accounts_locked(self, accounts):
        account_rows = {row["entry_id"]: row for row in STATE_DB.list_accounts()}
        selectable = []
        for account in accounts:
            status = str((account_rows.get(account.name) or {}).get("status") or "").strip().lower()
            if status in {"rate_limited", "expired", "banned"}:
                continue
            selectable.append(account)
        return selectable or accounts

    def _base_order_locked(self):
        accounts = list(self.accounts)
        if not accounts:
            return []
        accounts = self._selectable_accounts_locked(accounts)
        now = time.time()
        active_accounts = [account for account in accounts if self.cooldowns.get(account.name, 0) <= now] or accounts
        rotation_mode = current_rotation_mode()
        if rotation_mode == "least_used":
            ordered = sorted(active_accounts, key=lambda account: (int(self.usage_counts.get(account.name, 0)), account.name))
        else:
            start = self.next_index % len(active_accounts if active_accounts else accounts)
            self.next_index = (start + 1) % len(active_accounts if active_accounts else accounts)
            ordered = accounts[start:] + accounts[:start]
            active = [a for a in ordered if self.cooldowns.get(a.name, 0) <= now]
            ordered = active or ordered
        return self._prefer_account_locked(ordered, self.preferred_account_name if rotation_mode != "round_robin" else "")

    def _filter_accounts_for_model_locked(self, ordered, model_name):
        if not model_name:
            return ordered
        plans = RUNTIME_SETTINGS.get("plans")
        required_plans = []
        if isinstance(plans, dict):
            raw = plans.get(model_name)
            if isinstance(raw, list):
                required_plans = [str(item).strip() for item in raw if str(item).strip()]
        if not required_plans:
            return ordered
        account_rows = {row["entry_id"]: row for row in STATE_DB.list_accounts()}
        any_known = False
        matched = []
        for account in ordered:
            plan_type = str((account_rows.get(account.name) or {}).get("plan_type") or "").strip()
            if not plan_type:
                continue
            any_known = True
            if plan_type in required_plans:
                matched.append(account)
        if matched:
            return matched
        if not any_known:
            return ordered
        return []

    def candidates(self, session_key: str = "", model_name: str = ""):
        with self.lock:
            self._prune_sticky_sessions_locked()
            ordered = self._filter_accounts_for_model_locked(self._base_order_locked(), model_name)
            if not ordered:
                return []
            session_key = session_key.strip()
            if not session_key:
                return self._deprioritize_codex_app_account_locked(ordered)
            binding = self.sticky_sessions.get(session_key)
            if not binding:
                return self._deprioritize_codex_app_account_locked(ordered)
            reordered = self._prefer_account_locked(ordered, binding["account_name"])
            if reordered == ordered:
                self.sticky_sessions.pop(session_key, None)
                return self._deprioritize_codex_app_account_locked(ordered)
            return self._deprioritize_codex_app_account_locked(reordered)

    def bind_session(self, session_key: str, account_name: str):
        session_key = session_key.strip()
        if not session_key or not account_name:
            return
        with self.lock:
            self._prune_sticky_sessions_locked()
            self.sticky_sessions[session_key] = {
                "account_name": account_name,
                "expires_at": time.time() + max(1, SESSION_STICKY_TTL),
            }

    def mark_success(self, account_name: str):
        account_name = account_name.strip()
        if not account_name:
            return
        with self.lock:
            self.cooldowns.pop(account_name, None)
            self.preferred_account_name = account_name
            self.usage_counts[account_name] = int(self.usage_counts.get(account_name, 0)) + 1

    def mark_failure(self, account_name: str, error):
        cooldown = 30
        if isinstance(error, urllib.error.HTTPError):
            if error.code == 429:
                cooldown = 300
            elif error.code == 402:
                cooldown = 3600
            elif error.code in {401, 403}:
                cooldown = 120
            elif error.code >= 500:
                cooldown = 30
        with self.lock:
            self.cooldowns[account_name] = time.time() + cooldown
            if self.preferred_account_name == account_name:
                self.preferred_account_name = ""
            self.sticky_sessions = {
                session_key: binding
                for session_key, binding in self.sticky_sessions.items()
                if binding["account_name"] != account_name
            }


pool = AccountPool(AUTH_DIR)


class SessionCoordinator:
    def __init__(self, ttl_seconds: int):
        self.ttl_seconds = max(1, ttl_seconds)
        self.lock = threading.Lock()
        self.sessions = {}

    def _prune_locked(self):
        now = time.time()
        self.sessions = {
            session_key: state
            for session_key, state in self.sessions.items()
            if state["holders"] > 0 or state["waiters"] > 0 or state["expires_at"] > now
        }

    def snapshot(self):
        with self.lock:
            self._prune_locked()
            active_sessions = 0
            queued_requests = 0
            for state in self.sessions.values():
                if state["holders"] > 0:
                    active_sessions += 1
                queued_requests += state["waiters"]
            return {
                "tracked_sessions": len(self.sessions),
                "active_sessions": active_sessions,
                "queued_requests": queued_requests,
            }

    @contextlib.contextmanager
    def hold(self, session_key: str):
        session_key = session_key.strip()
        if not session_key:
            yield
            return

        with self.lock:
            self._prune_locked()
            state = self.sessions.get(session_key)
            if state is None:
                state = {
                    "lock": threading.Lock(),
                    "holders": 0,
                    "waiters": 0,
                    "expires_at": time.time() + self.ttl_seconds,
                    "previous_response_id": "",
                }
                self.sessions[session_key] = state
            state["waiters"] += 1
            state["expires_at"] = time.time() + self.ttl_seconds

        state["lock"].acquire()
        with self.lock:
            state["waiters"] = max(0, state["waiters"] - 1)
            state["holders"] += 1
            state["expires_at"] = time.time() + self.ttl_seconds

        try:
            yield
        finally:
            with self.lock:
                state["holders"] = max(0, state["holders"] - 1)
                state["expires_at"] = time.time() + self.ttl_seconds
            state["lock"].release()
            with self.lock:
                self._prune_locked()

    def previous_response_id(self, session_key: str):
        session_key = str(session_key or "").strip()
        if not session_key:
            return ""
        with self.lock:
            self._prune_locked()
            state = self.sessions.get(session_key) or {}
            return str(state.get("previous_response_id") or "").strip()

    def remember_response(self, session_key: str, response_id: str):
        session_key = str(session_key or "").strip()
        response_id = str(response_id or "").strip()
        if not session_key or not response_id:
            return
        with self.lock:
            self._prune_locked()
            state = self.sessions.get(session_key)
            if state is None:
                self.sessions[session_key] = {
                    "lock": threading.Lock(),
                    "holders": 0,
                    "waiters": 0,
                    "expires_at": time.time() + self.ttl_seconds,
                    "previous_response_id": response_id,
                }
                return
            state["previous_response_id"] = response_id
            state["expires_at"] = time.time() + self.ttl_seconds


session_coordinator = SessionCoordinator(SESSION_LOCK_TTL)


class ProxyError(Exception):
    def __init__(self, status: int, error_type: str, message: str):
        super().__init__(message)
        self.status = status
        self.error_type = error_type
        self.message = message


class TranscriptStore:
    def __init__(self, enabled: bool, base_dir: str):
        self.enabled = enabled
        self.base_dir = os.path.abspath(base_dir)
        self.lock = threading.Lock()

    def new_request_id(self):
        return uuid.uuid4().hex

    def snapshot(self):
        return {"enabled": self.enabled, "dir": self.base_dir}

    def _date_dir(self, now: datetime.datetime):
        return now.strftime("%Y-%m-%d")

    def _safe_session_key(self, session_key: str):
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(session_key or "").strip()).strip("._-")
        if not cleaned:
            return "session"
        return cleaned[:120]

    def _path_for(self, request_id: str, session_key: str, now: datetime.datetime):
        day_dir = os.path.join(self.base_dir, self._date_dir(now))
        if session_key and str(session_key).strip():
            return os.path.join(day_dir, f"{self._safe_session_key(session_key)}.jsonl")
        return os.path.join(day_dir, "anon", f"{request_id}.jsonl")

    def append(self, request_id: str, session_key: str, record):
        if not self.enabled:
            return None
        now = datetime.datetime.now().astimezone()
        path = self._path_for(request_id, session_key, now)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        with self.lock:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "ab") as f:
                f.write(line)
        return path


transcript_store = TranscriptStore(TRANSCRIPTS_ENABLED, TRANSCRIPTS_DIR)


class RecentRequestStore:
    def __init__(self, limit: int = 100):
        self.limit = max(10, int(limit or 100))
        self.lock = threading.Lock()
        self.items = []

    def append(self, item):
        with self.lock:
            self.items.insert(0, item)
            if len(self.items) > self.limit:
                self.items = self.items[: self.limit]

    def list(self, limit: int = 20):
        with self.lock:
            return list(self.items[: max(1, int(limit or 20))])


RECENT_REQUESTS = RecentRequestStore()


def is_record(value):
    return isinstance(value, dict)


def now_iso():
    return datetime.datetime.now().astimezone().isoformat(timespec="seconds")


def decode_jwt_payload(token):
    if not isinstance(token, str) or token.count(".") < 2:
        return {}
    payload = token.split(".")[1]
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode((payload + padding).encode("ascii"))
        value = json.loads(decoded.decode("utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def is_local_request(client_ip):
    client_ip = str(client_ip or "").strip().lower()
    return client_ip in {"127.0.0.1", "::1", "localhost"}


def dashboard_secret():
    return DASHBOARD_PASSWORD or API_KEY


def current_rotation_mode():
    mode = str(RUNTIME_SETTINGS.get("rotation_mode") or "least_used").strip().lower()
    if mode not in {"least_used", "round_robin", "sticky"}:
        return "least_used"
    return mode


def current_responses_transport_mode():
    mode = RESPONSES_TRANSPORT or str(RUNTIME_SETTINGS.get("responses_transport") or "auto")
    mode = mode.strip().lower()
    if mode not in {"auto", "http", "websocket"}:
        return "auto"
    return mode


def should_use_websocket_for_payload(payload):
    mode = current_responses_transport_mode()
    if mode == "http":
        return False
    if mode == "websocket":
        return True
    return bool(payload.get("_codex2gpt_enable_previous_response_id"))


def runtime_account_status_summary():
    summary = {}
    for account in STATE_DB.list_accounts():
        status = str(account.get("status") or "unknown")
        summary[status] = int(summary.get(status) or 0) + 1
    return summary


def runtime_warning_summary():
    warnings = STATE_DB.list_quota_warnings()
    summary = {"total": len(warnings), "warning": 0, "critical": 0}
    for item in warnings:
        level = str(item.get("level") or "warning")
        if level in summary:
            summary[level] += 1
    return summary


def advertised_model_catalog():
    plans = RUNTIME_SETTINGS.get("plans") if isinstance(RUNTIME_SETTINGS.get("plans"), dict) else {}
    return [
        {
            "id": model,
            "object": "model",
            "created": 1738368000,
            "owned_by": "openai",
            "type": "model",
            "display_name": model,
            "supported_plans": list(plans.get(model) or plans.get(spec["effective_model"]) or []),
        }
        for model, spec in advertised_model_entries()
    ]


def auth_file_metadata(auth_file):
    payload = read_json_file(auth_file, {})
    if not isinstance(payload, dict):
        payload = {}
    tokens = auth_tokens_from_payload(payload)
    id_claims = decode_jwt_payload(tokens.get("id_token", ""))
    account = {
        "entry_id": os.path.basename(auth_file),
        "auth_file": auth_file,
        "email": extract_auth_email(payload, id_claims),
        "user_id": extract_auth_user_id(payload, id_claims),
        "account_id": extract_auth_account_id(payload, id_claims),
        "plan_type": extract_auth_plan_type(payload, id_claims),
        "status": "active",
        "refresh_token": normalize_identifier(tokens.get("refresh_token")),
        "metadata": {
            "last_refresh": normalize_last_refresh(payload.get("last_refresh")) if payload.get("last_refresh") else "",
            "id_claims": id_claims,
            "identity_key": auth_identity_key(
                extract_auth_account_id(payload, id_claims),
                user_id=extract_auth_user_id(payload, id_claims),
                email=extract_auth_email(payload, id_claims),
            ),
        },
    }
    return account, payload


def sync_accounts_with_state():
    seen = set()
    if os.path.isdir(AUTH_DIR):
        for name in sorted(os.listdir(AUTH_DIR)):
            if not name.endswith(".json"):
                continue
            auth_file = os.path.join(AUTH_DIR, name)
            if not os.path.isfile(auth_file):
                continue
            account, raw_payload = auth_file_metadata(auth_file)
            existing = STATE_DB.get_account(account["entry_id"]) or {}
            STATE_DB.upsert_account(
                account["entry_id"],
                auth_file=auth_file,
                email=account["email"] or existing.get("email"),
                user_id=account["user_id"] or existing.get("user_id"),
                account_id=account["account_id"] or existing.get("account_id"),
                plan_type=account["plan_type"] or existing.get("plan_type"),
                status=existing.get("status") or account["status"],
                refresh_token=account["refresh_token"] or existing.get("refresh_token"),
                proxy_id=existing.get("proxy_id"),
                last_error=existing.get("last_error"),
                metadata={**(existing.get("metadata") or {}), **(account.get("metadata") or {}), "raw_keys": sorted(raw_payload.keys())},
                quota=existing.get("quota") or {},
                usage=existing.get("usage") or {"input_tokens": 0, "output_tokens": 0, "request_count": 0},
            )
            seen.add(account["entry_id"])
    return seen


def record_account_usage(account_name, response):
    if not account_name or not isinstance(response, dict):
        return
    usage = response.get("usage") if isinstance(response.get("usage"), dict) else {}
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    existing = STATE_DB.get_account(account_name) or {}
    current_usage = dict(existing.get("usage") or {})
    current_usage["input_tokens"] = int(current_usage.get("input_tokens") or 0) + input_tokens
    current_usage["output_tokens"] = int(current_usage.get("output_tokens") or 0) + output_tokens
    current_usage["request_count"] = int(current_usage.get("request_count") or 0) + 1
    STATE_DB.upsert_account(
        account_name,
        auth_file=existing.get("auth_file"),
        email=existing.get("email"),
        user_id=existing.get("user_id"),
        account_id=existing.get("account_id"),
        plan_type=existing.get("plan_type"),
        status="active",
        refresh_token=existing.get("refresh_token"),
        proxy_id=existing.get("proxy_id"),
        last_error=None,
        metadata=existing.get("metadata") or {},
        quota=existing.get("quota") or {},
        usage=current_usage,
    )
    STATE_DB.append_usage_snapshot(
        account_name,
        input_tokens=current_usage["input_tokens"],
        output_tokens=current_usage["output_tokens"],
        request_count=current_usage["request_count"],
        metadata={"response_id": response.get("id"), "model": response.get("model")},
    )


def set_account_status(account_name, status, *, last_error=None):
    if not account_name:
        return
    existing = STATE_DB.get_account(account_name) or {}
    STATE_DB.upsert_account(
        account_name,
        auth_file=existing.get("auth_file"),
        email=existing.get("email"),
        user_id=existing.get("user_id"),
        account_id=existing.get("account_id"),
        plan_type=existing.get("plan_type"),
        status=status,
        refresh_token=existing.get("refresh_token"),
        proxy_id=existing.get("proxy_id"),
        last_error=last_error,
        metadata=existing.get("metadata") or {},
        quota=existing.get("quota") or {},
        usage=existing.get("usage") or {},
    )


def first_enabled_relay_provider():
    for provider in STATE_DB.list_relay_providers(enabled_only=True):
        if str(provider.get("format") or "").strip() == "responses":
            return provider
    return None


def enabled_relay_provider():
    providers = STATE_DB.list_relay_providers(enabled_only=True)
    return providers[0] if providers else None


def oauth_account_by_name(entry_id):
    entry_id = str(entry_id or "").strip()
    if not entry_id:
        return None
    for account in pool.accounts:
        if account.name == entry_id:
            return account
    return None


def update_account_record(entry_id, **updates):
    existing = STATE_DB.get_account(entry_id) or {}
    STATE_DB.upsert_account(
        entry_id,
        auth_file=updates.get("auth_file", existing.get("auth_file")),
        email=updates.get("email", existing.get("email")),
        user_id=updates.get("user_id", existing.get("user_id")),
        account_id=updates.get("account_id", existing.get("account_id")),
        plan_type=updates.get("plan_type", existing.get("plan_type")),
        status=updates.get("status", existing.get("status", "active")),
        refresh_token=updates.get("refresh_token", existing.get("refresh_token")),
        proxy_id=updates.get("proxy_id", existing.get("proxy_id")),
        last_error=updates.get("last_error", existing.get("last_error")),
        metadata=updates.get("metadata", existing.get("metadata") or {}),
        quota=updates.get("quota", existing.get("quota") or {}),
        usage=updates.get("usage", existing.get("usage") or {}),
    )


def prune_expired_account_cookies(account_name, *, persist=True):
    account_name = str(account_name or "").strip()
    if not account_name:
        return {}
    cookies = COOKIE_STORE.get(account_name)
    if not isinstance(cookies, dict):
        return {}
    now = datetime.datetime.now(datetime.timezone.utc)
    cleaned = {}
    changed = False
    for key, value in cookies.items():
        if not isinstance(key, str) or not key.strip():
            changed = True
            continue
        current = value
        if isinstance(current, dict):
            expires_at = current.get("expires_at")
            if isinstance(expires_at, str) and expires_at.strip():
                try:
                    parsed = datetime.datetime.fromisoformat(expires_at)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
                    if parsed <= now:
                        changed = True
                        continue
                except ValueError:
                    current = {"value": current.get("value")}
                    changed = True
        elif current is None:
            changed = True
            continue
        cleaned[key] = current
    if cleaned != cookies:
        changed = True
    if changed:
        if cleaned:
            COOKIE_STORE[account_name] = cleaned
        else:
            COOKIE_STORE.pop(account_name, None)
        if persist:
            save_cookie_store(COOKIE_STORE)
    return cleaned


def account_cookie_header(account_name):
    cookies = prune_expired_account_cookies(account_name)
    if not isinstance(cookies, dict):
        return ""
    pairs = []
    for key, value in cookies.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, dict):
            value = value.get("value")
        if value is None:
            continue
        pairs.append(f"{key}={value}")
    return "; ".join(pairs)


def capture_set_cookie_headers(account_name, headers):
    if not account_name or headers is None:
        return
    values = []
    get_all = getattr(headers, "get_all", None)
    if callable(get_all):
        values = get_all("Set-Cookie") or []
    elif headers.get("Set-Cookie"):
        values = [headers.get("Set-Cookie")]
    if not values:
        return
    jar = dict(prune_expired_account_cookies(account_name, persist=False) or {})
    now = datetime.datetime.now(datetime.timezone.utc)
    for raw in values:
        raw = str(raw)
        first = raw.split(";", 1)[0]
        if "=" not in first:
            continue
        key, value = first.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        attrs = {"value": value}
        should_delete = value == ""
        for piece in raw.split(";")[1:]:
            piece = piece.strip()
            if not piece:
                continue
            if "=" in piece:
                attr_key, attr_value = piece.split("=", 1)
                attr_key = attr_key.strip().lower()
                attr_value = attr_value.strip()
            else:
                attr_key = piece.strip().lower()
                attr_value = ""
            if attr_key == "expires":
                expires_at = parse_cookie_expiry(attr_value)
                if expires_at:
                    attrs["expires_at"] = expires_at
            elif attr_key == "max-age":
                try:
                    max_age = int(attr_value)
                except (TypeError, ValueError):
                    continue
                if max_age <= 0:
                    should_delete = True
                else:
                    attrs["expires_at"] = (
                        now + datetime.timedelta(seconds=max_age)
                    ).astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat()
            elif attr_key in {"domain", "path", "samesite"} and attr_value:
                attrs[attr_key] = attr_value
            elif attr_key in {"secure", "httponly"}:
                attrs[attr_key] = True
        if should_delete:
            jar.pop(key, None)
            continue
        jar[key] = attrs
    if jar:
        COOKIE_STORE[account_name] = jar
    else:
        COOKIE_STORE.pop(account_name, None)
    save_cookie_store(COOKIE_STORE)


def get_account_proxy_mode(entry_id):
    account = STATE_DB.get_account(entry_id) or {}
    metadata = account.get("metadata") or {}
    mode = str(metadata.get("proxy_mode") or "global").strip().lower()
    if mode not in {"global", "direct", "auto", "specific"}:
        return "global"
    return mode


def set_account_proxy_mode(entry_id, mode, proxy_id=None):
    mode = str(mode or "global").strip().lower()
    if mode not in {"global", "direct", "auto", "specific"}:
        mode = "global"
    account = STATE_DB.get_account(entry_id) or {}
    metadata = dict(account.get("metadata") or {})
    metadata["proxy_mode"] = mode
    update_account_record(entry_id, metadata=metadata)
    if mode == "specific" and proxy_id:
        STATE_DB.assign_proxy(entry_id, proxy_id)
        update_account_record(entry_id, proxy_id=proxy_id)
    else:
        STATE_DB.delete_proxy_assignment(entry_id)
        update_account_record(entry_id, proxy_id=None)


def active_proxy_entries():
    return [proxy for proxy in STATE_DB.list_proxies() if str(proxy.get("status") or "") == "active"]


_auto_proxy_counter = {"value": 0}


def resolve_proxy_url_for_account(entry_id):
    if not entry_id:
        return GLOBAL_PROXY_URL or None
    mode = get_account_proxy_mode(entry_id)
    if mode == "direct":
        return None
    if mode == "specific":
        proxy_id = STATE_DB.get_proxy_assignment(entry_id)
        proxy = STATE_DB.get_proxy(proxy_id) if proxy_id else None
        return proxy.get("url") if proxy else (GLOBAL_PROXY_URL or None)
    if mode == "auto":
        proxies = active_proxy_entries()
        if not proxies:
            return GLOBAL_PROXY_URL or None
        index = _auto_proxy_counter["value"] % len(proxies)
        _auto_proxy_counter["value"] = (_auto_proxy_counter["value"] + 1) % len(proxies)
        return proxies[index].get("url")
    global_proxy_id = str((RUNTIME_SETTINGS.get("proxy_defaults") or {}).get("global_proxy_id") or "").strip()
    if global_proxy_id:
        proxy = STATE_DB.get_proxy(global_proxy_id)
        if proxy and str(proxy.get("status") or "") == "active":
            return proxy.get("url")
    return GLOBAL_PROXY_URL or None


def urlopen_with_optional_proxy(request, *, proxy_url=None, timeout=120):
    if current_transport_backend() == "curl_impersonate":
        response = curl_impersonate_request(
            request.full_url,
            method=request.get_method(),
            headers=dict(request.header_items()),
            body=getattr(request, "data", None),
            proxy_url=proxy_url,
            timeout=timeout,
        )
        if int(getattr(response, "status", 200)) >= 400:
            body = response.read()
            response.close()
            raise urllib.error.HTTPError(
                request.full_url,
                int(getattr(response, "status", 500)),
                "curl transport error",
                getattr(response, "headers", None),
                io.BytesIO(body),
            )
        return response
    if proxy_url:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        )
        return opener.open(request, timeout=timeout)
    return urllib.request.urlopen(request, timeout=timeout)


def urlopen_direct_with_optional_proxy(request, *, proxy_url=None, timeout=120):
    if proxy_url:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        )
        return opener.open(request, timeout=timeout)
    return urllib.request.urlopen(request, timeout=timeout)


def is_curl_pipe_error(exc):
    if isinstance(exc, BrokenPipeError):
        return True
    if isinstance(exc, OSError) and getattr(exc, "errno", None) == errno.EPIPE:
        return True
    return "Broken pipe" in str(exc)


def upstream_request_with_transport_fallback(request, *, proxy_url=None, timeout=120, account_name=""):
    try:
        response = urlopen_with_optional_proxy(request, proxy_url=proxy_url, timeout=timeout)
    except Exception as exc:
        if current_transport_backend() != "curl_impersonate" or not is_curl_pipe_error(exc):
            raise
        direct_response = urlopen_direct_with_optional_proxy(request, proxy_url=proxy_url, timeout=timeout)
        capture_set_cookie_headers(account_name, getattr(direct_response, "headers", None))
        return direct_response
    if current_transport_backend() != "curl_impersonate":
        return response
    first_chunk = response.read(1)
    if first_chunk:
        return PrefixedUpstreamResponse(response, first_chunk)
    response.close()
    direct_response = urlopen_direct_with_optional_proxy(request, proxy_url=proxy_url, timeout=timeout)
    capture_set_cookie_headers(account_name, getattr(direct_response, "headers", None))
    return direct_response


def load_json_with_transport_fallback(request, *, proxy_url=None, timeout=30, account_name=""):
    response = urlopen_with_optional_proxy(request, proxy_url=proxy_url, timeout=timeout)
    with response:
        capture_set_cookie_headers(account_name, getattr(response, "headers", None))
        body = response.read()
    if body.strip():
        try:
            payload = json.loads(body.decode("utf-8", errors="replace"))
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError:
            pass
    if current_transport_backend() != "curl_impersonate":
        preview = body[:160].decode("utf-8", errors="replace")
        raise RuntimeError(f"invalid json payload from upstream: {preview}")
    direct_response = urlopen_direct_with_optional_proxy(request, proxy_url=proxy_url, timeout=timeout)
    with direct_response:
        capture_set_cookie_headers(account_name, getattr(direct_response, "headers", None))
        direct_body = direct_response.read()
    try:
        payload = json.loads(direct_body.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as exc:
        preview = direct_body[:160].decode("utf-8", errors="replace")
        raise RuntimeError(f"invalid json payload from upstream: {preview}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("invalid json payload from upstream")
    return payload


def relay_endpoint_and_body(provider, codex_payload):
    relay_format = str(provider.get("format") or "responses").strip().lower()
    base_url = str(provider.get("base_url") or "").rstrip("/")
    headers = {"Authorization": f"Bearer {provider.get('api_key', '')}", "Content-Type": "application/json"}
    if relay_format == "responses":
        return f"{base_url}/v1/responses", codex_payload, headers, relay_format
    if relay_format == "openai_chat":
        body = codex_request_to_openai_chat(codex_payload)
        headers["Accept"] = "text/event-stream"
        return f"{base_url}/v1/chat/completions", body, headers, relay_format
    if relay_format == "anthropic":
        body = codex_request_to_anthropic(codex_payload)
        headers["anthropic-version"] = "2023-06-01"
        headers["x-api-key"] = str(provider.get("api_key") or "")
        headers.pop("Authorization", None)
        headers["Accept"] = "text/event-stream"
        return f"{base_url}/v1/messages", body, headers, relay_format
    if relay_format == "gemini":
        body = codex_request_to_gemini(codex_payload)
        model = urllib.parse.quote(str(codex_payload.get("model") or DEFAULT_MODEL), safe="")
        return f"{base_url}/v1beta/models/{model}:streamGenerateContent", body, headers, relay_format
    raise RuntimeError(f"unsupported relay format: {relay_format}")


def relay_stream_bytes_to_codex_sse(raw_body, relay_format, model_name):
    if relay_format == "openai_chat":
        chunks = list(stream_openai_chat_to_codex_sse([raw_body]))
    elif relay_format == "anthropic":
        chunks = list(stream_anthropic_to_codex_sse([raw_body]))
    elif relay_format == "gemini":
        chunks = list(stream_gemini_to_codex_sse([raw_body]))
    else:
        chunks = [raw_body.decode("utf-8", errors="replace") if isinstance(raw_body, bytes) else str(raw_body)]
    return "".join(chunks).encode("utf-8")


def perform_relay_request(provider, codex_payload, *, timeout=120):
    request_url, relay_body, headers, relay_format = relay_endpoint_and_body(provider, codex_payload)
    body_bytes = canonical_json_bytes(relay_body)
    request = urllib.request.Request(request_url, data=body_bytes, headers=headers, method="POST")
    with urlopen_with_optional_proxy(request, timeout=timeout) as response:
        raw_body = response.read()
        response_headers = getattr(response, "headers", None)
    status = getattr(response, "status", 200)
    if relay_format == "responses":
        return BufferedUpstreamResponse(raw_body, status=status, headers=response_headers)
    converted = relay_stream_bytes_to_codex_sse(raw_body, relay_format, str(codex_payload.get("model") or DEFAULT_MODEL))
    return BufferedUpstreamResponse(converted, status=status, headers=response_headers)


def fetch_account_quota(account):
    headers = build_default_desktop_headers()
    headers["Authorization"] = f"Bearer {account.access_token()}"
    headers["Accept"] = "application/json"
    headers.pop("Content-Type", None)
    cookie_header = account_cookie_header(account.name)
    if cookie_header:
        headers["Cookie"] = cookie_header
    request = urllib.request.Request(QUOTA_URL, headers=order_headers(headers), method="GET")
    proxy_url = resolve_proxy_url_for_account(account.name)
    payload = load_json_with_transport_fallback(request, proxy_url=proxy_url, timeout=30, account_name=account.name)
    if not isinstance(payload, dict):
        raise RuntimeError("invalid quota payload from upstream")
    return payload


def extract_quota_summary(raw_quota):
    if not isinstance(raw_quota, dict):
        return {}
    rate_limit = raw_quota.get("rate_limit") if isinstance(raw_quota.get("rate_limit"), dict) else {}
    primary_window = rate_limit.get("primary_window") if isinstance(rate_limit.get("primary_window"), dict) else {}
    secondary_window = rate_limit.get("secondary_window") if isinstance(rate_limit.get("secondary_window"), dict) else {}
    additional_limits = raw_quota.get("additional_rate_limits") if isinstance(raw_quota.get("additional_rate_limits"), list) else []
    code_review_rate_limit = raw_quota.get("code_review_rate_limit") if isinstance(raw_quota.get("code_review_rate_limit"), dict) else {}
    windows = []

    def append_window(candidate):
        if isinstance(candidate, dict) and isinstance(candidate.get("limit_window_seconds"), (int, float)):
            windows.append(candidate)

    append_window(primary_window)
    append_window(secondary_window)
    for limit in additional_limits:
        if not isinstance(limit, dict):
            continue
        nested_rate_limit = limit.get("rate_limit") if isinstance(limit.get("rate_limit"), dict) else {}
        append_window(nested_rate_limit.get("primary_window"))
        append_window(nested_rate_limit.get("secondary_window"))

    def nearest_window(target_seconds):
        if not windows:
            return {}
        return min(
            windows,
            key=lambda window: abs(int(window.get("limit_window_seconds") or 0) - target_seconds),
        )

    display_primary = nearest_window(5 * 60 * 60) or primary_window
    display_secondary = nearest_window(7 * 24 * 60 * 60) or secondary_window
    return {
        "user_id": raw_quota.get("user_id"),
        "account_id": raw_quota.get("account_id"),
        "email": raw_quota.get("email"),
        "plan_type": raw_quota.get("plan_type"),
        "allowed": rate_limit.get("allowed"),
        "limit_reached": rate_limit.get("limit_reached"),
        "used_percent": display_primary.get("used_percent"),
        "reset_at": display_primary.get("reset_at"),
        "reset_after_seconds": display_primary.get("reset_after_seconds"),
        "limit_window_seconds": display_primary.get("limit_window_seconds"),
        "secondary_rate_limit": {
            "used_percent": display_secondary.get("used_percent"),
            "reset_at": display_secondary.get("reset_at"),
            "reset_after_seconds": display_secondary.get("reset_after_seconds"),
            "limit_window_seconds": display_secondary.get("limit_window_seconds"),
            "limit_reached": False,
        }
        if display_secondary
        else {},
        "rate_limit": {
            "allowed": rate_limit.get("allowed"),
            "limit_reached": rate_limit.get("limit_reached"),
            "primary_window": primary_window,
            "secondary_window": secondary_window,
        },
        "code_review_rate_limit": code_review_rate_limit,
        "credits": raw_quota.get("credits") if isinstance(raw_quota.get("credits"), dict) else {},
        "additional_rate_limits": additional_limits,
        "raw": raw_quota,
    }


def quota_used_percent_fraction(value):
    if not isinstance(value, (int, float)):
        return None
    numeric = float(value)
    if numeric > 1:
        numeric /= 100.0
    return max(0.0, min(1.0, numeric))


def update_quota_warning_state(entry_id, quota_summary):
    thresholds = RUNTIME_SETTINGS.get("quota") or {}
    warning_threshold = float(thresholds.get("warning_threshold") or 0.9)
    used_percent = quota_summary.get("used_percent")
    used_fraction = quota_used_percent_fraction(used_percent)
    warnings = []
    if used_fraction is not None:
        if used_fraction >= 1:
            warnings.append(
                {
                    "level": "critical",
                    "warning_type": "quota_exhausted",
                    "message": f"{entry_id} quota exhausted",
                    "used_percent": used_percent,
                }
            )
        elif used_fraction >= warning_threshold:
            warnings.append(
                {
                    "level": "warning",
                    "warning_type": "quota_high",
                    "message": f"{entry_id} quota usage is high",
                    "used_percent": used_percent,
                }
            )
    STATE_DB.set_quota_warnings(entry_id, warnings)
    return warnings


def refresh_account_quota(account):
    raw_quota = fetch_account_quota(account)
    summary = extract_quota_summary(raw_quota)
    existing = STATE_DB.get_account(account.name) or {}
    update_account_record(
        account.name,
        email=summary.get("email") or existing.get("email"),
        user_id=summary.get("user_id") or existing.get("user_id"),
        account_id=summary.get("account_id") or existing.get("account_id"),
        plan_type=summary.get("plan_type") or existing.get("plan_type"),
        status="rate_limited" if summary.get("limit_reached") else "active",
        quota=summary,
        last_error=None,
    )
    update_quota_warning_state(account.name, summary)
    return summary


def refresh_all_account_quotas():
    results = []
    errors = []
    for account in pool.accounts:
        try:
            results.append({"entry_id": account.name, "quota": refresh_account_quota(account)})
        except urllib.error.HTTPError as exc:
            status = "expired" if exc.code == 401 else "banned" if exc.code == 403 else "error"
            update_account_record(account.name, status=status, last_error=str(exc))
            errors.append({"entry_id": account.name, "error": str(exc), "status_code": exc.code})
        except Exception as exc:
            update_account_record(account.name, status="error", last_error=str(exc))
            errors.append({"entry_id": account.name, "error": str(exc)})
    return {"results": results, "errors": errors}


def diagnose_proxy_connection(proxy_id):
    proxy_id = str(proxy_id or "").strip()
    proxy = STATE_DB.get_proxy(proxy_id) if proxy_id else None
    if proxy is None:
        raise RuntimeError("proxy not found")
    started = time.perf_counter()
    result = proxy_health_check(proxy)
    return {
        "ok": str(result.get("status") or "") == "active",
        "target": "proxy",
        "proxy_id": proxy_id,
        "latency_ms": int((time.perf_counter() - started) * 1000),
        "result": result,
    }


def diagnose_account_connection(account_id=""):
    sync_accounts_with_state()
    account = oauth_account_by_name(account_id) if account_id else (pool.accounts[0] if pool.accounts else None)
    if account is None:
        raise RuntimeError("no oauth account available")
    started = time.perf_counter()
    summary = refresh_account_quota(account)
    return {
        "ok": not bool(summary.get("limit_reached")),
        "target": "account",
        "account_id": account.name,
        "latency_ms": int((time.perf_counter() - started) * 1000),
        "proxy_mode": get_account_proxy_mode(account.name),
        "proxy_url": resolve_proxy_url_for_account(account.name),
        "quota": summary,
    }


def diagnose_relay_connection(provider_id=""):
    provider = STATE_DB.get_relay_provider(provider_id) if provider_id else enabled_relay_provider()
    if provider is None:
        raise RuntimeError("no relay provider available")
    payload = normalize_payload({"model": DEFAULT_MODEL, "input": "ping"})
    payload["stream"] = True
    started = time.perf_counter()
    with perform_relay_request(provider, payload, timeout=30) as response:
        body = response.read().decode("utf-8", errors="replace")
    final = extract_final_response(body)
    return {
        "ok": final is not None,
        "target": "relay",
        "provider_id": str(provider.get("provider_id") or ""),
        "relay_format": str(provider.get("format") or ""),
        "latency_ms": int((time.perf_counter() - started) * 1000),
        "response_id": final.get("id") if isinstance(final, dict) else None,
        "preview": (response_output_text(final) if isinstance(final, dict) else body[:160]).strip()[:160],
    }


def run_connection_diagnostics(payload=None):
    payload = payload if isinstance(payload, dict) else {}
    if payload.get("proxy_id"):
        return diagnose_proxy_connection(payload.get("proxy_id"))
    if payload.get("relay_provider_id"):
        return diagnose_relay_connection(payload.get("relay_provider_id"))
    if payload.get("account_id"):
        return diagnose_account_connection(payload.get("account_id"))
    if pool.accounts:
        return diagnose_account_connection()
    if enabled_relay_provider() is not None:
        return diagnose_relay_connection()
    raise RuntimeError("no account or relay provider available for diagnostics")


def proxy_health_check(proxy):
    request = urllib.request.Request(IPIFY_URL, headers={"Accept": "application/json"}, method="GET")
    proxy_url = str(proxy.get("url") or "").strip() or None
    started_at = time.time()
    try:
        with urlopen_with_optional_proxy(request, proxy_url=proxy_url, timeout=10) as response:
            payload = json.load(response)
        latency_ms = int((time.time() - started_at) * 1000)
        health = {
            "exit_ip": payload.get("ip") if isinstance(payload, dict) else None,
            "latency_ms": latency_ms,
            "last_checked": now_iso(),
            "error": None,
        }
        updated = STATE_DB.upsert_proxy(
            proxy["proxy_id"],
            name=str(proxy.get("name") or proxy["proxy_id"]),
            url=str(proxy.get("url") or ""),
            status="active",
            health=health,
            metadata=proxy.get("metadata") if isinstance(proxy.get("metadata"), dict) else {},
        )
        return updated
    except Exception as exc:
        health = {
            "exit_ip": None,
            "latency_ms": None,
            "last_checked": now_iso(),
            "error": str(exc),
        }
        updated = STATE_DB.upsert_proxy(
            proxy["proxy_id"],
            name=str(proxy.get("name") or proxy["proxy_id"]),
            url=str(proxy.get("url") or ""),
            status="unreachable",
            health=health,
            metadata=proxy.get("metadata") if isinstance(proxy.get("metadata"), dict) else {},
        )
        return updated


def exchange_pkce_code(code, code_verifier, redirect_uri):
    body = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "code": code,
            "redirect_uri": redirect_uri,
            "code_verifier": code_verifier,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        TOKEN_URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def persist_oauth_tokens(tokens):
    id_claims = decode_jwt_payload(tokens.get("id_token", ""))
    email = str(id_claims.get("email") or tokens.get("email") or "").strip()
    sub = str(id_claims.get("sub") or tokens.get("sub") or "").strip()
    filename_root = email or sub or f"oauth_{int(time.time())}"
    safe_root = re.sub(r"[^A-Za-z0-9._-]+", "_", filename_root).strip("._-") or f"oauth_{int(time.time())}"
    auth_file = os.path.join(AUTH_DIR, f"{safe_root}.json")
    payload = read_json_file(auth_file, {})
    payload["email"] = email
    if sub:
        payload["user_id"] = sub
    payload["tokens"] = {
        **auth_tokens_from_payload(payload),
        **{key: value for key, value in tokens.items() if key in {"access_token", "refresh_token", "id_token", "token_type", "expires_in", "account_id"}},
    }
    payload["plan_type"] = extract_auth_plan_type(payload, id_claims)
    payload["last_refresh"] = normalize_last_refresh(time.time())
    payload = write_codex_auth_file(auth_file, payload)
    pool.reload()
    sync_accounts_with_state()
    return os.path.basename(auth_file), payload


def refresh_account_quota_by_entry_id(entry_id):
    account = oauth_account_by_name(entry_id)
    if account is None:
        return None
    return refresh_account_quota(account)


TOKEN_REFRESH_BACKOFF = {}
OAUTH_CALLBACK_SERVER = None
OAUTH_CALLBACK_THREAD = None
OAUTH_CALLBACK_SERVER_LOCK = threading.RLock()


def oauth_redirect_uri():
    return f"http://{OAUTH_CALLBACK_REDIRECT_HOST}:{OAUTH_CALLBACK_PORT}/auth/callback"


def oauth_pending_session():
    session = RUNTIME_SETTINGS.get("oauth_pkce")
    return session if isinstance(session, dict) else {}


def oauth_session_expired(session):
    created_at = str((session or {}).get("created_at") or "").strip()
    if not created_at:
        return False
    try:
        created = datetime.datetime.fromisoformat(created_at)
    except ValueError:
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=datetime.timezone.utc)
    age_seconds = (datetime.datetime.now(datetime.timezone.utc) - created.astimezone(datetime.timezone.utc)).total_seconds()
    return age_seconds > max(60, OAUTH_CALLBACK_TTL_SECONDS)


def oauth_completed_state_matches(state):
    session = oauth_pending_session()
    return str(state or "").strip() and str(session.get("completed_state") or "").strip() == str(state or "").strip()


def clear_oauth_pending_session(*, completed_state=""):
    RUNTIME_SETTINGS["oauth_pkce"] = {"completed_state": completed_state, "completed_at": now_iso()} if completed_state else {}
    save_runtime_settings(RUNTIME_SETTINGS)


def oauth_callback_result_html(success, title, message):
    event_type = "oauth-callback-success" if success else "oauth-callback-error"
    payload = {"type": event_type}
    if not success:
        payload["error"] = str(message)
    script = [
        "<script>",
        "(function(){",
        f"var payload = {json.dumps(payload, ensure_ascii=False)};",
        "try { if (window.opener) { window.opener.postMessage(payload, '*'); } } catch (err) {}",
    ]
    if success:
        script.append("setTimeout(function(){ try { window.close(); } catch (err) {} }, 600);")
    script.extend(["})();", "</script>"])
    accent = "#1f8f5f" if success else "#b34040"
    title_text = "Login Successful" if success else "Login Failed"
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{title_text}</title>
    <style>
      body {{
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        background: linear-gradient(180deg, #f6faf7, #e8efe7);
        color: #183126;
      }}
      .card {{
        width: min(560px, calc(100% - 32px));
        padding: 28px;
        border-radius: 24px;
        background: rgba(255, 255, 255, 0.94);
        border: 1px solid rgba(24, 49, 38, 0.1);
        box-shadow: 0 18px 50px rgba(24, 49, 38, 0.08);
      }}
      h1 {{
        margin: 0 0 10px;
        color: {accent};
        font-size: 1.6rem;
      }}
      p {{
        margin: 0;
        line-height: 1.55;
        white-space: pre-wrap;
        word-break: break-word;
      }}
    </style>
  </head>
  <body>
    <article class="card">
      <h1>{title}</h1>
      <p>{escape_html_text(message)}</p>
    </article>
    {''.join(script)}
  </body>
</html>"""


def escape_html_text(value):
    return (
        str(value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def complete_oauth_callback(code, state):
    pending = oauth_pending_session()
    state = str(state or "").strip()
    code = str(code or "").strip()
    if not code or not state:
        raise ProxyError(400, "invalid_request_error", "Missing code or state parameter")
    if pending and oauth_session_expired(pending):
        clear_oauth_pending_session()
        pending = {}
    if not pending or state != str(pending.get("state") or "").strip():
        if oauth_completed_state_matches(state):
            return {"completed_state": state, "already_completed": True}
        raise ProxyError(400, "invalid_request_error", "Invalid or expired OAuth session. Please try again.")
    try:
        redirect_uri = str(pending.get("redirect_uri") or oauth_redirect_uri())
        tokens = exchange_pkce_code(code, str(pending.get("verifier") or ""), redirect_uri)
        filename, account_payload = persist_oauth_tokens(tokens)
        quota = None
        quota_error = None
        try:
            quota = refresh_account_quota_by_entry_id(filename)
        except Exception as exc:
            quota_error = str(exc)
        clear_oauth_pending_session(completed_state=state)
        return {
            "filename": filename,
            "account_payload": account_payload,
            "redirect_uri": redirect_uri,
            "completed_state": state,
            "already_completed": False,
            "quota": quota,
            "quota_error": quota_error,
        }
    except ProxyError:
        raise
    except Exception as exc:
        raise ProxyError(502, "upstream_error", str(exc)) from exc


def stop_oauth_callback_server():
    global OAUTH_CALLBACK_SERVER, OAUTH_CALLBACK_THREAD
    with OAUTH_CALLBACK_SERVER_LOCK:
        server = OAUTH_CALLBACK_SERVER
        thread = OAUTH_CALLBACK_THREAD
        OAUTH_CALLBACK_SERVER = None
        OAUTH_CALLBACK_THREAD = None
    if server is not None:
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            server.server_close()
        except Exception:
            pass
    if thread is not None and thread.is_alive() and thread is not threading.current_thread():
        thread.join(timeout=1)


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        return

    def _write_html(self, status, html):
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path != "/auth/callback":
            self._write_html(404, "<html><body><h1>Not found</h1></body></html>")
            return
        params = urllib.parse.parse_qs(parsed.query)
        error = (params.get("error") or [""])[0]
        error_description = (params.get("error_description") or [""])[0]
        if error:
            self._write_html(400, oauth_callback_result_html(False, "Login Failed", error_description or error))
            threading.Thread(target=stop_oauth_callback_server, daemon=True).start()
            return
        code = (params.get("code") or [""])[0]
        state = (params.get("state") or [""])[0]
        try:
            result = complete_oauth_callback(code, state)
            if result.get("already_completed"):
                html = oauth_callback_result_html(True, "Login Successful", "This login session was already completed.")
            else:
                email = ((result.get("account_payload") or {}).get("email") or "unknown").strip() or "unknown"
                html = oauth_callback_result_html(True, "OAuth login complete", f"Saved account {result['filename']} ({email}).")
            self._write_html(200, html)
            threading.Thread(target=stop_oauth_callback_server, daemon=True).start()
        except ProxyError as exc:
            self._write_html(exc.status, oauth_callback_result_html(False, "Login Failed", exc.message))


def start_oauth_callback_server():
    global OAUTH_CALLBACK_SERVER, OAUTH_CALLBACK_THREAD
    stop_oauth_callback_server()
    try:
        server = ThreadingHTTPServer((OAUTH_CALLBACK_BIND_HOST, OAUTH_CALLBACK_PORT), OAuthCallbackHandler)
    except OSError as exc:
        return {"ok": False, "error": str(exc), "redirect_uri": oauth_redirect_uri()}
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    with OAUTH_CALLBACK_SERVER_LOCK:
        OAUTH_CALLBACK_SERVER = server
        OAUTH_CALLBACK_THREAD = thread
    thread.start()

    def auto_stop():
        time.sleep(max(30, OAUTH_CALLBACK_TTL_SECONDS))
        with OAUTH_CALLBACK_SERVER_LOCK:
            current = OAUTH_CALLBACK_SERVER
        if current is server:
            stop_oauth_callback_server()

    threading.Thread(target=auto_stop, daemon=True).start()
    return {"ok": True, "error": "", "redirect_uri": oauth_redirect_uri()}


atexit.register(stop_oauth_callback_server)


def refresh_accounts_if_needed(force=False):
    refreshed = []
    now_ts = int(time.time())
    for account in pool.accounts:
        data = read_json_file(account.auth_file, {})
        tokens = data.get("tokens") if isinstance(data.get("tokens"), dict) else {}
        claims = decode_jwt_payload(tokens.get("access_token") or tokens.get("id_token") or "")
        exp = int(claims.get("exp") or 0)
        backoff = TOKEN_REFRESH_BACKOFF.get(account.name, {})
        retry_after = int(backoff.get("retry_after") or 0)
        if not force:
            if retry_after and now_ts < retry_after:
                continue
            if exp and exp - now_ts > TOKEN_REFRESH_MARGIN_SECONDS:
                continue
        try:
            account.refresh_access_token()
            TOKEN_REFRESH_BACKOFF.pop(account.name, None)
            update_account_record(account.name, status="active", last_error=None)
            refreshed.append(account.name)
        except Exception as exc:
            attempts = int(backoff.get("attempts") or 0) + 1
            delay = min(3600, 2 ** min(attempts, 10))
            TOKEN_REFRESH_BACKOFF[account.name] = {"attempts": attempts, "retry_after": now_ts + delay}
            update_account_record(account.name, status="expired", last_error=str(exc))
    return refreshed


def refresh_fingerprint_cache(force=False):
    updated = dict(FINGERPRINT_CACHE)
    changed = False
    if FINGERPRINT_SOURCE_URL:
        request = urllib.request.Request(FINGERPRINT_SOURCE_URL, headers={"Accept": "application/json"}, method="GET")
        try:
            with urlopen_with_optional_proxy(request, timeout=15) as response:
                payload = json.load(response)
            if isinstance(payload, dict):
                for key in ("app_version", "build_number", "chromium_version", "header_order", "default_headers"):
                    if key in payload:
                        updated[key] = payload[key]
                        changed = True
        except Exception:
            if force:
                raise
    if not changed:
        updated["transport_backend"] = current_transport_backend()
    updated["updated_at"] = now_iso()
    FINGERPRINT_CACHE.clear()
    FINGERPRINT_CACHE.update(updated)
    write_json_file(FINGERPRINT_CACHE_PATH, FINGERPRINT_CACHE)
    return dict(FINGERPRINT_CACHE)


class BackgroundJobRunner:
    def __init__(self):
        self.lock = threading.Lock()
        self.threads = {}
        self.stop_event = threading.Event()
        self.last_run = {}
        self.last_error = {}

    def start(self, name, interval_seconds, fn):
        interval_seconds = max(5, int(interval_seconds))
        with self.lock:
            if name in self.threads:
                return

            def loop():
                while not self.stop_event.wait(interval_seconds):
                    try:
                        fn()
                        self.last_run[name] = now_iso()
                        self.last_error.pop(name, None)
                    except Exception as exc:
                        self.last_error[name] = str(exc)

            thread = threading.Thread(target=loop, name=f"codex2gpt-{name}", daemon=True)
            self.threads[name] = {"thread": thread, "interval_seconds": interval_seconds}
            thread.start()

    def stop(self):
        self.stop_event.set()

    def snapshot(self):
        with self.lock:
            return {
                "jobs": {
                    name: {
                        "interval_seconds": info["interval_seconds"],
                        "last_run": self.last_run.get(name),
                        "last_error": self.last_error.get(name),
                    }
                    for name, info in self.threads.items()
                }
            }


BACKGROUND_JOBS = BackgroundJobRunner()
atexit.register(BACKGROUND_JOBS.stop)


class RecordingUpstreamReader:
    def __init__(self, upstream):
        self.upstream = upstream
        self.chunks = []
        self.status = getattr(upstream, "status", 200)

    def read(self, size=-1):
        chunk = self.upstream.read(size)
        if chunk:
            self.chunks.append(chunk)
        return chunk

    def body_text(self):
        return b"".join(self.chunks).decode("utf-8", errors="replace")


class CurlProcessResponse:
    def __init__(self, process, status, headers, buffered_body):
        self.process = process
        self.status = status
        self.headers = headers
        self._buffer = buffered_body
        self._closed = False

    def read(self, size=-1):
        if self._closed:
            return b""
        if size is None or size < 0:
            remaining = self._buffer + (self.process.stdout.read() if self.process.stdout else b"")
            self._buffer = b""
            return remaining
        if len(self._buffer) >= size:
            chunk = self._buffer[:size]
            self._buffer = self._buffer[size:]
            return chunk
        chunk = self._buffer
        self._buffer = b""
        if self.process.stdout:
            more = self.process.stdout.read(size - len(chunk))
            if more:
                chunk += more
        return chunk

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self.process.stdout:
            self.process.stdout.close()
        if self.process.stderr:
            self.process.stderr.close()
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.process.kill()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


class BufferedUpstreamResponse:
    def __init__(self, body: bytes, status: int = 200, headers: dict[str, Any] | None = None):
        self._body = body
        self._offset = 0
        self.status = status
        self.headers = headers or {}

    def read(self, size=-1):
        if size is None or size < 0:
            size = len(self._body) - self._offset
        chunk = self._body[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class PrefixedUpstreamResponse:
    def __init__(self, upstream, prefix: bytes):
        self.upstream = upstream
        self.status = getattr(upstream, "status", 200)
        self.headers = getattr(upstream, "headers", {})
        self._prefix = prefix or b""
        self._offset = 0

    def read(self, size=-1):
        if size is None or size < 0:
            prefix = self._prefix[self._offset :]
            self._offset = len(self._prefix)
            return prefix + self.upstream.read()
        chunk = b""
        if self._offset < len(self._prefix):
            remaining = len(self._prefix) - self._offset
            take = min(size, remaining)
            chunk = self._prefix[self._offset : self._offset + take]
            self._offset += take
            size -= take
        if size > 0:
            chunk += self.upstream.read(size)
        return chunk

    def close(self):
        close = getattr(self.upstream, "close", None)
        if callable(close):
            close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def websocket_sync_connect():
    from websockets.sync.client import connect

    return connect


def websocket_transport_available():
    try:
        websocket_sync_connect()
    except Exception:
        return False
    return True


def build_websocket_request(payload):
    request = {
        "type": "response.create",
        "model": str(payload.get("model") or DEFAULT_MODEL),
        "instructions": str(payload.get("instructions") or ""),
        "input": payload.get("input") or [],
    }
    for key in ("previous_response_id", "reasoning", "tools", "tool_choice", "text"):
        value = payload.get(key)
        if value not in (None, "", [], {}):
            request[key] = value
    return request


def build_websocket_url(url):
    if url.startswith("https://"):
        return "wss://" + url[len("https://") :]
    if url.startswith("http://"):
        return "ws://" + url[len("http://") :]
    return url


def build_websocket_headers(account):
    headers = build_default_desktop_headers()
    headers["OpenAI-Beta"] = "responses_websockets=2026-02-06"
    headers["x-openai-internal-codex-residency"] = "us"
    headers["Authorization"] = f"Bearer {account.access_token()}"
    cookie_header = account_cookie_header(account.name)
    if cookie_header:
        headers["Cookie"] = cookie_header
    headers.pop("Content-Type", None)
    headers.pop("Accept", None)
    return order_headers(headers)


class WebSocketSSEUpstreamResponse:
    def __init__(self, ws_url, headers, request_payload, *, proxy_url=None, timeout=120):
        self.status = 200
        self.headers = {"Content-Type": "text/event-stream; charset=utf-8"}
        self._buffer = bytearray()
        self._offset = 0
        self._done = False
        self._cond = threading.Condition()
        self._websocket = self._connect(ws_url, headers, request_payload, proxy_url, timeout)
        self._thread = threading.Thread(
            target=self._run,
            name="codex2gpt-websocket-upstream",
            daemon=True,
        )
        self._thread.start()

    def _append(self, chunk):
        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8")
        with self._cond:
            self._buffer.extend(chunk)
            self._cond.notify_all()

    def _finish(self):
        with self._cond:
            self._done = True
            self._cond.notify_all()

    def _connect(self, ws_url, headers, request_payload, proxy_url, timeout):
        connect = websocket_sync_connect()
        ws_headers = dict(headers)
        origin = ws_headers.pop("Origin", None)
        connect_kwargs = {
            "additional_headers": list(ws_headers.items()),
            "user_agent_header": None,
            "proxy": proxy_url or None,
            "open_timeout": min(float(timeout), 15.0),
            "close_timeout": 5,
            "max_size": None,
        }
        if origin:
            connect_kwargs["origin"] = origin
        websocket = connect(ws_url, **connect_kwargs)
        try:
            websocket.send(json.dumps(request_payload, ensure_ascii=False))
        except Exception:
            close = getattr(websocket, "close", None)
            if callable(close):
                close()
            raise
        return websocket

    def _run(self):
        try:
            while True:
                raw = self._websocket.recv()
                if raw is None:
                    break
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    self._append(f"data: {raw}\n\n")
                    continue
                event_type = str(payload.get("type") or "message")
                self._append(f"event: {event_type}\ndata: {raw}\n\n")
                if event_type in {"response.completed", "response.failed", "error"}:
                    break
        except Exception as exc:
            error_payload = json.dumps(
                {
                    "type": "error",
                    "error": {"message": str(exc)},
                },
                ensure_ascii=False,
            )
            self._append(f"event: error\ndata: {error_payload}\n\n")
        finally:
            close = getattr(self._websocket, "close", None)
            if callable(close):
                close()
            self._finish()

    def read(self, size=-1):
        with self._cond:
            while self._offset >= len(self._buffer) and not self._done:
                self._cond.wait(timeout=0.1)
            if self._offset >= len(self._buffer) and self._done:
                return b""
            if size is None or size < 0:
                size = len(self._buffer) - self._offset
            chunk = bytes(self._buffer[self._offset : self._offset + size])
            self._offset += len(chunk)
            return chunk

    def close(self):
        close = getattr(self._websocket, "close", None)
        if callable(close):
            close()
        self._thread.join(timeout=1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def _read_until_header_end(stream):
    buffer = b""
    while b"\r\n\r\n" not in buffer and b"\n\n" not in buffer:
        chunk = stream.read(1)
        if not chunk:
            break
        buffer += chunk
        if len(buffer) > 65536:
            break
    separator = b"\r\n\r\n" if b"\r\n\r\n" in buffer else b"\n\n"
    if separator in buffer:
        header_bytes, body = buffer.split(separator, 1)
    else:
        header_bytes, body = buffer, b""
    return header_bytes.decode("utf-8", errors="replace"), body


def _parse_header_block(header_text):
    lines = [line for line in header_text.splitlines() if line.strip()]
    status = 200
    headers = {}
    for line in lines:
        if line.upper().startswith("HTTP/"):
            parts = line.split()
            if len(parts) >= 2:
                try:
                    status = int(parts[1])
                except ValueError:
                    status = 200
            headers = {}
            continue
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key.strip()] = value.strip()
    return status, headers


def _read_curl_response_headers(stream):
    buffered_body = b""
    headers = {}
    status = 200
    while True:
        header_text, buffered_body = _read_until_header_end(stream)
        status, headers = _parse_header_block(header_text)
        if status >= 200 and status not in {204, 304}:
            break
        if not header_text:
            break
        if status >= 200:
            break
    return status, headers, buffered_body


def curl_impersonate_request(url, *, method="GET", headers=None, body=None, proxy_url=None, timeout=120):
    binary = find_curl_impersonate_binary()
    if not binary:
        raise RuntimeError("curl-impersonate binary not available")
    command = [binary, "--silent", "--show-error", "--no-buffer", "--include", "--location", "--compressed", "--max-time", str(int(timeout))]
    if os.path.basename(binary) == "curl":
        command.extend(["--impersonate", "chrome"])
    command.extend(["-X", method.upper(), url])
    ordered_headers = order_headers(headers or {})
    for key, value in ordered_headers.items():
        if str(key).lower() == "accept-encoding":
            continue
        command.extend(["-H", f"{key}: {value}"])
    command.extend(["-H", "Expect:"])
    if proxy_url:
        command.extend(["--proxy", proxy_url])
    if body is not None:
        if isinstance(body, str):
            body = body.encode("utf-8")
        command.extend(["--data-binary", "@-"])
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE if body is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if body is not None and process.stdin:
        process.stdin.write(body)
        process.stdin.close()
    if process.stdout is None:
        raise RuntimeError("failed to open curl stdout")
    status, parsed_headers, buffered_body = _read_curl_response_headers(process.stdout)
    if status >= 400 and not buffered_body:
        stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr else ""
        if stderr:
            buffered_body = stderr.encode("utf-8")
    return CurlProcessResponse(process, status, parsed_headers, buffered_body)


def transcript_error_body(body):
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return body
    return body


def transcript_text_from_input_items(input_items):
    chunks = []
    for item in input_items or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "message":
            for content in item.get("content") or []:
                if not isinstance(content, dict):
                    continue
                content_type = content.get("type")
                if content_type in {"input_text", "output_text"} and isinstance(content.get("text"), str):
                    chunks.append(content["text"])
                elif content_type == "refusal" and isinstance(content.get("refusal"), str):
                    chunks.append(content["refusal"])
        elif item_type == "function_call_output":
            output = item.get("output")
            if isinstance(output, str):
                chunks.append(output)
    return "\n\n".join(chunk for chunk in chunks if chunk)


def anthropic_message_output_text(message_payload):
    chunks = []
    for block in message_payload.get("content") or []:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text" and isinstance(block.get("text"), str):
            chunks.append(block["text"])
    return "".join(chunks)


def anthropic_message_tool_uses(message_payload):
    return [
        dict(block)
        for block in message_payload.get("content") or []
        if isinstance(block, dict) and block.get("type") == "tool_use"
    ]


def chat_completion_output_text(completion_payload):
    choices = completion_payload.get("choices") or []
    if not choices:
        return ""
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content
    return ""


def chat_completion_tool_calls(completion_payload):
    choices = completion_payload.get("choices") or []
    if not choices:
        return []
    message = choices[0].get("message") or {}
    tool_calls = message.get("tool_calls") or []
    return [dict(tool_call) for tool_call in tool_calls if isinstance(tool_call, dict)]


def transcript_request_section(requested_model, payload, raw_payload):
    return {
        "requested_model": requested_model,
        "effective_model": payload.get("model"),
        "stream": bool(raw_payload.get("stream")) if isinstance(raw_payload, dict) else False,
        "prompt_cache_key": payload.get("prompt_cache_key"),
        "input_text": transcript_text_from_input_items(payload.get("input") or []),
        "input_items": payload.get("input") or [],
        "request_payload": raw_payload,
    }


def transcript_response_section_for_responses(response):
    return {
        "response_id": response.get("id"),
        "output_text": response_output_text(response),
        "tool_calls": response_output_tool_calls(response),
        "usage": response.get("usage") or {},
        "response_payload": response,
    }


def transcript_response_section_for_chat_completion(completion_payload):
    return {
        "response_id": completion_payload.get("id"),
        "output_text": chat_completion_output_text(completion_payload),
        "tool_calls": chat_completion_tool_calls(completion_payload),
        "usage": completion_payload.get("usage") or {},
        "response_payload": completion_payload,
    }


def transcript_response_section_for_anthropic(message_payload):
    return {
        "response_id": message_payload.get("id"),
        "output_text": anthropic_message_output_text(message_payload),
        "tool_calls": anthropic_message_tool_uses(message_payload),
        "usage": message_payload.get("usage") or {},
        "response_payload": message_payload,
    }


def usage_prompt_tokens(usage):
    if not isinstance(usage, dict):
        return 0
    return int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)


def usage_cached_tokens(usage):
    if not isinstance(usage, dict):
        return 0
    prompt_details = usage.get("prompt_tokens_details") if isinstance(usage.get("prompt_tokens_details"), dict) else {}
    input_details = usage.get("input_tokens_details") if isinstance(usage.get("input_tokens_details"), dict) else {}
    return int(
        prompt_details.get("cached_tokens")
        or input_details.get("cached_tokens")
        or usage.get("cache_read_input_tokens")
        or usage.get("cached_tokens")
        or 0
    )


def usage_cache_hit_rate(usage):
    prompt_tokens = usage_prompt_tokens(usage)
    cached_tokens = usage_cached_tokens(usage)
    if prompt_tokens <= 0:
        return None
    return round((cached_tokens / prompt_tokens) * 100, 2)


def recent_request_entry(path, requested_model, account_name, status, usage=None, error=None):
    usage = usage if isinstance(usage, dict) else {}
    return {
        "timestamp": now_iso(),
        "path": path,
        "requested_model": requested_model,
        "account_name": account_name,
        "status": status,
        "prompt_tokens": usage_prompt_tokens(usage),
        "completion_tokens": int(usage.get("completion_tokens") or usage.get("output_tokens") or 0),
        "cached_tokens": usage_cached_tokens(usage),
        "cache_hit_rate": usage_cache_hit_rate(usage),
        "error": error,
    }


def normalize_input(value):
    if isinstance(value, str):
        return [
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": value}],
            }
        ]
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return value
    raise ValueError("input must be a string, object, or list")


def strip_unsupported_top_level_fields(payload):
    normalized = dict(payload)
    for key in UNSUPPORTED_TOP_LEVEL_FIELDS:
        normalized.pop(key, None)
    return normalized


def normalize_response_format(format_value):
    if not is_record(format_value):
        return None
    format_type = str(format_value.get("type") or "").strip()
    if format_type not in OPENAI_RESPONSE_FORMAT_TYPES or format_type == "text":
        return None
    if format_type == "json_object":
        return {"format": {"type": "json_object"}}
    json_schema = format_value.get("json_schema")
    if not is_record(json_schema):
        return None
    schema = json_schema.get("schema")
    if not is_record(schema):
        return None
    normalized_schema = prepare_json_schema(schema)
    text_format = {
        "type": "json_schema",
        "name": str(json_schema.get("name") or "structured_output"),
        "schema": normalized_schema,
    }
    if "strict" in json_schema:
        text_format["strict"] = bool(json_schema.get("strict"))
    return {"format": text_format}


def normalize_payload(raw_payload):
    payload = strip_unsupported_top_level_fields(raw_payload)
    model_spec = resolve_model_spec(payload.get("model") or DEFAULT_MODEL)
    payload["model"] = model_spec["effective_model"]
    payload["input"] = normalize_input(payload.get("input", ""))
    payload["store"] = False
    instructions = payload.get("instructions")
    if not isinstance(instructions, str) or not instructions.strip():
        payload["instructions"] = DEFAULT_INSTRUCTIONS
    reasoning = payload.get("reasoning")
    if isinstance(reasoning, dict):
        reasoning = dict(reasoning)
        if not reasoning.get("effort"):
            reasoning["effort"] = DEFAULT_REASONING_EFFORT
        payload["reasoning"] = reasoning
    elif reasoning is None:
        payload["reasoning"] = {"effort": DEFAULT_REASONING_EFFORT}

    text = payload.get("text")
    if isinstance(text, dict):
        text = dict(text)
        if not text.get("verbosity"):
            text["verbosity"] = DEFAULT_TEXT_VERBOSITY
        payload["text"] = text
    elif text is None:
        payload["text"] = {"verbosity": DEFAULT_TEXT_VERBOSITY}

    response_format = normalize_response_format(raw_payload.get("response_format"))
    if response_format:
        text = dict(payload.get("text") or {})
        text.update(response_format)
        payload["text"] = text
    return payload


def chat_content_to_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        chunks = []
        for item in content:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text" and isinstance(item.get("text"), str):
                chunks.append(item["text"])
        return "".join(chunks)
    if content is None:
        return ""
    return str(content)


def normalize_chat_user_content(content):
    if isinstance(content, str):
        return [{"type": "input_text", "text": content}]
    if isinstance(content, list):
        parts = []
        for item in content:
            if not isinstance(item, dict):
                continue
            item_type = item.get("type")
            if item_type == "text" and isinstance(item.get("text"), str):
                parts.append({"type": "input_text", "text": item["text"]})
                continue
            if item_type == "image_url":
                image_url = item.get("image_url")
                if isinstance(image_url, dict):
                    image_url = image_url.get("url")
                if isinstance(image_url, str) and image_url.strip():
                    parts.append({"type": "input_image", "image_url": image_url.strip(), "detail": "auto"})
        if parts:
            return parts
    return [{"type": "input_text", "text": chat_content_to_text(content)}]


def normalize_chat_tools(tools):
    if not isinstance(tools, list):
        return None
    normalized = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        if tool.get("type") != "function":
            continue
        function = tool.get("function")
        if not isinstance(function, dict):
            continue
        name = str(function.get("name") or "").strip()
        if not name:
            continue
        normalized.append(
            {
                "type": "function",
                "name": name,
                "description": function.get("description"),
                "parameters": function.get("parameters") or {"type": "object", "properties": {}},
            }
        )
    return normalized or None


def chat_tool_choice_to_responses(tool_choice):
    if isinstance(tool_choice, str):
        return tool_choice
    if not isinstance(tool_choice, dict):
        return None
    if tool_choice.get("type") != "function":
        return None
    function = tool_choice.get("function")
    if not isinstance(function, dict):
        return None
    name = str(function.get("name") or "").strip()
    if not name:
        return None
    return {"type": "function", "name": name}


def normalize_chat_functions(functions):
    if not isinstance(functions, list):
        return None
    synthetic_tools = []
    for function in functions:
        if not isinstance(function, dict):
            continue
        synthetic_tools.append({"type": "function", "function": function})
    return normalize_chat_tools(synthetic_tools)


def legacy_function_call_to_tool_choice(function_call):
    if function_call is None:
        return None
    if isinstance(function_call, str):
        return function_call
    if not isinstance(function_call, dict):
        return None
    name = str(function_call.get("name") or "").strip()
    if not name:
        return None
    return {"type": "function", "name": name}


def build_responses_payload_from_chat(raw_payload):
    payload = strip_unsupported_top_level_fields(raw_payload)
    payload.pop("messages", None)
    messages = raw_payload.get("messages")
    if not isinstance(messages, list):
        raise ValueError("messages must be a list")

    instructions = []
    input_items = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip()
        if role in {"system", "developer"}:
            text = chat_content_to_text(msg.get("content"))
            if text:
                instructions.append(text)
            continue
        if role == "user":
            input_items.append(
                {
                    "type": "message",
                    "role": "user",
                    "content": normalize_chat_user_content(msg.get("content")),
                }
            )
            continue
        if role == "assistant":
            output_parts = []
            text = chat_content_to_text(msg.get("content"))
            if text:
                output_parts.append({"type": "output_text", "text": text, "annotations": []})
            if output_parts:
                input_items.append(
                    {
                        "type": "message",
                        "role": "assistant",
                        "status": "completed",
                        "content": output_parts,
                    }
                )
            for tool_call in msg.get("tool_calls") or []:
                if not isinstance(tool_call, dict):
                    continue
                if tool_call.get("type") != "function":
                    continue
                function = tool_call.get("function") or {}
                name = str(function.get("name") or "").strip()
                if not name:
                    continue
                arguments = function.get("arguments", "{}")
                if not isinstance(arguments, str):
                    arguments = json.dumps(arguments, ensure_ascii=False)
                call_id = str(tool_call.get("id") or f"call_{len(input_items)}")
                input_items.append(
                    {
                        "type": "function_call",
                        "call_id": call_id,
                        "name": name,
                        "arguments": arguments,
                    }
                )
            continue
        if role == "tool":
            call_id = str(msg.get("tool_call_id") or "").strip()
            if not call_id:
                continue
            input_items.append(
                {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": chat_content_to_text(msg.get("content")),
                }
            )

    if not input_items:
        input_items = normalize_input("")

    payload["model"] = str(payload.get("model") or DEFAULT_MODEL)
    payload["input"] = input_items
    if instructions:
        payload["instructions"] = "\n\n".join(instructions)
    reasoning_effort = raw_payload.get("reasoning_effort")
    if isinstance(reasoning_effort, str):
        effort = reasoning_effort.strip().lower()
        if effort in REASONING_EFFORT_VALUES:
            payload["reasoning"] = {"effort": effort}
    tools = None
    if "tools" in raw_payload:
        tools = normalize_chat_tools(raw_payload.get("tools"))
    elif "functions" in raw_payload:
        tools = normalize_chat_functions(raw_payload.get("functions"))
    if tools is not None:
        payload["tools"] = tools
    tool_choice = chat_tool_choice_to_responses(raw_payload.get("tool_choice"))
    if tool_choice is None and "function_call" in raw_payload:
        tool_choice = legacy_function_call_to_tool_choice(raw_payload.get("function_call"))
    if tool_choice is not None:
        payload["tool_choice"] = tool_choice
    response_format = normalize_response_format(raw_payload.get("response_format"))
    if response_format:
        payload["text"] = response_format
    return normalize_payload(payload)


def anthropic_error_payload(error_type: str, message: str):
    return {"type": "error", "error": {"type": error_type, "message": message}}


def anthropic_status_error_type(status: int):
    return ANTHROPIC_ERROR_TYPES.get(status, "api_error")


def normalize_anthropic_model(model):
    model_name = str(model or "").strip()
    if not model_name:
        raise ProxyError(400, "invalid_request_error", "model is required")
    if model_name in ANTHROPIC_MODEL_ALIASES:
        mapped = ANTHROPIC_MODEL_ALIASES[model_name]
    else:
        mapped = model_name
    mapped_spec = resolve_model_spec(mapped)
    if mapped_spec["effective_model"] not in {"gpt-5.4", "gpt-5.3-codex"}:
        raise ProxyError(400, "invalid_request_error", f"unsupported model: {model_name}")
    if model_name not in ANTHROPIC_SUPPORTED_MODELS and model_name not in MODEL_OVERRIDES:
        raise ProxyError(400, "invalid_request_error", f"unsupported model: {model_name}")
    return model_name, mapped


def anthropic_budget_model(model_name):
    name = str(model_name or "").strip()
    if not name:
        return DEFAULT_MODEL
    if name in ANTHROPIC_MODEL_ALIASES:
        return ANTHROPIC_MODEL_ALIASES[name]
    return name


def require_anthropic_max_tokens(raw_payload):
    if not isinstance(raw_payload, dict) or "max_tokens" not in raw_payload:
        raise ProxyError(400, "invalid_request_error", "max_tokens is required")
    try:
        max_tokens = int(raw_payload.get("max_tokens"))
    except (TypeError, ValueError):
        raise ProxyError(400, "invalid_request_error", "max_tokens must be an integer") from None
    if max_tokens <= 0:
        raise ProxyError(400, "invalid_request_error", "max_tokens must be greater than 0")
    return max_tokens


def anthropic_text_from_block(block, context):
    if not isinstance(block, dict):
        raise ProxyError(400, "invalid_request_error", f"{context} block must be an object")
    if block.get("type") != "text":
        raise ProxyError(400, "invalid_request_error", f"unsupported {context} block type: {block.get('type')}")
    text = block.get("text")
    if not isinstance(text, str):
        raise ProxyError(400, "invalid_request_error", f"{context} text block must include text")
    return text


def normalize_anthropic_system(system):
    if system is None:
        return ""
    if isinstance(system, str):
        return system
    if not isinstance(system, list):
        raise ProxyError(400, "invalid_request_error", "system must be a string or list of text blocks")
    parts = []
    for block in system:
        parts.append(anthropic_text_from_block(block, "system"))
    return "\n\n".join(part for part in parts if part)


def normalize_anthropic_message_content(content, role):
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    if not isinstance(content, list):
        raise ProxyError(400, "invalid_request_error", f"{role} content must be a string or list")
    normalized = []
    for block in content:
        if not isinstance(block, dict):
            raise ProxyError(400, "invalid_request_error", f"{role} content block must be an object")
        block_type = str(block.get("type") or "").strip()
        if block_type in {"thinking", "redacted_thinking"}:
            if role != "assistant":
                raise ProxyError(400, "invalid_request_error", f"{block_type} blocks are only supported in assistant messages")
            normalized.append(block)
            continue
        if block_type not in {"text", "tool_use", "tool_result"}:
            raise ProxyError(400, "invalid_request_error", f"unsupported {role} content block type: {block_type or 'unknown'}")
        normalized.append(block)
    return normalized


def anthropic_tool_result_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            parts.append(anthropic_text_from_block(block, "tool_result"))
        return "".join(parts)
    raise ProxyError(400, "invalid_request_error", "tool_result.content must be a string or list of text blocks")


def anthropic_message_to_input_items(msg):
    if not isinstance(msg, dict):
        raise ProxyError(400, "invalid_request_error", "messages entries must be objects")
    role = str(msg.get("role") or "").strip()
    if role not in {"user", "assistant"}:
        raise ProxyError(400, "invalid_request_error", f"unsupported role: {role or 'unknown'}")
    content_blocks = normalize_anthropic_message_content(msg.get("content"), role)
    input_items = []
    buffered_text = []

    def flush_text():
        nonlocal buffered_text
        if not buffered_text:
            return
        if role == "user":
            input_items.append(
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": text} for text in buffered_text],
                }
            )
        else:
            input_items.append(
                {
                    "type": "message",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{"type": "output_text", "text": text, "annotations": []} for text in buffered_text],
                }
            )
        buffered_text = []

    for block in content_blocks:
        block_type = block.get("type")
        if block_type == "text":
            buffered_text.append(anthropic_text_from_block(block, role))
            continue
        if block_type in {"thinking", "redacted_thinking"}:
            # Claude clients may replay assistant reasoning blocks in history.
            # Responses input does not accept prior reasoning items, so we safely skip them.
            continue

        flush_text()
        if block_type == "tool_use":
            if role != "assistant":
                raise ProxyError(400, "invalid_request_error", "tool_use blocks are only supported in assistant messages")
            call_id = str(block.get("id") or "").strip()
            name = str(block.get("name") or "").strip()
            if not call_id or not name:
                raise ProxyError(400, "invalid_request_error", "tool_use blocks must include id and name")
            arguments = block.get("input")
            if arguments is None:
                arguments = {}
            if not isinstance(arguments, dict):
                raise ProxyError(400, "invalid_request_error", "tool_use input must be an object")
            input_items.append(
                {
                    "type": "function_call",
                    "call_id": call_id,
                    "name": name,
                    "arguments": json.dumps(arguments, ensure_ascii=False),
                }
            )
            continue

        if role != "user":
            raise ProxyError(400, "invalid_request_error", "tool_result blocks are only supported in user messages")
        call_id = str(block.get("tool_use_id") or "").strip()
        if not call_id:
            raise ProxyError(400, "invalid_request_error", "tool_result blocks must include tool_use_id")
        input_items.append(
            {
                "type": "function_call_output",
                "call_id": call_id,
                "output": anthropic_tool_result_text(block.get("content")),
            }
        )

    flush_text()
    return input_items


def normalize_anthropic_tools(tools):
    if tools is None:
        return None
    if not isinstance(tools, list):
        raise ProxyError(400, "invalid_request_error", "tools must be a list")
    normalized = []
    for tool in tools:
        if not isinstance(tool, dict):
            raise ProxyError(400, "invalid_request_error", "tools entries must be objects")
        name = str(tool.get("name") or "").strip()
        if not name:
            raise ProxyError(400, "invalid_request_error", "tools entries must include name")
        input_schema = tool.get("input_schema")
        if input_schema is None:
            input_schema = {"type": "object", "properties": {}}
        if not isinstance(input_schema, dict):
            raise ProxyError(400, "invalid_request_error", "tool input_schema must be an object")
        normalized.append(
            {
                "type": "function",
                "name": name,
                "description": tool.get("description"),
                "parameters": input_schema,
            }
        )
    return normalized or None


def anthropic_tool_choice_to_responses(tool_choice):
    if tool_choice is None:
        return None
    if isinstance(tool_choice, str):
        choice_type = tool_choice.strip()
        if choice_type in {"auto", "none"}:
            return choice_type
        if choice_type == "any":
            return "required"
        raise ProxyError(400, "invalid_request_error", f"unsupported tool_choice: {tool_choice}")
    if not isinstance(tool_choice, dict):
        raise ProxyError(400, "invalid_request_error", "tool_choice must be a string or object")
    choice_type = str(tool_choice.get("type") or "").strip()
    if choice_type in {"auto", "none"}:
        return choice_type
    if choice_type == "any":
        return "required"
    if choice_type != "tool":
        raise ProxyError(400, "invalid_request_error", f"unsupported tool_choice: {choice_type or 'unknown'}")
    name = str(tool_choice.get("name") or "").strip()
    if not name:
        raise ProxyError(400, "invalid_request_error", "tool_choice tool entries must include name")
    return {"type": "function", "name": name}


def anthropic_thinking_to_reasoning(thinking):
    if not isinstance(thinking, dict):
        return None
    budget = thinking.get("budget_tokens")
    try:
        budget = int(budget)
    except (TypeError, ValueError):
        budget = 0
    if budget <= 0:
        return None
    if budget < 1024:
        effort = "low"
    elif budget < 4096:
        effort = "medium"
    elif budget < 8192:
        effort = "high"
    else:
        effort = "xhigh"
    return {"effort": effort}


def build_responses_payload_from_anthropic(raw_payload):
    if not isinstance(raw_payload, dict):
        raise ProxyError(400, "invalid_request_error", "request body must be an object")

    requested_model, mapped_model = normalize_anthropic_model(raw_payload.get("model"))
    require_anthropic_max_tokens(raw_payload)
    messages = raw_payload.get("messages")
    if not isinstance(messages, list):
        raise ProxyError(400, "invalid_request_error", "messages must be a list")

    input_items = []
    for msg in messages:
        input_items.extend(anthropic_message_to_input_items(msg))
    if not input_items:
        input_items = normalize_input("")

    payload = strip_unsupported_top_level_fields(raw_payload)
    payload.pop("messages", None)
    payload.pop("system", None)
    payload.pop("tools", None)
    payload.pop("tool_choice", None)
    payload["model"] = mapped_model
    payload["input"] = input_items

    instructions = normalize_anthropic_system(raw_payload.get("system"))
    if instructions:
        payload["instructions"] = instructions

    tools = normalize_anthropic_tools(raw_payload.get("tools"))
    if tools is not None:
        payload["tools"] = tools

    tool_choice = anthropic_tool_choice_to_responses(raw_payload.get("tool_choice"))
    if tool_choice is not None:
        payload["tool_choice"] = tool_choice
    reasoning = anthropic_thinking_to_reasoning(raw_payload.get("thinking"))
    if reasoning is not None:
        payload["reasoning"] = reasoning

    return requested_model, normalize_payload(payload)


def extract_header_value(headers, *names):
    if headers is None:
        return ""
    for name in names:
        value = headers.get(name, "")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def request_source_ip(headers, client_ip):
    forwarded_for = extract_header_value(headers, "x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    real_ip = extract_header_value(headers, "x-real-ip")
    if real_ip:
        return real_ip
    return str(client_ip or "").strip()


def extract_explicit_session_key(headers, raw_payload):
    session_key = extract_header_value(headers, "session_id", "conversation_id")
    if session_key:
        return session_key
    if not isinstance(raw_payload, dict):
        return ""
    for key in ("session_id", "conversation_id", "prompt_cache_key"):
        value = raw_payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def extract_client_id(headers, raw_payload, client_ip):
    if isinstance(raw_payload, dict):
        value = raw_payload.get("client_id")
        if isinstance(value, str) and value.strip():
            return value.strip()
    value = extract_header_value(headers, "x-client-id", "client_id")
    if value:
        return value
    return request_source_ip(headers, client_ip)


def extract_business_key(headers, raw_payload):
    if isinstance(raw_payload, dict):
        value = raw_payload.get("business_key")
        if isinstance(value, str) and value.strip():
            return value.strip()
    value = extract_header_value(headers, "x-business-key", "business_key")
    if value:
        return value
    return DEFAULT_BUSINESS_KEY


def build_session_key(client_id, business_key):
    client_id = str(client_id or "").strip()
    business_key = str(business_key or "").strip() or DEFAULT_BUSINESS_KEY
    if not client_id:
        return ""
    return f"{client_id}:{business_key}"


def extract_session_context(headers, raw_payload, client_ip):
    explicit_session_key = extract_explicit_session_key(headers, raw_payload)
    client_id = extract_client_id(headers, raw_payload, client_ip)
    business_key = extract_business_key(headers, raw_payload)
    derived_session_key = build_session_key(client_id, business_key)
    return {
        "session_key": explicit_session_key or derived_session_key,
        "client_id": client_id,
        "business_key": business_key,
        "explicit_session_key": explicit_session_key,
        "derived_session_key": derived_session_key,
    }


def ensure_prompt_cache_key(payload, session_key):
    if not session_key:
        return
    if not isinstance(payload.get("prompt_cache_key"), str) or not payload.get("prompt_cache_key", "").strip():
        payload["prompt_cache_key"] = session_key


def build_upstream_headers(account, session_key):
    headers = build_default_desktop_headers()
    headers["Content-Type"] = "application/json"
    headers["OpenAI-Beta"] = "responses_websockets=2026-02-06"
    headers["Authorization"] = f"Bearer {account.access_token()}"
    cookie_header = account_cookie_header(account.name)
    if cookie_header:
        headers["Cookie"] = cookie_header
    if session_key:
        headers["conversation_id"] = session_key
        headers["session_id"] = session_key
    return order_headers(headers)


def estimate_text_tokens(text):
    if not text:
        return 0
    data = str(text).encode("utf-8", errors="ignore")
    return max(1, (len(data) + 3) // 4)


def estimate_input_tokens(value):
    if isinstance(value, str):
        return estimate_text_tokens(value)
    if isinstance(value, dict):
        total = 0
        for key, current in value.items():
            total += estimate_text_tokens(key)
            total += estimate_input_tokens(current)
        return total
    if isinstance(value, list):
        return sum(estimate_input_tokens(item) for item in value)
    if value is None:
        return 0
    return estimate_text_tokens(value)


def estimate_request_tokens(payload):
    total = 0
    total += estimate_text_tokens(payload.get("model", ""))
    total += estimate_text_tokens(payload.get("instructions", ""))
    total += estimate_input_tokens(payload.get("input"))
    # Keep a small fixed overhead for roles/types/message wrappers.
    return total + 32


def apply_session_response_context(payload, session_key):
    if not isinstance(payload, dict):
        return payload
    enabled = bool(payload.pop("_codex2gpt_enable_previous_response_id", False))
    if not enabled:
        return payload
    session_key = str(session_key or "").strip()
    if not session_key:
        return payload
    previous_response_id = payload.get("previous_response_id")
    if isinstance(previous_response_id, str) and previous_response_id.strip():
        return payload
    remembered = session_coordinator.previous_response_id(session_key)
    if remembered:
        payload["previous_response_id"] = remembered
    return payload


def validate_context_budget(payload, *, requested_model=None):
    estimated = estimate_request_tokens(payload)
    model_spec = resolve_model_spec(requested_model or payload.get("model"))
    context_window = int(model_spec["context_window"])
    compact_limit = min(int(model_spec["auto_compact_token_limit"]), context_window)
    if estimated > context_window:
        return estimated, model_spec, (
            f"estimated input tokens {estimated} exceed configured model context window "
            f"{context_window}"
        )
    if estimated > compact_limit:
        return estimated, model_spec, (
            f"estimated input tokens {estimated} exceed configured auto compact guard "
            f"{compact_limit}; this proxy does not compact context automatically"
        )
    return estimated, model_spec, None


def extract_final_response(sse_body: str):
    for line in sse_body.splitlines():
        if not line.startswith("data: "):
            continue
        data = line[6:]
        if not data or data == "[DONE]":
            continue
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            continue
        if payload.get("type") in {"response.completed", "response.done"} and isinstance(payload.get("response"), dict):
            return payload["response"]
    return None


def response_output_text(response):
    chunks = []
    for item in response.get("output") or []:
        if item.get("type") != "message" or item.get("role") != "assistant":
            continue
        for content in item.get("content") or []:
            content_type = content.get("type")
            if content_type == "output_text":
                chunks.append(content.get("text", ""))
            elif content_type == "refusal":
                chunks.append(content.get("refusal", ""))
    return "".join(chunks)


def response_output_tool_calls(response):
    tool_calls = []
    for index, item in enumerate(response.get("output") or []):
        if item.get("type") != "function_call":
            continue
        arguments = item.get("arguments", "{}")
        tool_calls.append(
            {
                "id": str(item.get("call_id") or f"call_{index}"),
                "type": "function",
                "function": {
                    "name": item.get("name", ""),
                    "arguments": arguments if isinstance(arguments, str) else json.dumps(arguments, ensure_ascii=False),
                },
            }
        )
    return tool_calls


def response_finish_reason(response):
    if response_output_tool_calls(response):
        return "tool_calls"
    status = response.get("status")
    if status == "incomplete":
        return "length"
    return "stop"


def response_usage_to_chat(response):
    usage = response.get("usage") or {}
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    prompt_details = {}
    input_details = usage.get("input_tokens_details") or {}
    cached_tokens = int(input_details.get("cached_tokens") or 0)
    if cached_tokens:
        prompt_details["cached_tokens"] = cached_tokens
    chat_usage = {
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
    }
    if prompt_details:
        chat_usage["prompt_tokens_details"] = prompt_details
    return chat_usage


def response_to_chat_completion(response):
    message = {
        "role": "assistant",
        "content": response_output_text(response) or None,
    }
    tool_calls = response_output_tool_calls(response)
    if tool_calls:
        message["tool_calls"] = tool_calls
    return {
        "id": response.get("id", f"chatcmpl-{int(time.time())}"),
        "object": "chat.completion",
        "created": int(response.get("created_at") or time.time()),
        "model": response.get("model", DEFAULT_MODEL),
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": response_finish_reason(response),
            }
        ],
        "usage": response_usage_to_chat(response),
    }


def chat_completion_chunk_from_response(response):
    delta = {"role": "assistant"}
    text = response_output_text(response)
    if text:
        delta["content"] = text
    tool_calls = response_output_tool_calls(response)
    if tool_calls:
        delta["tool_calls"] = tool_calls
    return {
        "id": response.get("id", f"chatcmpl-{int(time.time())}"),
        "object": "chat.completion.chunk",
        "created": int(response.get("created_at") or time.time()),
        "model": response.get("model", DEFAULT_MODEL),
        "choices": [
            {
                "index": 0,
                "delta": delta,
                "finish_reason": response_finish_reason(response),
            }
        ],
    }


def chat_completion_usage_chunk_from_response(response):
    return {
        "id": response.get("id", f"chatcmpl-{int(time.time())}"),
        "object": "chat.completion.chunk",
        "created": int(response.get("created_at") or time.time()),
        "model": response.get("model", DEFAULT_MODEL),
        "choices": [],
        "usage": response_usage_to_chat(response),
    }


def anthropic_usage_from_response(response, include_output_tokens=True):
    usage = response.get("usage") or {}
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    payload = {"input_tokens": input_tokens, "output_tokens": output_tokens if include_output_tokens else 0}
    cached_tokens = int((usage.get("input_tokens_details") or {}).get("cached_tokens") or 0)
    if cached_tokens:
        payload["cache_read_input_tokens"] = cached_tokens
    return payload


def response_to_anthropic_content_blocks(response):
    blocks = []
    for item in response.get("output") or []:
        item_type = item.get("type")
        if item_type == "reasoning":
            continue
        if item_type == "message" and item.get("role") == "assistant":
            for content in item.get("content") or []:
                content_type = content.get("type")
                if content_type == "output_text":
                    blocks.append({"type": "text", "text": content.get("text", "")})
                elif content_type == "refusal":
                    blocks.append({"type": "text", "text": content.get("refusal", "")})
            continue
        if item_type != "function_call":
            continue
        arguments = item.get("arguments", {})
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments or "{}")
            except json.JSONDecodeError as exc:
                raise ProxyError(502, "api_error", f"invalid function_call arguments from upstream: {exc}") from exc
        if not isinstance(arguments, dict):
            raise ProxyError(502, "api_error", "function_call arguments from upstream must be a JSON object")
        blocks.append(
            {
                "type": "tool_use",
                "id": str(item.get("call_id") or f"toolu_{len(blocks)}"),
                "name": str(item.get("name") or ""),
                "input": arguments,
            }
        )
    return blocks


def anthropic_stop_reason(response, content_blocks):
    if any(block.get("type") == "tool_use" for block in content_blocks):
        return "tool_use"
    if response.get("status") == "incomplete":
        return "max_tokens"
    return "end_turn"


def response_to_anthropic_message(response, requested_model):
    content_blocks = response_to_anthropic_content_blocks(response)
    return {
        "id": response.get("id", f"msg_{int(time.time())}"),
        "type": "message",
        "role": "assistant",
        "model": requested_model,
        "content": content_blocks,
        "stop_reason": anthropic_stop_reason(response, content_blocks),
        "stop_sequence": None,
        "usage": anthropic_usage_from_response(response),
    }


def anthropic_sse_event(name, payload):
    return f"event: {name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def anthropic_sse_body_from_message(message_payload):
    frames = [
        anthropic_sse_event(
            "message_start",
            {
                "type": "message_start",
                "message": {
                    "id": message_payload["id"],
                    "type": "message",
                    "role": "assistant",
                    "model": message_payload["model"],
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {
                        key: value
                        for key, value in message_payload["usage"].items()
                        if key in {"input_tokens", "cache_read_input_tokens"}
                    }
                    | {"output_tokens": 0},
                },
            },
        )
    ]
    for index, block in enumerate(message_payload["content"]):
        if block["type"] == "text":
            frames.append(
                anthropic_sse_event(
                    "content_block_start",
                    {"type": "content_block_start", "index": index, "content_block": {"type": "text", "text": ""}},
                )
            )
            frames.append(
                anthropic_sse_event(
                    "content_block_delta",
                    {"type": "content_block_delta", "index": index, "delta": {"type": "text_delta", "text": block["text"]}},
                )
            )
        else:
            frames.append(
                anthropic_sse_event(
                    "content_block_start",
                    {
                        "type": "content_block_start",
                        "index": index,
                        "content_block": {"type": "tool_use", "id": block["id"], "name": block["name"], "input": {}},
                    },
                )
            )
            frames.append(
                anthropic_sse_event(
                    "content_block_delta",
                    {
                        "type": "content_block_delta",
                        "index": index,
                        "delta": {
                            "type": "input_json_delta",
                            "partial_json": json.dumps(block["input"], ensure_ascii=False, separators=(",", ":")),
                        },
                    },
                )
            )
        frames.append(anthropic_sse_event("content_block_stop", {"type": "content_block_stop", "index": index}))
    frames.append(
        anthropic_sse_event(
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": message_payload["stop_reason"], "stop_sequence": message_payload["stop_sequence"]},
                "usage": {"output_tokens": message_payload["usage"].get("output_tokens", 0)},
            },
        )
    )
    frames.append(anthropic_sse_event("message_stop", {"type": "message_stop"}))
    return "".join(frames).encode("utf-8")


def iter_upstream_extracted_events(upstream):
    def chunk_iter():
        while True:
            chunk = upstream.read(4096)
            if not chunk:
                break
            yield chunk

    for event in iter_sse_messages(chunk_iter()):
        yield extract_event_details(event)


def openai_sse_frame(payload):
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")


def anthropic_sse_event_bytes(name, payload):
    return f"event: {name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")


def iter_chat_completion_sse(upstream, model, include_usage=False, include_reasoning=False):
    chunk_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
    created = int(time.time())
    usage = None
    saw_tool_calls = False
    tool_call_indexes = {}
    next_tool_call_index = 0
    yielded_tool_deltas = set()

    yield openai_sse_frame(
        {
            "id": chunk_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
        }
    )

    for detail in iter_upstream_extracted_events(upstream):
        if detail["error"]:
            yield openai_sse_frame(
                {
                    "id": chunk_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"content": f"[Error] {detail['error']['code']}: {detail['error']['message']}"},
                            "finish_reason": None,
                        }
                    ],
                }
            )
            break
        if detail["function_call_start"]:
            start = detail["function_call_start"]
            saw_tool_calls = True
            call_id = start["call_id"]
            tool_call_indexes[call_id] = next_tool_call_index
            next_tool_call_index += 1
            yield openai_sse_frame(
                {
                    "id": chunk_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "tool_calls": [
                                    {
                                        "index": tool_call_indexes[call_id],
                                        "id": call_id,
                                        "type": "function",
                                        "function": {"name": start["name"], "arguments": ""},
                                    }
                                ]
                            },
                            "finish_reason": None,
                        }
                    ],
                }
            )
            continue
        if detail["function_call_delta"]:
            delta = detail["function_call_delta"]
            call_id = delta["call_id"]
            yielded_tool_deltas.add(call_id)
            yield openai_sse_frame(
                {
                    "id": chunk_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "tool_calls": [
                                    {
                                        "index": tool_call_indexes.get(call_id, 0),
                                        "function": {"arguments": delta["delta"]},
                                    }
                                ]
                            },
                            "finish_reason": None,
                        }
                    ],
                }
            )
            continue
        if detail["function_call_done"]:
            done = detail["function_call_done"]
            call_id = done["call_id"]
            if call_id not in yielded_tool_deltas:
                yield openai_sse_frame(
                    {
                        "id": chunk_id,
                        "object": "chat.completion.chunk",
                        "created": created,
                        "model": model,
                        "choices": [
                            {
                                "index": 0,
                                "delta": {
                                    "tool_calls": [
                                        {
                                            "index": tool_call_indexes.get(call_id, 0),
                                            "function": {"arguments": done["arguments"]},
                                        }
                                    ]
                                },
                                "finish_reason": None,
                            }
                        ],
                    }
                )
            continue
        if detail["reasoning_delta"] and include_reasoning:
            yield openai_sse_frame(
                {
                    "id": chunk_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"reasoning_content": detail["reasoning_delta"]},
                            "finish_reason": None,
                        }
                    ],
                }
            )
            continue
        if detail["text_delta"]:
            yield openai_sse_frame(
                {
                    "id": chunk_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": model,
                    "choices": [{"index": 0, "delta": {"content": detail["text_delta"]}, "finish_reason": None}],
                }
            )
        if detail["event"] == "response.completed":
            usage = detail["usage"]

    finish_reason = "tool_calls" if saw_tool_calls else "stop"
    yield openai_sse_frame(
        {
            "id": chunk_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
        }
    )
    if include_usage and usage:
        yield openai_sse_frame(
            {
                "id": chunk_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [],
                "usage": {
                    "prompt_tokens": int(usage.get("input_tokens") or 0),
                    "completion_tokens": int(usage.get("output_tokens") or 0),
                    "total_tokens": int(usage.get("input_tokens") or 0) + int(usage.get("output_tokens") or 0),
                    **(
                        {
                            "prompt_tokens_details": {
                                "cached_tokens": int(usage.get("cached_tokens") or 0)
                            }
                        }
                        if usage.get("cached_tokens") is not None
                        else {}
                    ),
                },
            }
        )
    yield b"data: [DONE]\n\n"


def iter_anthropic_message_sse(upstream, model, include_thinking=False):
    message_id = f"msg_{uuid.uuid4().hex[:24]}"
    usage = {"input_tokens": 0, "output_tokens": 0}
    content_index = 0
    text_block_open = False
    thinking_block_open = False
    saw_tool = False
    emitted_call_deltas = set()

    yield anthropic_sse_event_bytes(
        "message_start",
        {
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [],
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {"input_tokens": 0, "output_tokens": 0},
            },
        },
    )

    def close_text():
        nonlocal text_block_open, content_index
        if text_block_open:
            text_block_open = False
            frame = anthropic_sse_event_bytes("content_block_stop", {"type": "content_block_stop", "index": content_index})
            content_index += 1
            return [frame]
        return []

    def close_thinking():
        nonlocal thinking_block_open, content_index
        if thinking_block_open:
            thinking_block_open = False
            frame = anthropic_sse_event_bytes("content_block_stop", {"type": "content_block_stop", "index": content_index})
            content_index += 1
            return [frame]
        return []

    for detail in iter_upstream_extracted_events(upstream):
        if detail["error"]:
            for frame in close_thinking() + close_text():
                yield frame
            yield anthropic_sse_event_bytes(
                "content_block_start",
                {"type": "content_block_start", "index": content_index, "content_block": {"type": "text", "text": ""}},
            )
            yield anthropic_sse_event_bytes(
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": content_index,
                    "delta": {"type": "text_delta", "text": f"[Error] {detail['error']['code']}: {detail['error']['message']}"},
                },
            )
            yield anthropic_sse_event_bytes("content_block_stop", {"type": "content_block_stop", "index": content_index})
            content_index += 1
            break
        if detail["reasoning_delta"] and include_thinking:
            for frame in close_text():
                yield frame
            if not thinking_block_open:
                yield anthropic_sse_event_bytes(
                    "content_block_start",
                    {"type": "content_block_start", "index": content_index, "content_block": {"type": "thinking", "thinking": ""}},
                )
                thinking_block_open = True
            yield anthropic_sse_event_bytes(
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": content_index,
                    "delta": {"type": "thinking_delta", "thinking": detail["reasoning_delta"]},
                },
            )
            continue
        if detail["function_call_start"]:
            saw_tool = True
            for frame in close_thinking() + close_text():
                yield frame
            start = detail["function_call_start"]
            yield anthropic_sse_event_bytes(
                "content_block_start",
                {
                    "type": "content_block_start",
                    "index": content_index,
                    "content_block": {"type": "tool_use", "id": start["call_id"], "name": start["name"], "input": {}},
                },
            )
            continue
        if detail["function_call_delta"]:
            delta = detail["function_call_delta"]
            emitted_call_deltas.add(delta["call_id"])
            yield anthropic_sse_event_bytes(
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": content_index,
                    "delta": {"type": "input_json_delta", "partial_json": delta["delta"]},
                },
            )
            continue
        if detail["function_call_done"]:
            done = detail["function_call_done"]
            if done["call_id"] not in emitted_call_deltas:
                yield anthropic_sse_event_bytes(
                    "content_block_delta",
                    {
                        "type": "content_block_delta",
                        "index": content_index,
                        "delta": {"type": "input_json_delta", "partial_json": done["arguments"]},
                    },
                )
            yield anthropic_sse_event_bytes("content_block_stop", {"type": "content_block_stop", "index": content_index})
            content_index += 1
            continue
        if detail["text_delta"]:
            for frame in close_thinking():
                yield frame
            if not text_block_open:
                yield anthropic_sse_event_bytes(
                    "content_block_start",
                    {"type": "content_block_start", "index": content_index, "content_block": {"type": "text", "text": ""}},
                )
                text_block_open = True
            yield anthropic_sse_event_bytes(
                "content_block_delta",
                {
                    "type": "content_block_delta",
                    "index": content_index,
                    "delta": {"type": "text_delta", "text": detail["text_delta"]},
                },
            )
        if detail["event"] == "response.completed" and detail["usage"]:
            usage = {
                "input_tokens": int(detail["usage"].get("input_tokens") or 0),
                "output_tokens": int(detail["usage"].get("output_tokens") or 0),
            }
            if detail["usage"].get("cached_tokens") is not None:
                usage["cache_read_input_tokens"] = int(detail["usage"].get("cached_tokens") or 0)

    for frame in close_thinking() + close_text():
        yield frame
    yield anthropic_sse_event_bytes(
        "message_delta",
        {
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use" if saw_tool else "end_turn", "stop_sequence": None},
            "usage": {"output_tokens": usage.get("output_tokens", 0)},
        },
    )
    yield anthropic_sse_event_bytes("message_stop", {"type": "message_stop"})


def parse_auth_header(headers):
    value = headers.get("authorization", "")
    if value.lower().startswith("bearer "):
        return value[7:].strip()
    return headers.get("x-api-key", "").strip()


def is_retryable_error(error):
    if isinstance(error, urllib.error.HTTPError):
        return error.code in RETRYABLE_STATUS_CODES
    return isinstance(error, urllib.error.URLError)


def read_http_error_body(error):
    cached = getattr(error, "_cached_body", None)
    if cached is not None:
        return cached
    body = error.read()
    error._cached_body = body
    error.fp = io.BytesIO(body)
    return body


def is_account_unusable_error(error):
    if not isinstance(error, urllib.error.HTTPError) or error.code != 402:
        return False
    body = read_http_error_body(error)
    try:
        payload = json.loads(body.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return False
    detail = payload.get("detail")
    return isinstance(detail, dict) and str(detail.get("code", "")).strip() == "deactivated_workspace"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _request_path(self):
        return urllib.parse.urlsplit(self.path).path

    def _reset_request_tracking(self):
        self._transcript_request_id = transcript_store.new_request_id()
        self._last_account_name = ""
        self._last_attempted_account_name = ""

    def _current_account_name(self):
        return getattr(self, "_last_account_name", "") or getattr(self, "_last_attempted_account_name", "")

    def _transcript_record_base(self, path, session_context, requested_model, payload, raw_payload, status):
        session_context = session_context or {}
        return {
            "timestamp": now_iso(),
            "request_id": getattr(self, "_transcript_request_id", transcript_store.new_request_id()),
            "path": path,
            "session_key": session_context.get("session_key", ""),
            "client_id": session_context.get("client_id", ""),
            "business_key": session_context.get("business_key", ""),
            "account_name": self._current_account_name(),
            "status": status,
            "request": transcript_request_section(requested_model, payload, raw_payload),
            "response": None,
            "error": None,
        }

    def _append_transcript(self, record):
        transcript_store.append(record["request_id"], record.get("session_key", ""), record)

    def _record_completed_transcript(self, path, session_context, requested_model, payload, raw_payload, response_section):
        record = self._transcript_record_base(path, session_context, requested_model, payload, raw_payload, "completed")
        record["response"] = response_section
        self._append_transcript(record)
        RECENT_REQUESTS.append(
            recent_request_entry(
                path,
                requested_model,
                self._current_account_name(),
                "completed",
                usage=response_section.get("usage") if isinstance(response_section, dict) else {},
            )
        )

    def _record_failed_transcript(self, path, session_context, requested_model, payload, raw_payload, status, error_type, message, status_code=None, body=None):
        record = self._transcript_record_base(path, session_context, requested_model, payload, raw_payload, status)
        record["error"] = {
            "type": error_type,
            "message": message,
            "status_code": status_code,
            "body": transcript_error_body(body),
        }
        self._append_transcript(record)
        RECENT_REQUESTS.append(
            recent_request_entry(
                path,
                requested_model,
                self._current_account_name(),
                status,
                error={"type": error_type, "message": message, "status_code": status_code},
            )
        )

    def _http_error_details(self, exc):
        body = read_http_error_body(exc).decode("utf-8", errors="replace")
        message = body or str(exc)
        parsed = None
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            error = parsed.get("error")
            if isinstance(error, dict) and isinstance(error.get("message"), str):
                message = error["message"]
            elif isinstance(parsed.get("detail"), dict) and isinstance(parsed["detail"].get("message"), str):
                message = parsed["detail"]["message"]
        return {"status_code": exc.code, "message": message, "body": parsed if parsed is not None else body}

    def _response_metric_headers(self, usage=None):
        usage = usage if isinstance(usage, dict) else {}
        headers = {}
        account_name = self._current_account_name()
        if account_name:
            headers["X-Codex2gpt-Account"] = account_name
        prompt_tokens = usage_prompt_tokens(usage)
        cached_tokens = usage_cached_tokens(usage)
        cache_hit_rate = usage_cache_hit_rate(usage)
        if prompt_tokens is not None:
            headers["X-Codex2gpt-Prompt-Tokens"] = str(prompt_tokens)
        if cached_tokens is not None:
            headers["X-Codex2gpt-Cached-Tokens"] = str(cached_tokens)
        if cache_hit_rate is not None:
            headers["X-Codex2gpt-Cache-Hit-Rate"] = f"{cache_hit_rate:.2f}"
        return headers

    def _write_json(self, status, payload, extra_headers=None):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        for key, value in (extra_headers or {}).items():
            self.send_header(str(key), str(value))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_text(self, status, body, content_type="text/plain; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_html(self, status, html):
        self._write_text(status, html, "text/html; charset=utf-8")

    def _send_sse_headers(self, status=200, extra_headers=None):
        self.send_response(status)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        for key, value in (extra_headers or {}).items():
            self.send_header(str(key), str(value))
        self.end_headers()

    def _write_stream(self, iterator, *, status=200, content_type="text/event-stream; charset=utf-8"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        for chunk in iterator:
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8")
            self.wfile.write(chunk)
            self.wfile.flush()

    def _client_ip(self):
        return request_source_ip(self.headers, self.client_address[0] if self.client_address else "")

    def _parse_cookies(self):
        raw = self.headers.get("cookie", "")
        cookies = {}
        for item in raw.split(";"):
            if "=" not in item:
                continue
            key, value = item.split("=", 1)
            cookies[key.strip()] = value.strip()
        return cookies

    def _dashboard_authenticated(self):
        if is_local_request(self._client_ip()):
            return True
        secret = dashboard_secret()
        if not secret:
            return True
        session_id = self._parse_cookies().get(DASHBOARD_SESSION_COOKIE, "")
        if not session_id:
            return False
        STATE_DB.cleanup_expired_dashboard_sessions()
        return STATE_DB.validate_dashboard_session(session_id)

    def _set_dashboard_session(self):
        session_id = secrets.token_urlsafe(24)
        expires_at = (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=max(60, DASHBOARD_SESSION_TTL))
        ).isoformat(timespec="seconds")
        STATE_DB.create_dashboard_session(session_id, expires_at=expires_at, remote_addr=self._client_ip())
        self.send_header(
            "Set-Cookie",
            f"{DASHBOARD_SESSION_COOKIE}={session_id}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max(60, DASHBOARD_SESSION_TTL)}",
        )

    def _clear_dashboard_session(self):
        session_id = self._parse_cookies().get(DASHBOARD_SESSION_COOKIE, "")
        if session_id:
            STATE_DB.delete_dashboard_session(session_id)
        self.send_header(f"Set-Cookie", f"{DASHBOARD_SESSION_COOKIE}=; Path=/; HttpOnly; Max-Age=0; SameSite=Lax")

    def _require_dashboard_access(self):
        if self._dashboard_authenticated():
            return True
        self._write_json(401, {"error": {"type": "authentication_error", "message": "dashboard login required"}})
        return False

    def _require_api_key(self):
        if not API_KEY:
            return True
        if parse_auth_header(self.headers) == API_KEY:
            return True
        self._write_json(401, {"error": {"type": "authentication_error", "message": "invalid api key"}})
        return False

    def _require_api_key_anthropic(self):
        if not API_KEY:
            return True
        if parse_auth_header(self.headers) == API_KEY:
            return True
        self._write_json(401, anthropic_error_payload("authentication_error", "invalid api key"))
        return False

    def _require_anthropic_version(self):
        version = extract_header_value(self.headers, ANTHROPIC_VERSION_HEADER)
        if version:
            return version
        raise ProxyError(400, "invalid_request_error", "missing required anthropic-version header")

    def _should_use_buffered_anthropic_sse(self):
        auth_header = str(self.headers.get("authorization") or "")
        user_agent = str(self.headers.get("user-agent") or "").lower()
        if "claude-code" in user_agent:
            return True
        return auth_header.lower().startswith("bearer ")

    def _write_anthropic_error(self, status, error_type, message):
        self._write_json(status, anthropic_error_payload(error_type, message))

    def _read_json(self):
        size = int(self.headers.get("content-length", "0") or "0")
        raw = self.rfile.read(size)
        return json.loads(raw.decode() or "{}")

    def do_GET(self):
        path = self._request_path()
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        if path == "/health":
            sync_accounts_with_state()
            session_stats = session_coordinator.snapshot()
            self._write_json(
                200,
                {
                    "status": "ok",
                    "accounts": pool.names(),
                    "sticky_sessions": pool.sticky_size(),
                    "preferred_account": pool.preferred_account(),
                    "tracked_sessions": session_stats["tracked_sessions"],
                    "active_sessions": session_stats["active_sessions"],
                    "queued_requests": session_stats["queued_requests"],
                    "model_context_window": DEFAULT_MODEL_CONTEXT_WINDOW,
                    "model_auto_compact_token_limit": DEFAULT_MODEL_AUTO_COMPACT_TOKEN_LIMIT,
                    "model_overrides": configured_model_overrides_snapshot(),
                    "transcripts_enabled": transcript_store.enabled,
                    "transcript_dir": transcript_store.base_dir,
                    "rotation_mode": current_rotation_mode(),
                    "responses_transport": current_responses_transport_mode(),
                    "transport_backend": FINGERPRINT_CACHE.get("transport_backend"),
                    "websocket_transport_available": websocket_transport_available(),
                    "account_statuses": runtime_account_status_summary(),
                    "warnings": runtime_warning_summary(),
                    "state_db_path": STATE_DB_PATH,
                },
            )
            return
        if path == "/":
            if not self._dashboard_authenticated():
                self._write_html(
                    200,
                    """<!doctype html><html><body><form method="post" action="/auth/login">
                    <h1>Codex2gpt Dashboard Login</h1>
                    <input type="password" name="password" placeholder="Password" />
                    <button type="submit">Login</button>
                    </form></body></html>""",
                )
                return
            index_path = os.path.join(WEB_DIR, "index.html")
            if os.path.isfile(index_path):
                with open(index_path, "rb") as f:
                    return self._write_text(200, f.read(), "text/html; charset=utf-8")
            self._write_html(
                200,
                """<!doctype html><html><body><h1>Codex2gpt Dashboard</h1>
                <p>Dashboard assets not built yet. Use the JSON admin endpoints directly.</p></body></html>""",
            )
            return
        if path == "/auth/status":
            sync_accounts_with_state()
            secret = dashboard_secret()
            codex_app = current_codex_app_state()
            self._write_json(
                200,
                {
                    "authenticated": self._dashboard_authenticated(),
                    "password_required": bool(secret) and not is_local_request(self._client_ip()),
                    "local_dashboard_bypass": is_local_request(self._client_ip()),
                    "accounts": len(STATE_DB.list_accounts()),
                    "proxies": len(STATE_DB.list_proxies()),
                    "relay_providers": len(STATE_DB.list_relay_providers(enabled_only=True)),
                    "rotation_mode": current_rotation_mode(),
                    "responses_transport": current_responses_transport_mode(),
                    "transport_backend": FINGERPRINT_CACHE.get("transport_backend"),
                    "websocket_transport_available": websocket_transport_available(),
                    "model_overrides": configured_model_overrides_snapshot(),
                    "account_statuses": runtime_account_status_summary(),
                    "warnings": runtime_warning_summary(),
                    "fingerprint": FINGERPRINT_CACHE,
                    "codex_app": codex_app,
                },
            )
            return
        if path == "/auth/accounts":
            if not self._require_dashboard_access():
                return
            sync_accounts_with_state()
            quota_mode = (query.get("quota") or [""])[0]
            quota_refresh = refresh_all_account_quotas() if quota_mode == "fresh" else None
            codex_app = current_codex_app_state()
            reserved_entry_id = codex_app.get("current_entry_id") if codex_app.get("matched") else ""
            accounts = []
            for account in STATE_DB.list_accounts():
                account = dict(account)
                account["proxy_mode"] = get_account_proxy_mode(account["entry_id"])
                account["proxy_assignment"] = STATE_DB.get_proxy_assignment(account["entry_id"])
                account["is_codex_app_current"] = account["entry_id"] == reserved_entry_id
                account["is_codex_app_reserved"] = account["entry_id"] == reserved_entry_id
                accounts.append(account)
            self._write_json(
                200,
                {
                    "data": accounts,
                    "codex_app": codex_app,
                    "warnings": STATE_DB.list_quota_warnings(),
                    "proxy_assignments": STATE_DB.list_proxy_assignments(),
                    "quota_refresh": quota_refresh,
                },
            )
            return
        if path == "/auth/quota/warnings":
            if not self._require_dashboard_access():
                return
            level = query.get("level", [None])[0]
            self._write_json(200, {"data": STATE_DB.list_quota_warnings(level=level)})
            return
        if path == "/v1/models":
            if not self._require_api_key():
                return
            self._write_json(200, {"object": "list", "data": advertised_model_catalog()})
            return
        if path == "/v1/models/catalog":
            if not self._require_api_key():
                return
            self._write_json(
                200,
                {
                    "data": advertised_model_catalog(),
                    "default_model": DEFAULT_MODEL,
                    "model_overrides": configured_model_overrides_snapshot(),
                    "rotation_mode": current_rotation_mode(),
                },
            )
            return
        if path == "/v1beta/models":
            if not self._require_api_key():
                return
            self._write_json(
                200,
                {
                    "models": [
                        {
                            "name": f"models/{model}",
                            "displayName": model,
                            "description": "Codex2gpt Gemini-compatible model shim",
                            "supportedGenerationMethods": sorted(GEMINI_ACTIONS),
                        }
                        for model, _ in advertised_model_entries()
                    ]
                },
            )
            return
        if path == "/api/proxies":
            if not self._require_dashboard_access():
                return
            self._write_json(
                200,
                {
                    "data": STATE_DB.list_proxies(),
                    "assignments": STATE_DB.list_proxy_assignments(),
                    "defaults": RUNTIME_SETTINGS.get("proxy_defaults") or {},
                },
            )
            return
        if path == "/api/relay-providers":
            if not self._require_dashboard_access():
                return
            self._write_json(200, {"data": STATE_DB.list_relay_providers()})
            return
        if path == "/admin/settings":
            if not self._require_dashboard_access():
                return
            self._write_json(200, RUNTIME_SETTINGS)
            return
        if path == "/admin/runtime-status":
            if not self._require_dashboard_access():
                return
            self._write_json(
                200,
                {
                    "transport_backend": current_transport_backend(),
                    "responses_transport": current_responses_transport_mode(),
                    "websocket_transport_available": websocket_transport_available(),
                    "curl_binary": find_curl_impersonate_binary(),
                    "fingerprint": FINGERPRINT_CACHE,
                    "background": BACKGROUND_JOBS.snapshot(),
                    "account_statuses": runtime_account_status_summary(),
                    "warnings": runtime_warning_summary(),
                    "token_refresh_backoff": TOKEN_REFRESH_BACKOFF,
                },
            )
            return
        if path == "/admin/recent-requests":
            if not self._require_dashboard_access():
                return
            limit = query.get("limit", ["20"])[0]
            try:
                limit_value = max(1, min(100, int(limit)))
            except ValueError:
                limit_value = 20
            self._write_json(200, {"data": RECENT_REQUESTS.list(limit_value)})
            return
        if path == "/admin/rotation-settings":
            if not self._require_dashboard_access():
                return
            self._write_json(
                200,
                {
                    "rotation_mode": current_rotation_mode(),
                    "plans": RUNTIME_SETTINGS.get("plans") or {},
                },
            )
            return
        if path == "/admin/quota-settings":
            if not self._require_dashboard_access():
                return
            self._write_json(200, RUNTIME_SETTINGS.get("quota") or {})
            return
        if path == "/admin/usage-stats/summary":
            if not self._require_dashboard_access():
                return
            self._write_json(200, STATE_DB.get_usage_summary())
            return
        if path == "/admin/usage-stats/history":
            if not self._require_dashboard_access():
                return
            hours = query.get("hours", ["24"])[0]
            granularity = query.get("granularity", ["hourly"])[0]
            try:
                hours_value = int(hours) if hours not in {"", "none", "None"} else None
            except ValueError:
                hours_value = 24
            self._write_json(200, {"data": STATE_DB.get_usage_history(hours=hours_value, granularity=granularity)})
            return
        if path.startswith("/auth/accounts/") and path.endswith("/cookies"):
            if not self._require_dashboard_access():
                return
            account_id = urllib.parse.unquote(path[len("/auth/accounts/") : -len("/cookies")]).strip("/")
            cookies = COOKIE_STORE.get(account_id, {})
            self._write_json(200, {"account_id": account_id, "cookies": cookies})
            return
        if path.startswith("/auth/accounts/") and path.endswith("/quota"):
            if not self._require_dashboard_access():
                return
            entry_id = urllib.parse.unquote(path[len("/auth/accounts/") : -len("/quota")]).strip("/")
            account = next((item for item in pool.accounts if item.name == entry_id), None)
            if account is None:
                self._write_json(404, {"error": {"type": "not_found", "message": "account not found"}})
                return
            try:
                quota = refresh_account_quota(account)
                self._write_json(200, {"entry_id": entry_id, "quota": quota})
            except urllib.error.HTTPError as exc:
                self._write_json(exc.code, {"error": {"type": "upstream_error", "message": str(exc)}})
            return
        if path == "/auth/callback":
            params = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            error = (params.get("error") or [""])[0]
            error_description = (params.get("error_description") or [""])[0]
            if error:
                self._write_html(400, oauth_callback_result_html(False, "Login Failed", error_description or error))
                return
            code = (params.get("code") or [""])[0]
            state = (params.get("state") or [""])[0]
            try:
                result = complete_oauth_callback(code, state)
                if result.get("already_completed"):
                    self._write_html(200, oauth_callback_result_html(True, "OAuth login complete", "This login session was already completed."))
                else:
                    email = ((result.get("account_payload") or {}).get("email") or "unknown").strip() or "unknown"
                    self._write_html(
                        200,
                        oauth_callback_result_html(True, "OAuth login complete", f"Saved account {result['filename']} ({email})."),
                    )
            except ProxyError as exc:
                self._write_html(exc.status, oauth_callback_result_html(False, "Login Failed", exc.message))
            return
        if path.startswith("/assets/") or path.endswith(".js") or path.endswith(".css"):
            file_path = os.path.join(WEB_DIR, path.lstrip("/"))
            if os.path.isfile(file_path):
                content_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
                with open(file_path, "rb") as f:
                    self._write_text(200, f.read(), content_type)
                return
        self._write_json(404, {"error": {"type": "not_found", "message": "not found"}})

    def do_POST(self):
        self._reset_request_tracking()
        path = self._request_path()
        if path == "/auth/login":
            size = int(self.headers.get("content-length", "0") or "0")
            raw = self.rfile.read(size).decode("utf-8", errors="replace")
            content_type = self.headers.get("content-type", "")
            if "application/json" in content_type:
                try:
                    payload = json.loads(raw or "{}")
                except json.JSONDecodeError:
                    payload = {}
                password = str(payload.get("password") or "")
            else:
                form = urllib.parse.parse_qs(raw)
                password = str((form.get("password") or [""])[0])
            secret = dashboard_secret()
            if secret and password != secret:
                self._write_json(401, {"error": {"type": "authentication_error", "message": "invalid dashboard password"}})
                return
            self.send_response(200)
            self._set_dashboard_session()
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/auth/login-start":
            if not self._require_dashboard_access() and not is_local_request(self._client_ip()):
                return
            try:
                payload = self._read_json()
            except Exception:
                payload = {}
            verifier = secrets.token_urlsafe(48)
            challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("utf-8")).digest()).decode("ascii").rstrip("=")
            state = secrets.token_urlsafe(24)
            redirect_uri = oauth_redirect_uri()
            RUNTIME_SETTINGS["oauth_pkce"] = {
                "state": state,
                "verifier": verifier,
                "redirect_uri": redirect_uri,
                "created_at": now_iso(),
                "requested_by": self._client_ip(),
                "source": str((payload or {}).get("source") or "dashboard"),
            }
            save_runtime_settings(RUNTIME_SETTINGS)
            callback_server = start_oauth_callback_server()
            authorize_url = (
                AUTHORIZE_URL + "?"
                + urllib.parse.urlencode(
                    {
                        "response_type": "code",
                        "client_id": CLIENT_ID,
                        "redirect_uri": redirect_uri,
                        "scope": "openid profile email offline_access",
                        "code_challenge": challenge,
                        "code_challenge_method": "S256",
                        "originator": "codex_cli_rs",
                        "codex_cli_simplified_flow": "true",
                        "state": state,
                    }
                )
            )
            self._write_json(
                200,
                {
                    "state": state,
                    "authorize_url": authorize_url,
                    "redirect_uri": redirect_uri,
                    "callback_server": callback_server,
                },
            )
            return

        if path == "/auth/code-relay":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            callback_url = str((payload.get("callback_url") or payload.get("callbackUrl") or "") if isinstance(payload, dict) else "").strip()
            if not callback_url:
                self._write_json(400, {"error": {"type": "invalid_request_error", "message": "callbackUrl is required"}})
                return
            try:
                url = urllib.parse.urlsplit(callback_url)
            except Exception:
                self._write_json(400, {"error": {"type": "invalid_request_error", "message": "invalid callbackUrl"}})
                return
            params = urllib.parse.parse_qs(url.query)
            error = (params.get("error") or [""])[0]
            error_description = (params.get("error_description") or [""])[0]
            if error:
                self._write_json(400, {"error": {"type": "authentication_error", "message": error_description or error}})
                return
            code = (params.get("code") or [""])[0]
            state = (params.get("state") or [""])[0]
            try:
                result = complete_oauth_callback(code, state)
                self._write_json(
                    200,
                    {
                        "ok": True,
                        "already_completed": bool(result.get("already_completed")),
                        "filename": result.get("filename"),
                        "email": ((result.get("account_payload") or {}).get("email") or ""),
                    },
                )
            except ProxyError as exc:
                self._write_json(exc.status, {"error": {"type": exc.error_type, "message": exc.message}})
            return

        if path == "/auth/accounts/import":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            accounts = payload.get("accounts") if isinstance(payload, dict) else None
            if isinstance(payload, dict) and accounts is None and "tokens" in payload:
                accounts = [payload]
            if not isinstance(accounts, list):
                self._write_json(400, {"error": {"type": "invalid_request_error", "message": "accounts must be a list"}})
                return
            imported = []
            os.makedirs(AUTH_DIR, exist_ok=True)
            for index, account_payload in enumerate(accounts):
                if not isinstance(account_payload, dict):
                    continue
                filename = str(account_payload.get("filename") or account_payload.get("email") or f"imported_{index}.json").strip()
                if not filename.endswith(".json"):
                    filename += ".json"
                auth_file = os.path.join(AUTH_DIR, os.path.basename(filename))
                try:
                    write_codex_auth_file(auth_file, account_payload)
                except RuntimeError as exc:
                    self._write_json(400, {"error": {"type": "invalid_request_error", "message": str(exc), "filename": os.path.basename(filename)}})
                    return
                imported.append(os.path.basename(auth_file))
            pool.reload()
            sync_accounts_with_state()
            self._write_json(200, {"imported": imported, "count": len(imported)})
            return

        if path == "/auth/codex-app/select":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            entry_id = os.path.basename(str((payload.get("entry_id") if isinstance(payload, dict) else "") or "")).strip()
            if not entry_id:
                self._write_json(400, {"error": {"type": "invalid_request_error", "message": "entry_id is required"}})
                return
            auth_file = os.path.join(AUTH_DIR, entry_id)
            if not os.path.isfile(auth_file):
                self._write_json(404, {"error": {"type": "not_found", "message": "account not found"}})
                return
            source_payload = read_json_file(auth_file, {})
            try:
                normalized = write_codex_auth_file(CODEX_AUTH_PATH, source_payload)
            except RuntimeError as exc:
                self._write_json(400, {"error": {"type": "invalid_request_error", "message": str(exc)}})
                return
            selection = save_codex_app_selection(entry_id, auth_identity_key_from_payload(normalized))
            sync_accounts_with_state()
            codex_app = current_codex_app_state()
            self._write_json(
                200,
                {
                    "ok": True,
                    "selected_entry_id": entry_id,
                    "selected_identity_key": selection.get("selected_identity_key") or "",
                    "codex_app": codex_app,
                },
            )
            return

        if path == "/auth/accounts/batch-delete":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            ids = payload.get("ids") if isinstance(payload, dict) else []
            deleted = []
            for entry_id in ids or []:
                entry_id = os.path.basename(str(entry_id))
                auth_file = os.path.join(AUTH_DIR, entry_id)
                if os.path.isfile(auth_file):
                    os.remove(auth_file)
                STATE_DB.delete_account(entry_id)
                COOKIE_STORE.pop(entry_id, None)
                deleted.append(entry_id)
            save_cookie_store(COOKIE_STORE)
            pool.reload()
            self._write_json(200, {"deleted": deleted})
            return

        if path == "/auth/accounts/batch-status":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            ids = payload.get("ids") if isinstance(payload, dict) else []
            status = str((payload.get("status") if isinstance(payload, dict) else "") or "").strip() or "active"
            for entry_id in ids or []:
                set_account_status(os.path.basename(str(entry_id)), status)
            self._write_json(200, {"updated": [os.path.basename(str(item)) for item in ids or []], "status": status})
            return

        if path.startswith("/auth/accounts/") and path.endswith("/cookies"):
            if not self._require_dashboard_access():
                return
            account_id = urllib.parse.unquote(path[len("/auth/accounts/") : -len("/cookies")]).strip("/")
            payload = self._read_json()
            COOKIE_STORE[account_id] = payload if isinstance(payload, dict) else {}
            save_cookie_store(COOKIE_STORE)
            self._write_json(200, {"account_id": account_id, "cookies": COOKIE_STORE[account_id]})
            return

        if path == "/api/proxies":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            proxy_id = str(payload.get("proxy_id") or payload.get("id") or secrets.token_hex(6))
            proxy = STATE_DB.upsert_proxy(
                proxy_id,
                name=str(payload.get("name") or proxy_id),
                url=str(payload.get("url") or ""),
                status=str(payload.get("status") or "active"),
                health=payload.get("health") if isinstance(payload.get("health"), dict) else {},
                metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
            )
            assignment_mode = str(payload.get("assignment_mode") or payload.get("mode") or "specific")
            if "global_proxy_id" in payload:
                RUNTIME_SETTINGS["proxy_defaults"] = {
                    **(RUNTIME_SETTINGS.get("proxy_defaults") or {}),
                    "global_proxy_id": str(payload.get("global_proxy_id") or ""),
                }
                save_runtime_settings(RUNTIME_SETTINGS)
            if payload.get("account_id"):
                set_account_proxy_mode(str(payload.get("account_id")), assignment_mode, proxy_id=proxy_id)
            self._write_json(200, proxy)
            return

        if path == "/api/proxies/health-check":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            proxy_id = str(payload.get("proxy_id") or "").strip() if isinstance(payload, dict) else ""
            if proxy_id:
                proxy = STATE_DB.get_proxy(proxy_id)
                if proxy is None:
                    self._write_json(404, {"error": {"type": "not_found", "message": "proxy not found"}})
                    return
                self._write_json(200, {"data": [proxy_health_check(proxy)]})
                return
            self._write_json(200, {"data": [proxy_health_check(proxy) for proxy in STATE_DB.list_proxies()]})
            return

        if path == "/api/relay-providers":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            provider_id = str(payload.get("provider_id") or payload.get("id") or secrets.token_hex(6))
            provider = STATE_DB.upsert_relay_provider(
                provider_id,
                name=str(payload.get("name") or provider_id),
                base_url=str(payload.get("base_url") or ""),
                api_key=str(payload.get("api_key") or ""),
                format=str(payload.get("format") or "responses"),
                enabled=bool(payload.get("enabled", True)),
                metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
            )
            self._write_json(200, provider)
            return

        if path.startswith("/auth/accounts/") and path.endswith("/reset-usage"):
            if not self._require_dashboard_access():
                return
            entry_id = urllib.parse.unquote(path[len("/auth/accounts/") : -len("/reset-usage")]).strip("/")
            account = STATE_DB.get_account(entry_id)
            if account is None:
                self._write_json(404, {"error": {"type": "not_found", "message": "account not found"}})
                return
            update_account_record(entry_id, usage={"input_tokens": 0, "output_tokens": 0, "request_count": 0})
            self._write_json(200, {"entry_id": entry_id, "reset": True})
            return

        if path == "/admin/settings":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            if isinstance(payload, dict):
                RUNTIME_SETTINGS.update(payload)
                save_runtime_settings(RUNTIME_SETTINGS)
            self._write_json(200, RUNTIME_SETTINGS)
            return

        if path == "/admin/runtime-jobs/run":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            job = str((payload.get("job") if isinstance(payload, dict) else "") or "").strip()
            if job == "quota_refresh":
                self._write_json(200, refresh_all_account_quotas())
                return
            if job == "proxy_health":
                self._write_json(200, {"data": [proxy_health_check(proxy) for proxy in STATE_DB.list_proxies()]})
                return
            if job == "fingerprint_refresh":
                self._write_json(200, refresh_fingerprint_cache(force=True))
                return
            if job == "token_refresh":
                self._write_json(200, {"refreshed": refresh_accounts_if_needed(force=True)})
                return
            self._write_json(400, {"error": {"type": "invalid_request_error", "message": "unknown runtime job"}})
            return

        if path == "/admin/rotation-settings":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            if isinstance(payload, dict):
                if "rotation_mode" in payload:
                    RUNTIME_SETTINGS["rotation_mode"] = str(payload.get("rotation_mode") or "least_used")
                if isinstance(payload.get("plans"), dict):
                    RUNTIME_SETTINGS["plans"] = payload.get("plans")
                if "responses_transport" in payload:
                    RUNTIME_SETTINGS["responses_transport"] = str(payload.get("responses_transport") or "auto")
                save_runtime_settings(RUNTIME_SETTINGS)
            self._write_json(
                200,
                {
                    "rotation_mode": current_rotation_mode(),
                    "responses_transport": current_responses_transport_mode(),
                    "plans": RUNTIME_SETTINGS.get("plans") or {},
                },
            )
            return

        if path == "/admin/quota-settings":
            if not self._require_dashboard_access():
                return
            payload = self._read_json()
            if isinstance(payload, dict):
                RUNTIME_SETTINGS["quota"] = {
                    **(RUNTIME_SETTINGS.get("quota") or {}),
                    **payload,
                }
                save_runtime_settings(RUNTIME_SETTINGS)
            self._write_json(200, RUNTIME_SETTINGS.get("quota") or {})
            return

        if path == "/admin/quota-refresh":
            if not self._require_dashboard_access():
                return
            self._write_json(200, refresh_all_account_quotas())
            return

        if path == "/admin/test-connection":
            if not self._require_dashboard_access():
                return
            sync_accounts_with_state()
            payload = self._read_json()
            try:
                result = run_connection_diagnostics(payload)
                result["transport_backend"] = current_transport_backend()
                result["responses_transport"] = current_responses_transport_mode()
                self._write_json(200, result)
            except urllib.error.HTTPError as exc:
                self._write_json(
                    502,
                    {
                        "ok": False,
                        "target": "upstream",
                        "error": str(exc),
                        "status_code": exc.code,
                        "transport_backend": current_transport_backend(),
                        "responses_transport": current_responses_transport_mode(),
                    },
                )
            except Exception as exc:
                self._write_json(
                    502,
                    {
                        "ok": False,
                        "target": "upstream",
                        "error": str(exc),
                        "transport_backend": current_transport_backend(),
                        "responses_transport": current_responses_transport_mode(),
                    },
                )
            return

        if path.startswith("/v1beta/models/"):
            if not self._require_api_key():
                return
            parsed = parse_model_action(path[len("/v1beta/models/") :])
            if parsed is None or parsed.action not in GEMINI_ACTIONS:
                self._write_json(404, {"error": {"type": "not_found", "message": "unknown gemini action"}})
                return
            raw_payload = self._read_json()
            session_context = extract_session_context(self.headers, raw_payload, self.client_address[0])
            session_key = session_context["session_key"]
            try:
                translated = translate_gemini_request(raw_payload, parsed.model, default_reasoning_effort=DEFAULT_REASONING_EFFORT)
                payload = translated.codex_request
                ensure_prompt_cache_key(payload, session_key)
                payload["stream"] = True
            except Exception as exc:
                self._write_json(400, {"error": {"type": "invalid_request_error", "message": str(exc)}})
                return
            try:
                if parsed.action == "streamGenerateContent":
                    with session_coordinator.hold(session_key):
                        response = self._stream_gemini_from_upstream(payload, session_key, model=parsed.model, tuple_schema=translated.tuple_schema)
                else:
                    with session_coordinator.hold(session_key):
                        response = self._fetch_final_response(payload, session_key)
                    self._write_json(200, codex_response_to_gemini(response, parsed.model, tuple_schema=translated.tuple_schema))
                if response is not None:
                    self._record_completed_transcript(
                        path,
                        session_context,
                        parsed.model,
                        payload,
                        raw_payload,
                        transcript_response_section_for_responses(response),
                    )
            except urllib.error.HTTPError as exc:
                self._forward_http_error(exc)
            except Exception as exc:
                self._write_json(502, {"error": {"type": "upstream_error", "message": str(exc)}})
            return

        if path == "/v1/messages/count_tokens":
            if not self._require_api_key_anthropic():
                return
            try:
                self._require_anthropic_version()
                raw_payload = self._read_json()
                _, payload = build_responses_payload_from_anthropic(raw_payload)
            except ProxyError as exc:
                self._write_anthropic_error(exc.status, exc.error_type, exc.message)
                return
            except Exception as exc:
                self._write_anthropic_error(400, "invalid_request_error", str(exc))
                return
            self._write_json(200, {"input_tokens": estimate_request_tokens(payload)})
            return

        if path == "/v1/messages":
            if not self._require_api_key_anthropic():
                return
            raw_payload = {}
            payload = {}
            session_context = {}
            requested_model = ""
            try:
                self._require_anthropic_version()
                raw_payload = self._read_json()
                requested_model, payload = build_responses_payload_from_anthropic(raw_payload)
                session_context = extract_session_context(self.headers, raw_payload, self.client_address[0])
                session_key = session_context["session_key"]
                ensure_prompt_cache_key(payload, session_key)
                payload["stream"] = True
            except ProxyError as exc:
                self._record_failed_transcript(
                    path,
                    session_context,
                    requested_model or str(raw_payload.get("model") or ""),
                    payload or {"input": [], "model": ""},
                    raw_payload,
                    "proxy_error",
                    exc.error_type,
                    exc.message,
                    exc.status,
                    anthropic_error_payload(exc.error_type, exc.message),
                )
                self._write_anthropic_error(exc.status, exc.error_type, exc.message)
                return
            except Exception as exc:
                self._record_failed_transcript(
                    path,
                    session_context,
                    requested_model or str(raw_payload.get("model") or ""),
                    payload or {"input": [], "model": ""},
                    raw_payload,
                    "proxy_error",
                    "invalid_request_error",
                    str(exc),
                    400,
                    str(exc),
                )
                self._write_anthropic_error(400, "invalid_request_error", str(exc))
                return

            estimated_tokens, budget_spec, budget_error = validate_context_budget(
                payload, requested_model=anthropic_budget_model(requested_model)
            )
            if budget_error:
                self._record_failed_transcript(
                    path,
                    session_context,
                    requested_model,
                    payload,
                    raw_payload,
                    "proxy_error",
                    "invalid_request_error",
                    budget_error,
                    400,
                    budget_error,
                )
                self._write_json(
                    400,
                    {
                        "type": "error",
                        "error": {
                            "type": "invalid_request_error",
                            "message": budget_error,
                            "estimated_input_tokens": estimated_tokens,
                            "requested_model": budget_spec["requested_model"],
                            "effective_model": budget_spec["effective_model"],
                            "model_context_window": budget_spec["context_window"],
                            "model_auto_compact_token_limit": budget_spec["auto_compact_token_limit"],
                        },
                    },
                )
                return

            try:
                if raw_payload.get("stream"):
                    include_thinking = bool((raw_payload.get("thinking") or {}).get("enabled"))
                    if self._should_use_buffered_anthropic_sse():
                        with session_coordinator.hold(session_key):
                            response = self._fetch_final_response(payload, session_key)
                        message_payload = response_to_anthropic_message(response, requested_model)
                        self._write_anthropic_message_sse(message_payload)
                    else:
                        try:
                            with session_coordinator.hold(session_key):
                                response = self._stream_anthropic_message_from_upstream(
                                    payload,
                                    session_key,
                                    model=requested_model,
                                    include_thinking=include_thinking,
                                )
                        except RuntimeError:
                            with session_coordinator.hold(session_key):
                                response = self._fetch_final_response(payload, session_key)
                            message_payload = response_to_anthropic_message(response, requested_model)
                            self._write_anthropic_message_sse(message_payload)
                        else:
                            message_payload = response_to_anthropic_message(response, requested_model) if response else None
                else:
                    with session_coordinator.hold(session_key):
                        response = self._fetch_final_response(payload, session_key)
                    message_payload = response_to_anthropic_message(response, requested_model)
                    self._write_json(200, message_payload, extra_headers=self._response_metric_headers(message_payload.get("usage") or {}))
                self._record_completed_transcript(
                    path,
                    session_context,
                    requested_model,
                    payload,
                    raw_payload,
                    transcript_response_section_for_anthropic(message_payload or response_to_anthropic_message(response, requested_model)),
                )
            except ProxyError as exc:
                self._record_failed_transcript(
                    path,
                    session_context,
                    requested_model,
                    payload,
                    raw_payload,
                    "proxy_error",
                    exc.error_type,
                    exc.message,
                    exc.status,
                    anthropic_error_payload(exc.error_type, exc.message),
                )
                self._write_anthropic_error(exc.status, exc.error_type, exc.message)
            except urllib.error.HTTPError as exc:
                details = self._http_error_details(exc)
                self._record_failed_transcript(
                    path,
                    session_context,
                    requested_model,
                    payload,
                    raw_payload,
                    "upstream_http_error",
                    anthropic_status_error_type(exc.code),
                    details["message"],
                    details["status_code"],
                    details["body"],
                )
                self._forward_anthropic_http_error(exc)
            except Exception as exc:
                self._record_failed_transcript(
                    path,
                    session_context,
                    requested_model,
                    payload,
                    raw_payload,
                    "proxy_error",
                    "api_error",
                    str(exc),
                    502,
                    str(exc),
                )
                self._write_anthropic_error(502, "api_error", str(exc))
            return

        if path == "/v1/chat/completions":
            if not self._require_api_key():
                return
            raw_payload = {}
            payload = {}
            session_context = {}
            try:
                raw_payload = self._read_json()
                payload = build_responses_payload_from_chat(raw_payload)
                session_context = extract_session_context(self.headers, raw_payload, self.client_address[0])
                session_key = session_context["session_key"]
                ensure_prompt_cache_key(payload, session_key)
                payload["stream"] = True
            except Exception as exc:
                self._record_failed_transcript(
                    path,
                    session_context,
                    str(raw_payload.get("model") or ""),
                    payload or {"input": [], "model": ""},
                    raw_payload,
                    "proxy_error",
                    "invalid_request_error",
                    str(exc),
                    400,
                    str(exc),
                )
                self._write_json(400, {"error": {"type": "invalid_request_error", "message": str(exc)}})
                return
            requested_model_name = str(raw_payload.get("model") or payload.get("model") or DEFAULT_MODEL)
            estimated_tokens, budget_spec, budget_error = validate_context_budget(
                payload, requested_model=requested_model_name
            )
            if budget_error:
                self._record_failed_transcript(
                    path,
                    session_context,
                    str(raw_payload.get("model") or payload.get("model") or ""),
                    payload,
                    raw_payload,
                    "proxy_error",
                    "context_limit_error",
                    budget_error,
                    413,
                    budget_error,
                )
                self._write_json(
                    413,
                    {
                        "error": {
                            "type": "context_limit_error",
                            "message": budget_error,
                            "estimated_input_tokens": estimated_tokens,
                            "requested_model": budget_spec["requested_model"],
                            "effective_model": budget_spec["effective_model"],
                            "model_context_window": budget_spec["context_window"],
                            "model_auto_compact_token_limit": budget_spec["auto_compact_token_limit"],
                        }
                    },
                )
                return
            try:
                if raw_payload.get("stream"):
                    stream_options = raw_payload.get("stream_options") or {}
                    include_usage = bool(stream_options.get("include_usage"))
                    include_reasoning = bool(raw_payload.get("reasoning"))
                    try:
                        with session_coordinator.hold(session_key):
                            response = self._stream_chat_completion_from_upstream(
                                payload,
                                session_key,
                                model=str(raw_payload.get("model") or payload.get("model") or DEFAULT_MODEL),
                                include_usage=include_usage,
                                include_reasoning=include_reasoning,
                            )
                    except RuntimeError:
                        with session_coordinator.hold(session_key):
                            response = self._fetch_final_response(payload, session_key)
                        self._write_chat_completion_sse(response, include_usage=include_usage)
                    completion_payload = response_to_chat_completion(response)
                else:
                    with session_coordinator.hold(session_key):
                        response = self._fetch_final_response(payload, session_key)
                    completion_payload = response_to_chat_completion(response)
                    self._write_json(200, completion_payload, extra_headers=self._response_metric_headers(completion_payload.get("usage") or {}))
                self._record_completed_transcript(
                    path,
                    session_context,
                    str(raw_payload.get("model") or payload.get("model") or ""),
                    payload,
                    raw_payload,
                    transcript_response_section_for_chat_completion(completion_payload),
                )
            except urllib.error.HTTPError as exc:
                details = self._http_error_details(exc)
                self._record_failed_transcript(
                    path,
                    session_context,
                    str(raw_payload.get("model") or payload.get("model") or ""),
                    payload,
                    raw_payload,
                    "upstream_http_error",
                    "upstream_error",
                    details["message"],
                    details["status_code"],
                    details["body"],
                )
                self._forward_http_error(exc)
            except Exception as exc:
                self._record_failed_transcript(
                    path,
                    session_context,
                    str(raw_payload.get("model") or payload.get("model") or ""),
                    payload,
                    raw_payload,
                    "proxy_error",
                    "upstream_error",
                    str(exc),
                    502,
                    str(exc),
                )
                self._write_json(502, {"error": {"type": "upstream_error", "message": str(exc)}})
            return

        if path != "/v1/responses":
            self._write_json(404, {"error": {"type": "not_found", "message": "not found"}})
            return
        if not self._require_api_key():
            return
        raw_payload = {}
        payload = {}
        session_context = {}
        try:
            raw_payload = self._read_json()
            payload = normalize_payload(raw_payload)
            session_context = extract_session_context(self.headers, raw_payload, self.client_address[0])
            session_key = session_context["session_key"]
            ensure_prompt_cache_key(payload, session_key)
            payload["_codex2gpt_enable_previous_response_id"] = True
        except Exception as exc:
            self._record_failed_transcript(
                path,
                session_context,
                str(raw_payload.get("model") or ""),
                payload or {"input": [], "model": ""},
                raw_payload,
                "proxy_error",
                "invalid_request_error",
                str(exc),
                400,
                str(exc),
            )
            self._write_json(400, {"error": {"type": "invalid_request_error", "message": str(exc)}})
            return

        requested_model_name = str(raw_payload.get("model") or payload.get("model") or DEFAULT_MODEL)
        estimated_tokens, budget_spec, budget_error = validate_context_budget(
            payload, requested_model=requested_model_name
        )
        if budget_error:
            self._record_failed_transcript(
                path,
                session_context,
                str(raw_payload.get("model") or payload.get("model") or ""),
                payload,
                raw_payload,
                "proxy_error",
                "context_limit_error",
                budget_error,
                413,
                budget_error,
            )
            self._write_json(
                413,
                {
                    "error": {
                        "type": "context_limit_error",
                        "message": budget_error,
                        "estimated_input_tokens": estimated_tokens,
                        "requested_model": budget_spec["requested_model"],
                        "effective_model": budget_spec["effective_model"],
                        "model_context_window": budget_spec["context_window"],
                        "model_auto_compact_token_limit": budget_spec["auto_compact_token_limit"],
                    }
                },
            )
            return

        wants_stream = bool(raw_payload.get("stream"))
        payload["stream"] = True

        try:
            with session_coordinator.hold(session_key):
                response = self._forward_responses(payload, wants_stream, session_key)
            if response is None:
                self._record_failed_transcript(
                    path,
                    session_context,
                    str(raw_payload.get("model") or payload.get("model") or ""),
                    payload,
                    raw_payload,
                    "proxy_error",
                    "upstream_error",
                    "failed to extract final response from upstream stream",
                    502,
                    "failed to extract final response from upstream stream",
                )
            else:
                self._record_completed_transcript(
                    path,
                    session_context,
                    str(raw_payload.get("model") or payload.get("model") or ""),
                    payload,
                    raw_payload,
                    transcript_response_section_for_responses(response),
                )
        except urllib.error.HTTPError as exc:
            details = self._http_error_details(exc)
            self._record_failed_transcript(
                path,
                session_context,
                str(raw_payload.get("model") or payload.get("model") or ""),
                payload,
                raw_payload,
                "upstream_http_error",
                "upstream_error",
                details["message"],
                details["status_code"],
                details["body"],
            )
            self._forward_http_error(exc)
        except Exception as exc:
            self._record_failed_transcript(
                path,
                session_context,
                str(raw_payload.get("model") or payload.get("model") or ""),
                payload,
                raw_payload,
                "proxy_error",
                "upstream_error",
                str(exc),
                502,
                str(exc),
            )
            self._write_json(502, {"error": {"type": "upstream_error", "message": str(exc)}})

    def _upstream_once(self, payload, account, session_key, allow_refresh=True):
        websocket_enabled = should_use_websocket_for_payload(payload)
        payload = apply_session_response_context(dict(payload), session_key)
        relay_provider = None
        proxy_url = None
        if account is None:
            body = canonical_json_bytes(payload)
            relay_provider = enabled_relay_provider()
            if relay_provider is None:
                raise RuntimeError(f"no oauth json found in {AUTH_DIR}")
            return perform_relay_request(relay_provider, payload, timeout=120)
        else:
            headers = build_upstream_headers(account, session_key)
            request_url = UPSTREAM_URL
            proxy_url = resolve_proxy_url_for_account(account.name)
            if websocket_enabled and websocket_transport_available():
                try:
                    return WebSocketSSEUpstreamResponse(
                        build_websocket_url(request_url),
                        build_websocket_headers(account),
                        build_websocket_request(payload),
                        proxy_url=proxy_url,
                        timeout=120,
                    )
                except Exception:
                    payload.pop("previous_response_id", None)
            body = canonical_json_bytes(payload)
        req = urllib.request.Request(request_url, data=body, headers=headers, method="POST")
        try:
            response = upstream_request_with_transport_fallback(
                req,
                proxy_url=proxy_url,
                timeout=120,
                account_name=account.name if account is not None else "",
            )
            if account is not None:
                capture_set_cookie_headers(account.name, getattr(response, "headers", None))
            return response
        except urllib.error.HTTPError as exc:
            if account is not None:
                capture_set_cookie_headers(account.name, getattr(exc, "headers", None))
            if account is not None and proxy_url and exc.code in {403, 408, 409, 429, 500, 502, 503, 504}:
                fallback = urlopen_with_optional_proxy(req, proxy_url=None, timeout=120)
                capture_set_cookie_headers(account.name, getattr(fallback, "headers", None))
                return fallback
            if account is not None and allow_refresh and exc.code in {401, 403}:
                account.refresh_access_token()
                return self._upstream_once(payload, account, session_key, allow_refresh=False)
            raise
        except urllib.error.URLError:
            if account is not None and proxy_url:
                fallback = urlopen_with_optional_proxy(req, proxy_url=None, timeout=120)
                capture_set_cookie_headers(account.name, getattr(fallback, "headers", None))
                return fallback
            raise

    def _upstream(self, payload, session_key):
        sync_accounts_with_state()
        accounts = pool.candidates(session_key, str(payload.get("model") or ""))
        if not accounts:
            relay_provider = enabled_relay_provider()
            if relay_provider is not None:
                upstream = self._upstream_once(payload, None, session_key, allow_refresh=False)
                self._last_attempted_account_name = str(relay_provider.get("provider_id") or "relay_http")
                self._last_account_name = self._last_attempted_account_name
                return upstream
            raise RuntimeError(f"no oauth json found in {AUTH_DIR}")
        last_error = None
        preferred_error = None
        for account in accounts:
            try:
                upstream = self._upstream_once(payload, account, session_key)
                self._last_attempted_account_name = account.name
                self._last_account_name = account.name
                pool.mark_success(account.name)
                set_account_status(account.name, "active")
                if session_key:
                    pool.bind_session(session_key, account.name)
                return upstream
            except Exception as exc:
                last_error = exc
                self._last_attempted_account_name = account.name
                if isinstance(exc, urllib.error.HTTPError):
                    if exc.code == 401:
                        set_account_status(account.name, "expired", last_error=str(exc))
                    elif exc.code == 403:
                        set_account_status(account.name, "banned", last_error=str(exc))
                    elif exc.code == 429:
                        set_account_status(account.name, "rate_limited", last_error=str(exc))
                    else:
                        set_account_status(account.name, "error", last_error=str(exc))
                else:
                    set_account_status(account.name, "error", last_error=str(exc))
                if not is_account_unusable_error(exc):
                    preferred_error = exc
                pool.mark_failure(account.name, exc)
                if not is_retryable_error(exc) and not is_account_unusable_error(exc):
                    raise
        raise preferred_error or last_error or RuntimeError("all accounts failed")

    def _fetch_final_response(self, payload, session_key):
        with self._upstream(payload, session_key) as upstream:
            sse_body = upstream.read().decode("utf-8", errors="replace")
        response = extract_final_response(sse_body)
        if response is None:
            raise RuntimeError("failed to extract final response from upstream stream")
        session_coordinator.remember_response(session_key, response.get("id"))
        record_account_usage(self._current_account_name(), response)
        return response

    def _forward_responses(self, payload, wants_stream, session_key):
        with self._upstream(payload, session_key) as upstream:
            if wants_stream:
                chunks = []
                self._send_sse_headers(
                    upstream.status,
                    extra_headers=self._response_metric_headers(),
                )
                while True:
                    chunk = upstream.read(4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    self.wfile.write(chunk)
                    self.wfile.flush()
                sse_body = b"".join(chunks).decode("utf-8", errors="replace")
                response = extract_final_response(sse_body)
                if response is not None:
                    session_coordinator.remember_response(session_key, response.get("id"))
                    record_account_usage(self._current_account_name(), response)
                return response

            response = extract_final_response(upstream.read().decode("utf-8", errors="replace"))
            if response is None:
                raise RuntimeError("failed to extract final response from upstream stream")
            session_coordinator.remember_response(session_key, response.get("id"))
            record_account_usage(self._current_account_name(), response)
            self._write_json(200, response, extra_headers=self._response_metric_headers(response.get("usage") or {}))
            return response

    def _write_chat_completion_sse(self, response, include_usage=False):
        frames = [f"data: {json.dumps(chat_completion_chunk_from_response(response), ensure_ascii=False)}\n\n"]
        if include_usage:
            usage_chunk = chat_completion_usage_chunk_from_response(response)
            frames.append(f"data: {json.dumps(usage_chunk, ensure_ascii=False)}\n\n")
        frames.append("data: [DONE]\n\n")
        body = "".join(frames).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        for key, value in self._response_metric_headers(response.get("usage") or {}).items():
            self.send_header(str(key), str(value))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def _stream_chat_completion_from_upstream(self, payload, session_key, *, model, include_usage=False, include_reasoning=False):
        with self._upstream(payload, session_key) as upstream:
            reader = RecordingUpstreamReader(upstream)
            self._send_sse_headers(
                getattr(upstream, "status", 200),
                extra_headers=self._response_metric_headers(),
            )
            for chunk in iter_chat_completion_sse(
                reader,
                model,
                include_usage=include_usage,
                include_reasoning=include_reasoning,
            ):
                self.wfile.write(chunk)
                self.wfile.flush()
            response = extract_final_response(reader.body_text())
        if response is not None:
            session_coordinator.remember_response(session_key, response.get("id"))
            record_account_usage(self._current_account_name(), response)
        return response

    def _write_anthropic_message_sse(self, message_payload):
        body = anthropic_sse_body_from_message(message_payload)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        for key, value in self._response_metric_headers(message_payload.get("usage") or {}).items():
            self.send_header(str(key), str(value))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def _stream_anthropic_message_from_upstream(self, payload, session_key, *, model, include_thinking=False):
        with self._upstream(payload, session_key) as upstream:
            reader = RecordingUpstreamReader(upstream)
            self._send_sse_headers(
                getattr(upstream, "status", 200),
                extra_headers=self._response_metric_headers(),
            )
            for chunk in iter_anthropic_message_sse(reader, model, include_thinking=include_thinking):
                self.wfile.write(chunk)
                self.wfile.flush()
            response = extract_final_response(reader.body_text())
        if response is not None:
            session_coordinator.remember_response(session_key, response.get("id"))
            record_account_usage(self._current_account_name(), response)
        return response

    def _stream_gemini_from_upstream(self, payload, session_key, *, model, tuple_schema=None):
        with self._upstream(payload, session_key) as upstream:
            reader = RecordingUpstreamReader(upstream)
            def event_iter():
                return iter_sse_messages(iter(lambda: reader.read(4096), b""))
            self._send_sse_headers(
                getattr(upstream, "status", 200),
                extra_headers=self._response_metric_headers(),
            )
            for chunk in stream_gemini_sse_from_codex_events(event_iter(), model, tuple_schema=tuple_schema):
                self.wfile.write(chunk.encode("utf-8"))
                self.wfile.flush()
            response = extract_final_response(reader.body_text())
        if response is not None:
            session_coordinator.remember_response(session_key, response.get("id"))
            record_account_usage(self._current_account_name(), response)
        return response

    def _forward_http_error(self, exc):
        body = read_http_error_body(exc).decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"error": {"type": "upstream_error", "message": body or str(exc)}}
        self._write_json(exc.code, payload)

    def _forward_anthropic_http_error(self, exc):
        body = read_http_error_body(exc).decode("utf-8", errors="replace")
        message = body or str(exc)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            error = payload.get("error")
            if isinstance(error, dict) and isinstance(error.get("message"), str):
                message = error["message"]
            elif isinstance(payload.get("detail"), dict) and isinstance(payload["detail"].get("message"), str):
                message = payload["detail"]["message"]
        self._write_anthropic_error(exc.code, anthropic_status_error_type(exc.code), message)

    def do_DELETE(self):
        path = self._request_path()
        if path.startswith("/auth/accounts/") and path.endswith("/cookies"):
            if not self._require_dashboard_access():
                return
            account_id = urllib.parse.unquote(path[len("/auth/accounts/") : -len("/cookies")]).strip("/")
            COOKIE_STORE.pop(account_id, None)
            save_cookie_store(COOKIE_STORE)
            self._write_json(200, {"deleted": True, "account_id": account_id})
            return
        if path.startswith("/api/proxies/"):
            if not self._require_dashboard_access():
                return
            proxy_id = urllib.parse.unquote(path[len("/api/proxies/") :]).strip("/")
            STATE_DB.delete_proxy(proxy_id)
            self._write_json(200, {"deleted": True, "proxy_id": proxy_id})
            return
        if path.startswith("/api/relay-providers/"):
            if not self._require_dashboard_access():
                return
            provider_id = urllib.parse.unquote(path[len("/api/relay-providers/") :]).strip("/")
            STATE_DB.delete_relay_provider(provider_id)
            self._write_json(200, {"deleted": True, "provider_id": provider_id})
            return
        if path == "/auth/login":
            self.send_response(200)
            self._clear_dashboard_session()
            body = json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8")
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self._write_json(404, {"error": {"type": "not_found", "message": "not found"}})

    def log_message(self, fmt, *args):
        return


def main():
    pool.reload()
    sync_accounts_with_state()
    if pool.size() == 0 and enabled_relay_provider() is None:
        raise SystemExit(f"no oauth json found in {AUTH_DIR}")
    background = RUNTIME_SETTINGS.get("background_jobs") or {}
    BACKGROUND_JOBS.start(
        "quota_refresh",
        int(background.get("quota_refresh_seconds") or 300),
        refresh_all_account_quotas,
    )
    BACKGROUND_JOBS.start(
        "proxy_health",
        int(background.get("proxy_health_seconds") or 300),
        lambda: [proxy_health_check(proxy) for proxy in STATE_DB.list_proxies()],
    )
    BACKGROUND_JOBS.start(
        "fingerprint_refresh",
        int(background.get("fingerprint_refresh_seconds") or 3600),
        refresh_fingerprint_cache,
    )
    BACKGROUND_JOBS.start(
        "token_refresh",
        int(background.get("token_refresh_seconds") or 300),
        refresh_accounts_if_needed,
    )
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    print(f"lite api listening on http://{LISTEN_HOST}:{LISTEN_PORT} with {pool.size()} account(s)", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
