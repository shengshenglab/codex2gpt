#!/usr/bin/env python3
import io
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUNTIME_DIR = os.path.join(BASE_DIR, "runtime")
AUTH_DIR = os.environ.get("LITE_AUTH_DIR", os.path.join(RUNTIME_DIR, "accounts"))
LISTEN_HOST = os.environ.get("LITE_HOST", "127.0.0.1")
LISTEN_PORT = int(os.environ.get("LITE_PORT", "18100"))
API_KEY = os.environ.get("LITE_API_KEY", "")
DEFAULT_MODEL = os.environ.get("LITE_MODEL", "gpt-5.4")
DEFAULT_MODELS_RAW = os.environ.get("LITE_MODELS", "")
DEFAULT_INSTRUCTIONS = os.environ.get("LITE_INSTRUCTIONS", "You are a helpful coding assistant.")
DEFAULT_REASONING_EFFORT = os.environ.get("LITE_REASONING_EFFORT", "medium").strip() or "medium"
DEFAULT_TEXT_VERBOSITY = os.environ.get("LITE_TEXT_VERBOSITY", "high").strip() or "high"
DEFAULT_MODEL_CONTEXT_WINDOW = int(os.environ.get("LITE_MODEL_CONTEXT_WINDOW", "258400") or "258400")
DEFAULT_MODEL_AUTO_COMPACT_TOKEN_LIMIT = int(
    os.environ.get("LITE_MODEL_AUTO_COMPACT_TOKEN_LIMIT", str((DEFAULT_MODEL_CONTEXT_WINDOW * 9) // 10))
    or str((DEFAULT_MODEL_CONTEXT_WINDOW * 9) // 10)
)
SESSION_STICKY_TTL = int(os.environ.get("LITE_SESSION_STICKY_TTL", "3600") or "3600")
CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
TOKEN_URL = "https://auth.openai.com/oauth/token"
UPSTREAM_URL = "https://chatgpt.com/backend-api/codex/responses"
RETRYABLE_STATUS_CODES = {401, 403, 408, 409, 429, 500, 502, 503, 504}
UPSTREAM_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "text/event-stream",
    "OpenAI-Beta": "responses=experimental",
    "originator": "codex_cli_rs",
    "user-agent": "codex-cli/0.104.0",
    "Origin": "https://chatgpt.com",
    "Referer": "https://chatgpt.com/codex",
}
UNSUPPORTED_TOP_LEVEL_FIELDS = {
    "max_output_tokens",
    "max_tokens",
    "max_completion_tokens",
    "metadata",
    "service_tier",
    "response_format",
    "parallel_tool_calls",
    "stream_options",
    "reasoning_effort",
    "user",
    "n",
}
REASONING_EFFORT_VALUES = {"minimal", "low", "medium", "high", "xhigh"}


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
        tmp = self.auth_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, self.auth_file)

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
            data["last_refresh"] = int(time.time())
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

    def _base_order_locked(self):
        accounts = list(self.accounts)
        if not accounts:
            return []
        start = self.next_index % len(accounts)
        self.next_index = (start + 1) % len(accounts)
        ordered = accounts[start:] + accounts[:start]
        now = time.time()
        active = [a for a in ordered if self.cooldowns.get(a.name, 0) <= now]
        ordered = active or ordered
        return self._prefer_account_locked(ordered, self.preferred_account_name)

    def candidates(self, session_key: str = ""):
        with self.lock:
            self._prune_sticky_sessions_locked()
            ordered = self._base_order_locked()
            if not ordered:
                return []
            session_key = session_key.strip()
            if not session_key:
                return ordered
            binding = self.sticky_sessions.get(session_key)
            if not binding:
                return ordered
            reordered = self._prefer_account_locked(ordered, binding["account_name"])
            if reordered == ordered:
                self.sticky_sessions.pop(session_key, None)
                return ordered
            return reordered

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


def normalize_payload(raw_payload):
    payload = strip_unsupported_top_level_fields(raw_payload)
    payload["model"] = str(payload.get("model") or DEFAULT_MODEL)
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
    if "tools" in raw_payload:
        payload["tools"] = normalize_chat_tools(raw_payload.get("tools"))
    tool_choice = chat_tool_choice_to_responses(raw_payload.get("tool_choice"))
    if tool_choice is not None:
        payload["tool_choice"] = tool_choice
    elif isinstance(raw_payload.get("tool_choice"), str):
        payload["tool_choice"] = raw_payload.get("tool_choice")
    return normalize_payload(payload)


def extract_session_key(headers, raw_payload):
    session_key = ""
    if headers is not None:
        session_key = str(headers.get("session_id", "")).strip()
        if not session_key:
            session_key = str(headers.get("conversation_id", "")).strip()
    if session_key:
        return session_key
    if isinstance(raw_payload, dict):
        prompt_cache_key = raw_payload.get("prompt_cache_key")
        if isinstance(prompt_cache_key, str) and prompt_cache_key.strip():
            return prompt_cache_key.strip()
    return ""


def ensure_prompt_cache_key(payload, session_key):
    if not session_key:
        return
    if not isinstance(payload.get("prompt_cache_key"), str) or not payload.get("prompt_cache_key", "").strip():
        payload["prompt_cache_key"] = session_key


def build_upstream_headers(account, session_key):
    headers = dict(UPSTREAM_HEADERS)
    headers["Authorization"] = f"Bearer {account.access_token()}"
    if session_key:
        headers["conversation_id"] = session_key
        headers["session_id"] = session_key
    return headers


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


def validate_context_budget(payload):
    estimated = estimate_request_tokens(payload)
    compact_limit = min(DEFAULT_MODEL_AUTO_COMPACT_TOKEN_LIMIT, DEFAULT_MODEL_CONTEXT_WINDOW)
    if estimated > DEFAULT_MODEL_CONTEXT_WINDOW:
        return estimated, (
            f"estimated input tokens {estimated} exceed configured model context window "
            f"{DEFAULT_MODEL_CONTEXT_WINDOW}"
        )
    if estimated > compact_limit:
        return estimated, (
            f"estimated input tokens {estimated} exceed configured auto compact guard "
            f"{compact_limit}; this proxy does not compact context automatically"
        )
    return estimated, None


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

    def _write_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _require_api_key(self):
        if not API_KEY:
            return True
        if parse_auth_header(self.headers) == API_KEY:
            return True
        self._write_json(401, {"error": {"type": "authentication_error", "message": "invalid api key"}})
        return False

    def _read_json(self):
        size = int(self.headers.get("content-length", "0") or "0")
        raw = self.rfile.read(size)
        return json.loads(raw.decode() or "{}")

    def do_GET(self):
        if self.path == "/health":
            self._write_json(
                200,
                {
                    "status": "ok",
                    "accounts": pool.names(),
                    "sticky_sessions": pool.sticky_size(),
                    "preferred_account": pool.preferred_account(),
                    "model_context_window": DEFAULT_MODEL_CONTEXT_WINDOW,
                    "model_auto_compact_token_limit": DEFAULT_MODEL_AUTO_COMPACT_TOKEN_LIMIT,
                },
            )
            return
        if self.path == "/v1/models":
            if not self._require_api_key():
                return
            self._write_json(
                200,
                {
                    "object": "list",
                    "data": [
                        {
                            "id": model,
                            "object": "model",
                            "created": 1738368000,
                            "owned_by": "openai",
                            "type": "model",
                            "display_name": model,
                        }
                        for model in ADVERTISED_MODELS
                    ],
                },
            )
            return
        self._write_json(404, {"error": {"type": "not_found", "message": "not found"}})

    def do_POST(self):
        if self.path == "/v1/chat/completions":
            if not self._require_api_key():
                return
            try:
                raw_payload = self._read_json()
                payload = build_responses_payload_from_chat(raw_payload)
                session_key = extract_session_key(self.headers, raw_payload)
                ensure_prompt_cache_key(payload, session_key)
                payload["stream"] = True
            except Exception as exc:
                self._write_json(400, {"error": {"type": "invalid_request_error", "message": str(exc)}})
                return
            estimated_tokens, budget_error = validate_context_budget(payload)
            if budget_error:
                self._write_json(
                    413,
                    {
                        "error": {
                            "type": "context_limit_error",
                            "message": budget_error,
                            "estimated_input_tokens": estimated_tokens,
                            "model_context_window": DEFAULT_MODEL_CONTEXT_WINDOW,
                            "model_auto_compact_token_limit": DEFAULT_MODEL_AUTO_COMPACT_TOKEN_LIMIT,
                        }
                    },
                )
                return
            try:
                response = self._fetch_final_response(payload, session_key)
                if raw_payload.get("stream"):
                    stream_options = raw_payload.get("stream_options") or {}
                    include_usage = bool(stream_options.get("include_usage"))
                    self._write_chat_completion_sse(response, include_usage=include_usage)
                    return
                self._write_json(200, response_to_chat_completion(response))
            except urllib.error.HTTPError as exc:
                self._forward_http_error(exc)
            except Exception as exc:
                self._write_json(502, {"error": {"type": "upstream_error", "message": str(exc)}})
            return

        if self.path != "/v1/responses":
            self._write_json(404, {"error": {"type": "not_found", "message": "not found"}})
            return
        if not self._require_api_key():
            return
        try:
            raw_payload = self._read_json()
            payload = normalize_payload(raw_payload)
            session_key = extract_session_key(self.headers, raw_payload)
            ensure_prompt_cache_key(payload, session_key)
        except Exception as exc:
            self._write_json(400, {"error": {"type": "invalid_request_error", "message": str(exc)}})
            return

        estimated_tokens, budget_error = validate_context_budget(payload)
        if budget_error:
            self._write_json(
                413,
                {
                    "error": {
                        "type": "context_limit_error",
                        "message": budget_error,
                        "estimated_input_tokens": estimated_tokens,
                        "model_context_window": DEFAULT_MODEL_CONTEXT_WINDOW,
                        "model_auto_compact_token_limit": DEFAULT_MODEL_AUTO_COMPACT_TOKEN_LIMIT,
                    }
                },
            )
            return

        wants_stream = bool(raw_payload.get("stream"))
        payload["stream"] = True

        try:
            self._forward_responses(payload, wants_stream, session_key)
        except urllib.error.HTTPError as exc:
            self._forward_http_error(exc)
        except Exception as exc:
            self._write_json(502, {"error": {"type": "upstream_error", "message": str(exc)}})

    def _upstream_once(self, payload, account, session_key, allow_refresh=True):
        body = canonical_json_bytes(payload)
        headers = build_upstream_headers(account, session_key)
        req = urllib.request.Request(UPSTREAM_URL, data=body, headers=headers, method="POST")
        try:
            return urllib.request.urlopen(req, timeout=120)
        except urllib.error.HTTPError as exc:
            if allow_refresh and exc.code in {401, 403}:
                account.refresh_access_token()
                return self._upstream_once(payload, account, session_key, allow_refresh=False)
            raise

    def _upstream(self, payload, session_key):
        accounts = pool.candidates(session_key)
        if not accounts:
            raise RuntimeError(f"no oauth json found in {AUTH_DIR}")
        last_error = None
        preferred_error = None
        for account in accounts:
            try:
                upstream = self._upstream_once(payload, account, session_key)
                pool.mark_success(account.name)
                if session_key:
                    pool.bind_session(session_key, account.name)
                return upstream
            except Exception as exc:
                last_error = exc
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
        return response

    def _forward_responses(self, payload, wants_stream, session_key):
        with self._upstream(payload, session_key) as upstream:
            if wants_stream:
                self.send_response(upstream.status)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                while True:
                    chunk = upstream.read(4096)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    self.wfile.flush()
                return

            response = extract_final_response(upstream.read().decode("utf-8", errors="replace"))
            if response is None:
                raise RuntimeError("failed to extract final response from upstream stream")
            self._write_json(200, response)

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
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def _forward_http_error(self, exc):
        body = read_http_error_body(exc).decode("utf-8", errors="replace")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"error": {"type": "upstream_error", "message": body or str(exc)}}
        self._write_json(exc.code, payload)

    def log_message(self, fmt, *args):
        return


def main():
    pool.reload()
    if pool.size() == 0:
        raise SystemExit(f"no oauth json found in {AUTH_DIR}")
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    print(f"lite api listening on http://{LISTEN_HOST}:{LISTEN_PORT} with {pool.size()} account(s)", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
