#!/usr/bin/env python3
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

    def candidates(self):
        with self.lock:
            accounts = list(self.accounts)
            if not accounts:
                return []
            start = self.next_index % len(accounts)
            self.next_index = (start + 1) % len(accounts)
            ordered = accounts[start:] + accounts[:start]
            now = time.time()
            active = [a for a in ordered if self.cooldowns.get(a.name, 0) <= now]
            return active or ordered

    def mark_failure(self, account_name: str, error):
        cooldown = 30
        if isinstance(error, urllib.error.HTTPError):
            if error.code == 429:
                cooldown = 300
            elif error.code in {401, 403}:
                cooldown = 120
            elif error.code >= 500:
                cooldown = 30
        with self.lock:
            self.cooldowns[account_name] = time.time() + cooldown


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


def normalize_payload(raw_payload):
    payload = dict(raw_payload)
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


def parse_auth_header(headers):
    value = headers.get("authorization", "")
    if value.lower().startswith("bearer "):
        return value[7:].strip()
    return headers.get("x-api-key", "").strip()


def is_retryable_error(error):
    if isinstance(error, urllib.error.HTTPError):
        return error.code in RETRYABLE_STATUS_CODES
    return isinstance(error, urllib.error.URLError)


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
        if self.path != "/v1/responses":
            self._write_json(404, {"error": {"type": "not_found", "message": "not found"}})
            return
        if not self._require_api_key():
            return
        try:
            raw_payload = self._read_json()
            payload = normalize_payload(raw_payload)
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
            self._forward(payload, wants_stream)
        except urllib.error.HTTPError as exc:
            self._forward_http_error(exc)
        except Exception as exc:
            self._write_json(502, {"error": {"type": "upstream_error", "message": str(exc)}})

    def _upstream_once(self, payload, account, allow_refresh=True):
        body = json.dumps(payload, ensure_ascii=False).encode()
        headers = dict(UPSTREAM_HEADERS)
        headers["Authorization"] = f"Bearer {account.access_token()}"
        req = urllib.request.Request(UPSTREAM_URL, data=body, headers=headers, method="POST")
        try:
            return urllib.request.urlopen(req, timeout=120)
        except urllib.error.HTTPError as exc:
            if allow_refresh and exc.code in {401, 403}:
                account.refresh_access_token()
                return self._upstream_once(payload, account, allow_refresh=False)
            raise

    def _upstream(self, payload):
        accounts = pool.candidates()
        if not accounts:
            raise RuntimeError(f"no oauth json found in {AUTH_DIR}")
        last_error = None
        for account in accounts:
            try:
                return self._upstream_once(payload, account)
            except Exception as exc:
                last_error = exc
                pool.mark_failure(account.name, exc)
                if not is_retryable_error(exc):
                    raise
        raise last_error or RuntimeError("all accounts failed")

    def _forward(self, payload, wants_stream):
        with self._upstream(payload) as upstream:
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

            sse_body = upstream.read().decode("utf-8", errors="replace")
            response = extract_final_response(sse_body)
            if response is None:
                raise RuntimeError("failed to extract final response from upstream stream")
            self._write_json(200, response)

    def _forward_http_error(self, exc):
        body = exc.read().decode("utf-8", errors="replace")
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
