import importlib.util
import io
import base64
import json
import os
import threading
import tempfile
import unittest
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from pathlib import Path


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def load_app_module(extra_env=None):
    tempdir = tempfile.TemporaryDirectory()
    extra_env = extra_env or {}
    original_env = {
        "LITE_AUTH_DIR": os.environ.get("LITE_AUTH_DIR"),
        "LITE_RUNTIME_ROOT": os.environ.get("LITE_RUNTIME_ROOT"),
        "LITE_CODEX_AUTH_PATH": os.environ.get("LITE_CODEX_AUTH_PATH"),
    }
    os.environ["LITE_AUTH_DIR"] = tempdir.name
    os.environ["LITE_RUNTIME_ROOT"] = tempdir.name
    os.environ["LITE_CODEX_AUTH_PATH"] = str(Path(tempdir.name) / ".codex" / "auth.json")
    for key, value in extra_env.items():
        original_env[key] = os.environ.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = str(value)
    try:
        module_name = f"codex2gpt_app_{uuid.uuid4().hex}"
        spec = importlib.util.spec_from_file_location(module_name, APP_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module, tempdir, original_env
    except Exception:
        tempdir.cleanup()
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        raise


def restore_auth_dir(tempdir, original_auth_dir):
    tempdir.cleanup()
    if isinstance(original_auth_dir, dict):
        for key, value in original_auth_dir.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        return
    if original_auth_dir is None:
        os.environ.pop("LITE_AUTH_DIR", None)
        return
    os.environ["LITE_AUTH_DIR"] = original_auth_dir


class FakeUpstreamResponse:
    def __init__(self, body, status=200):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self._body = body
        self._offset = 0
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self, size=-1):
        if size is None or size < 0:
            size = len(self._body) - self._offset
        chunk = self._body[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


def read_transcript_records(transcript_dir):
    files = sorted(Path(transcript_dir).rglob("*.jsonl"))
    records = []
    for path in files:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return files, records


def responses_sse_body(response):
    return f"data: {json.dumps({'type': 'response.completed', 'response': response}, ensure_ascii=False)}\n\n"


def make_test_jwt(payload):
    header = base64.urlsafe_b64encode(json.dumps({"alg": "none", "typ": "JWT"}).encode("utf-8")).decode("ascii").rstrip("=")
    body = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii").rstrip("=")
    return f"{header}.{body}."


@contextmanager
def run_test_server(module, fetch_response=None, upstream_body=None, upstream_status=200, upstream_exception=None, account_name="oauth-test"):
    original_fetch = module.Handler._fetch_final_response
    original_upstream = module.Handler._upstream
    if fetch_response is not None:
        module.Handler._fetch_final_response = lambda self, payload, session_key: fetch_response
    if upstream_body is not None or upstream_exception is not None:
        def fake_upstream(self, payload, session_key):
            self._last_attempted_account_name = account_name
            self._last_account_name = account_name
            if upstream_exception is not None:
                raise upstream_exception
            return FakeUpstreamResponse(upstream_body, status=upstream_status)

        module.Handler._upstream = fake_upstream
    server = ThreadingHTTPServer(("127.0.0.1", 0), module.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        module.Handler._fetch_final_response = original_fetch
        module.Handler._upstream = original_upstream


class Codex2GptCompatibilityTests(unittest.TestCase):
    def test_normalize_codex_auth_payload_flattens_tokens_and_timestamp(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            id_token = make_test_jwt(
                {
                    "sub": "user-123",
                    "email": "Test@Example.com",
                    "https://api.openai.com/auth": {
                        "chatgpt_account_id": "acct_123",
                        "chatgpt_plan_type": "team",
                    },
                }
            )
            normalized = app.normalize_codex_auth_payload(
                {
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                    "id_token": id_token,
                    "account_id": "acct_123",
                    "last_refresh": "2026-03-19T12:57:06.735503",
                }
            )
            self.assertEqual(normalized["auth_mode"], "chatgpt")
            self.assertEqual(normalized["tokens"]["access_token"], "access-token")
            self.assertEqual(normalized["tokens"]["refresh_token"], "refresh-token")
            self.assertEqual(normalized["tokens"]["id_token"], id_token)
            self.assertEqual(normalized["tokens"]["account_id"], "acct_123")
            self.assertEqual(normalized["email"], "Test@Example.com")
            self.assertEqual(normalized["user_id"], "user-123")
            self.assertEqual(normalized["plan_type"], "team")
            self.assertNotIn("access_token", normalized)
            self.assertNotIn("id_token", normalized)
            self.assertTrue(normalized["last_refresh"].endswith("Z"))
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_normalize_codex_auth_payload_rejects_missing_id_token(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            with self.assertRaises(RuntimeError) as ctx:
                app.normalize_codex_auth_payload({"access_token": "access-token"})
            self.assertEqual(str(ctx.exception), "auth.json missing id_token")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_normalize_payload_strips_unsupported_fields(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            payload = app.normalize_payload(
                {
                    "model": "gpt-5.4",
                    "input": "hello",
                    "max_output_tokens": 64,
                    "service_tier": "default",
                    "metadata": {"source": "test"},
                    "prompt_cache_key": "session-123",
                }
            )
            self.assertNotIn("max_output_tokens", payload)
            self.assertNotIn("service_tier", payload)
            self.assertNotIn("metadata", payload)
            self.assertEqual(payload["prompt_cache_key"], "session-123")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_chat_completion_usage_reports_cache_hits(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            completion = app.response_to_chat_completion(
                {
                    "id": "resp_cache_hit",
                    "created_at": 1773667000,
                    "model": "gpt-5.4",
                    "status": "completed",
                    "output": [
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "OK"}],
                        }
                    ],
                    "usage": {
                        "input_tokens": 120,
                        "output_tokens": 8,
                        "input_tokens_details": {"cached_tokens": 96},
                    },
                }
            )
            self.assertEqual(completion["choices"][0]["message"]["content"], "OK")
            self.assertEqual(completion["usage"]["prompt_tokens"], 120)
            self.assertEqual(completion["usage"]["completion_tokens"], 8)
            self.assertEqual(completion["usage"]["prompt_tokens_details"]["cached_tokens"], 96)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_build_responses_payload_from_chat_keeps_prompt_cache_key(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            payload = app.build_responses_payload_from_chat(
                {
                    "model": "gpt-5.4",
                    "messages": [
                        {"role": "system", "content": "你是一个助手"},
                        {"role": "user", "content": "只回复OK"},
                    ],
                    "prompt_cache_key": "cache-key-1",
                    "max_tokens": 64,
                    "metadata": {"source": "test"},
                }
            )
            self.assertEqual(payload["instructions"], "你是一个助手")
            self.assertEqual(payload["prompt_cache_key"], "cache-key-1")
            self.assertEqual(payload["input"][0]["role"], "user")
            self.assertNotIn("max_tokens", payload)
            self.assertNotIn("metadata", payload)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_chat_completion_usage_chunk_reports_cache_hits(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            chunk = app.chat_completion_usage_chunk_from_response(
                {
                    "id": "resp_cache_hit",
                    "created_at": 1773667000,
                    "model": "gpt-5.4",
                    "usage": {
                        "input_tokens": 120,
                        "output_tokens": 8,
                        "input_tokens_details": {"cached_tokens": 96},
                    },
                }
            )
            self.assertEqual(chunk["object"], "chat.completion.chunk")
            self.assertEqual(chunk["choices"], [])
            self.assertEqual(chunk["usage"]["prompt_tokens"], 120)
            self.assertEqual(chunk["usage"]["prompt_tokens_details"]["cached_tokens"], 96)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_canonical_json_bytes_match_between_chat_and_responses_payloads(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            long_text = "\n".join(
                [f"规则 {i}: 这是一个用于测试 prompt cache 的稳定长上下文，请不要改写。" for i in range(1, 6)]
            )
            responses_payload = app.normalize_payload(
                {
                    "model": "gpt-5.4",
                    "instructions": "你是一个严格遵守指令的助手。只回复 OK。",
                    "input": [
                        {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": long_text + "\n\n最终任务：只回复OK。"}],
                        }
                    ],
                    "prompt_cache_key": "cache-order-test-001",
                    "stream": False,
                }
            )
            responses_payload["stream"] = True
            chat_payload = app.build_responses_payload_from_chat(
                {
                    "model": "gpt-5.4",
                    "messages": [
                        {"role": "system", "content": "你是一个严格遵守指令的助手。只回复 OK。"},
                        {"role": "user", "content": long_text + "\n\n最终任务：只回复OK。"},
                    ],
                    "prompt_cache_key": "cache-order-test-001",
                    "stream": False,
                }
            )
            chat_payload["stream"] = True
            self.assertEqual(app.canonical_json_bytes(responses_payload), app.canonical_json_bytes(chat_payload))
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_build_responses_payload_from_anthropic_text_message(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            requested_model, payload = app.build_responses_payload_from_anthropic(
                {
                    "model": "claude-opus-4-6",
                    "max_tokens": 256,
                    "system": [{"type": "text", "text": "你是一个严格助手"}],
                    "messages": [{"role": "user", "content": "只回复 OK"}],
                    "client_id": "agent-a",
                    "business_key": "summary",
                }
            )
            self.assertEqual(requested_model, "claude-opus-4-6")
            self.assertEqual(payload["model"], "gpt-5.4")
            self.assertEqual(payload["instructions"], "你是一个严格助手")
            self.assertEqual(payload["input"][0]["role"], "user")
            self.assertEqual(payload["input"][0]["content"][0]["text"], "只回复 OK")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_build_responses_payload_from_anthropic_tools_and_tool_result(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            requested_model, payload = app.build_responses_payload_from_anthropic(
                {
                    "model": "claude-sonnet-4-6",
                    "max_tokens": 256,
                    "messages": [
                        {
                            "role": "assistant",
                            "content": [
                                {"type": "text", "text": "我来查一下"},
                                {"type": "tool_use", "id": "toolu_1", "name": "lookup", "input": {"q": "cache"}},
                            ],
                        },
                        {
                            "role": "user",
                            "content": [
                                {"type": "tool_result", "tool_use_id": "toolu_1", "content": [{"type": "text", "text": "结果"}]}
                            ],
                        },
                    ],
                    "tools": [
                        {
                            "name": "lookup",
                            "description": "query cache state",
                            "input_schema": {"type": "object", "properties": {"q": {"type": "string"}}},
                        }
                    ],
                    "tool_choice": {"type": "tool", "name": "lookup"},
                }
            )
            self.assertEqual(requested_model, "claude-sonnet-4-6")
            self.assertEqual(payload["model"], "gpt-5.3-codex")
            self.assertEqual(payload["tools"][0]["name"], "lookup")
            self.assertEqual(payload["tool_choice"], {"type": "function", "name": "lookup"})
            self.assertEqual(payload["input"][0]["content"][0]["text"], "我来查一下")
            self.assertEqual(payload["input"][1]["type"], "function_call")
            self.assertEqual(payload["input"][2]["type"], "function_call_output")
            self.assertEqual(payload["input"][2]["output"], "结果")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_build_responses_payload_from_anthropic_ignores_assistant_thinking_blocks(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            requested_model, payload = app.build_responses_payload_from_anthropic(
                {
                    "model": "claude-opus-4-6",
                    "max_tokens": 256,
                    "messages": [
                        {
                            "role": "assistant",
                            "content": [
                                {"type": "thinking", "thinking": "先分析一下", "signature": "sig_1"},
                                {"type": "redacted_thinking", "data": "opaque"},
                                {"type": "text", "text": "我来继续处理"},
                            ],
                        },
                        {"role": "user", "content": "继续"},
                    ],
                }
            )
            self.assertEqual(requested_model, "claude-opus-4-6")
            self.assertEqual(payload["input"][0]["role"], "assistant")
            self.assertEqual(payload["input"][0]["content"][0]["text"], "我来继续处理")
            self.assertEqual(payload["input"][1]["role"], "user")
            self.assertEqual(payload["input"][1]["content"][0]["text"], "继续")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_extract_quota_summary_uses_nearest_windows_from_additional_limits(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            summary = app.extract_quota_summary(
                {
                    "plan_type": "team",
                    "rate_limit": {
                        "allowed": True,
                        "limit_reached": False,
                        "primary_window": {
                            "used_percent": 3,
                            "reset_at": 111,
                            "reset_after_seconds": 120,
                            "limit_window_seconds": 300,
                        },
                        "secondary_window": {
                            "used_percent": 8,
                            "reset_at": 222,
                            "reset_after_seconds": 900,
                            "limit_window_seconds": 1200,
                        },
                    },
                    "additional_rate_limits": [
                        {
                            "rate_limit": {
                                "primary_window": {
                                    "used_percent": 61,
                                    "reset_at": 333,
                                    "reset_after_seconds": 7200,
                                    "limit_window_seconds": 18000,
                                },
                                "secondary_window": {
                                    "used_percent": 74,
                                    "reset_at": 444,
                                    "reset_after_seconds": 500000,
                                    "limit_window_seconds": 604800,
                                },
                            }
                        }
                    ],
                }
            )
            self.assertEqual(summary["used_percent"], 61)
            self.assertEqual(summary["reset_at"], 333)
            self.assertEqual(summary["limit_window_seconds"], 18000)
            self.assertEqual(summary["secondary_rate_limit"]["used_percent"], 74)
            self.assertEqual(summary["secondary_rate_limit"]["limit_window_seconds"], 604800)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_extract_session_context_supports_anthropic_business_fields(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            context = app.extract_session_context(None, {"client_id": "worker-1", "business_key": "review"}, "127.0.0.1")
            self.assertEqual(context["session_key"], "worker-1:review")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_response_to_anthropic_message_reports_cache_hits(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            message = app.response_to_anthropic_message(
                {
                    "id": "resp_1",
                    "status": "completed",
                    "output": [
                        {"type": "reasoning", "summary": []},
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "OK"}],
                        },
                        {"type": "function_call", "call_id": "toolu_1", "name": "lookup", "arguments": "{\"q\":\"cache\"}"},
                    ],
                    "usage": {
                        "input_tokens": 120,
                        "output_tokens": 8,
                        "input_tokens_details": {"cached_tokens": 96},
                    },
                },
                "claude-opus-4-6",
            )
            self.assertEqual(message["type"], "message")
            self.assertEqual(message["model"], "claude-opus-4-6")
            self.assertEqual(message["content"][0], {"type": "text", "text": "OK"})
            self.assertEqual(message["content"][1]["type"], "tool_use")
            self.assertEqual(message["usage"]["cache_read_input_tokens"], 96)
            self.assertEqual(message["stop_reason"], "tool_use")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_response_to_anthropic_message_rejects_non_object_tool_args(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            with self.assertRaises(app.ProxyError) as ctx:
                app.response_to_anthropic_message(
                    {
                        "id": "resp_1",
                        "output": [{"type": "function_call", "call_id": "toolu_1", "name": "lookup", "arguments": "[]"}],
                    },
                    "claude-opus-4-6",
                )
            self.assertEqual(ctx.exception.status, 502)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_anthropic_sse_body_has_named_events(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            body = app.anthropic_sse_body_from_message(
                {
                    "id": "msg_1",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-opus-4-6",
                    "content": [
                        {"type": "text", "text": "OK"},
                        {"type": "tool_use", "id": "toolu_1", "name": "lookup", "input": {"q": "cache"}},
                    ],
                    "stop_reason": "tool_use",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 11, "output_tokens": 7, "cache_read_input_tokens": 3},
                }
            ).decode("utf-8")
            self.assertIn("event: message_start", body)
            self.assertIn("event: content_block_start", body)
            self.assertIn("event: content_block_delta", body)
            self.assertIn("event: content_block_stop", body)
            self.assertIn("event: message_delta", body)
            self.assertIn("event: message_stop", body)
            self.assertNotIn("[DONE]", body)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_messages_count_tokens_endpoint_estimates_input_tokens(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            with run_test_server(app) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1/messages/count_tokens",
                    data=json.dumps(
                        {
                            "model": "claude-opus-4-6",
                            "max_tokens": 256,
                            "messages": [{"role": "user", "content": "hello"}],
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.load(resp)
            self.assertGreater(payload["input_tokens"], 0)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_messages_endpoint_requires_anthropic_version(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            canned = {
                "id": "resp_1",
                "status": "completed",
                "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "OK"}]}],
                "usage": {"input_tokens": 12, "output_tokens": 4},
            }
            with run_test_server(app, fetch_response=canned) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1/messages",
                    data=json.dumps(
                        {
                            "model": "claude-opus-4-6",
                            "max_tokens": 256,
                            "messages": [{"role": "user", "content": "hello"}],
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(req, timeout=30)
                payload = json.loads(ctx.exception.read().decode("utf-8"))
            self.assertEqual(ctx.exception.code, 400)
            self.assertEqual(payload["type"], "error")
            self.assertEqual(payload["error"]["type"], "invalid_request_error")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_messages_endpoint_rejects_unsupported_anthropic_model(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            with run_test_server(app) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1/messages",
                    data=json.dumps(
                        {
                            "model": "claude-haiku-4-5",
                            "max_tokens": 256,
                            "messages": [{"role": "user", "content": "hello"}],
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    urllib.request.urlopen(req, timeout=30)
                payload = json.loads(ctx.exception.read().decode("utf-8"))
            self.assertEqual(ctx.exception.code, 400)
            self.assertEqual(payload["error"]["type"], "invalid_request_error")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_messages_endpoint_streams_named_events(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            canned = {
                "id": "resp_1",
                "status": "completed",
                "output": [
                    {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "OK"}]},
                    {"type": "function_call", "call_id": "toolu_1", "name": "lookup", "arguments": "{\"q\":\"cache\"}"},
                ],
                "usage": {"input_tokens": 12, "output_tokens": 4},
            }
            with run_test_server(app, fetch_response=canned) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1/messages",
                    data=json.dumps(
                        {
                            "model": "claude-opus-4-6",
                            "max_tokens": 256,
                            "messages": [{"role": "user", "content": "hello"}],
                            "stream": True,
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    body = resp.read().decode("utf-8")
            self.assertIn("event: message_start", body)
            self.assertIn("event: message_stop", body)
            self.assertIn("text_delta", body)
            self.assertIn("input_json_delta", body)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_messages_endpoint_returns_anthropic_message(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            canned = {
                "id": "resp_1",
                "status": "completed",
                "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello"}]}],
                "usage": {"input_tokens": 12, "output_tokens": 4},
            }
            with run_test_server(app, fetch_response=canned) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1/messages",
                    data=json.dumps(
                        {
                            "model": "claude-opus-4-6",
                            "max_tokens": 256,
                            "messages": [{"role": "user", "content": "hello"}],
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.load(resp)
            self.assertEqual(payload["type"], "message")
            self.assertEqual(payload["model"], "claude-opus-4-6")
            self.assertEqual(payload["content"][0]["type"], "text")
            self.assertEqual(payload["content"][0]["text"], "hello")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_transcripts_disabled_does_not_write_files(self):
        with tempfile.TemporaryDirectory() as transcript_dir:
            app, tempdir, original_auth_dir = load_app_module(
                {"LITE_TRANSCRIPT_ENABLED": "0", "LITE_TRANSCRIPT_DIR": transcript_dir}
            )
            try:
                canned = {
                    "id": "resp_1",
                    "status": "completed",
                    "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello"}]}],
                    "usage": {"input_tokens": 12, "output_tokens": 4},
                }
                with run_test_server(app, fetch_response=canned) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/messages",
                        data=json.dumps(
                            {
                                "model": "claude-opus-4-6",
                                "max_tokens": 256,
                                "messages": [{"role": "user", "content": "hello"}],
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30):
                        pass
                files, records = read_transcript_records(transcript_dir)
                self.assertEqual(files, [])
                self.assertEqual(records, [])
            finally:
                restore_auth_dir(tempdir, original_auth_dir)

    def test_messages_endpoint_writes_transcript_record(self):
        with tempfile.TemporaryDirectory() as transcript_dir:
            app, tempdir, original_auth_dir = load_app_module(
                {"LITE_TRANSCRIPT_ENABLED": "1", "LITE_TRANSCRIPT_DIR": transcript_dir}
            )
            try:
                canned = {
                    "id": "resp_msg",
                    "status": "completed",
                    "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello"}]}],
                    "usage": {"input_tokens": 12, "output_tokens": 4},
                }
                with run_test_server(app, fetch_response=canned) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/messages",
                        data=json.dumps(
                            {
                                "model": "claude-opus-4-6",
                                "max_tokens": 256,
                                "client_id": "agent-1",
                                "business_key": "chat",
                                "messages": [{"role": "user", "content": "hello"}],
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30):
                        pass
                files, records = read_transcript_records(transcript_dir)
                self.assertEqual(len(files), 1)
                self.assertEqual(records[0]["status"], "completed")
                self.assertEqual(records[0]["path"], "/v1/messages")
                self.assertEqual(records[0]["session_key"], "agent-1:chat")
                self.assertEqual(records[0]["request"]["input_text"], "hello")
                self.assertEqual(records[0]["response"]["output_text"], "hello")
            finally:
                restore_auth_dir(tempdir, original_auth_dir)

    def test_chat_completions_endpoint_writes_transcript_record_with_tool_calls(self):
        with tempfile.TemporaryDirectory() as transcript_dir:
            app, tempdir, original_auth_dir = load_app_module(
                {"LITE_TRANSCRIPT_ENABLED": "1", "LITE_TRANSCRIPT_DIR": transcript_dir}
            )
            try:
                canned = {
                    "id": "resp_chat",
                    "status": "completed",
                    "output": [{"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{\"q\":\"cache\"}"}],
                    "usage": {"input_tokens": 12, "output_tokens": 4},
                }
                with run_test_server(app, fetch_response=canned) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/chat/completions",
                        data=json.dumps(
                            {
                                "model": "gpt-5.4",
                                "messages": [{"role": "user", "content": "hello"}],
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30):
                        pass
                _, records = read_transcript_records(transcript_dir)
                self.assertEqual(records[0]["path"], "/v1/chat/completions")
                self.assertEqual(records[0]["response"]["tool_calls"][0]["function"]["name"], "lookup")
            finally:
                restore_auth_dir(tempdir, original_auth_dir)

    def test_responses_endpoint_writes_transcript_record(self):
        with tempfile.TemporaryDirectory() as transcript_dir:
            app, tempdir, original_auth_dir = load_app_module(
                {"LITE_TRANSCRIPT_ENABLED": "1", "LITE_TRANSCRIPT_DIR": transcript_dir}
            )
            try:
                canned = {
                    "id": "resp_raw",
                    "status": "completed",
                    "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello raw"}]}],
                    "usage": {"input_tokens": 12, "output_tokens": 4},
                }
                with run_test_server(app, upstream_body=responses_sse_body(canned)) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/responses",
                        data=json.dumps({"model": "gpt-5.4", "input": "hello raw", "stream": False}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30):
                        pass
                _, records = read_transcript_records(transcript_dir)
                self.assertEqual(records[0]["path"], "/v1/responses")
                self.assertEqual(records[0]["response"]["output_text"], "hello raw")
                self.assertEqual(records[0]["account_name"], "oauth-test")
            finally:
                restore_auth_dir(tempdir, original_auth_dir)

    def test_streaming_responses_endpoint_writes_transcript_record(self):
        with tempfile.TemporaryDirectory() as transcript_dir:
            app, tempdir, original_auth_dir = load_app_module(
                {"LITE_TRANSCRIPT_ENABLED": "1", "LITE_TRANSCRIPT_DIR": transcript_dir}
            )
            try:
                canned = {
                    "id": "resp_stream",
                    "status": "completed",
                    "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello stream"}]}],
                    "usage": {"input_tokens": 12, "output_tokens": 4},
                }
                with run_test_server(app, upstream_body=responses_sse_body(canned)) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/responses",
                        data=json.dumps({"model": "gpt-5.4", "input": "hello stream", "stream": True}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        body = resp.read().decode("utf-8")
                self.assertIn("response.completed", body)
                _, records = read_transcript_records(transcript_dir)
                self.assertEqual(records[0]["response"]["output_text"], "hello stream")
            finally:
                restore_auth_dir(tempdir, original_auth_dir)

    def test_upstream_http_error_writes_transcript_record(self):
        with tempfile.TemporaryDirectory() as transcript_dir:
            app, tempdir, original_auth_dir = load_app_module(
                {"LITE_TRANSCRIPT_ENABLED": "1", "LITE_TRANSCRIPT_DIR": transcript_dir}
            )
            try:
                body = json.dumps({"error": {"type": "upstream_error", "message": "boom"}}).encode("utf-8")
                upstream_error = urllib.error.HTTPError(
                    "http://upstream.test/v1/responses",
                    429,
                    "rate limit",
                    None,
                    io.BytesIO(body),
                )
                with run_test_server(app, upstream_exception=upstream_error) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/responses",
                        data=json.dumps({"model": "gpt-5.4", "input": "hello"}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with self.assertRaises(urllib.error.HTTPError):
                        urllib.request.urlopen(req, timeout=30)
                _, records = read_transcript_records(transcript_dir)
                self.assertEqual(records[0]["status"], "upstream_http_error")
                self.assertEqual(records[0]["error"]["status_code"], 429)
                self.assertEqual(records[0]["error"]["message"], "boom")
            finally:
                restore_auth_dir(tempdir, original_auth_dir)

    def test_proxy_error_writes_transcript_record(self):
        with tempfile.TemporaryDirectory() as transcript_dir:
            app, tempdir, original_auth_dir = load_app_module(
                {"LITE_TRANSCRIPT_ENABLED": "1", "LITE_TRANSCRIPT_DIR": transcript_dir}
            )
            try:
                def raise_proxy_error(self, payload, session_key):
                    raise RuntimeError("broken upstream")

                original_fetch = app.Handler._fetch_final_response
                app.Handler._fetch_final_response = raise_proxy_error
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/messages",
                        data=json.dumps(
                            {
                                "model": "claude-opus-4-6",
                                "max_tokens": 256,
                                "messages": [{"role": "user", "content": "hello"}],
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01"},
                        method="POST",
                    )
                    with self.assertRaises(urllib.error.HTTPError):
                        urllib.request.urlopen(req, timeout=30)
                app.Handler._fetch_final_response = original_fetch
                _, records = read_transcript_records(transcript_dir)
                self.assertEqual(records[0]["status"], "proxy_error")
                self.assertEqual(records[0]["error"]["message"], "broken upstream")
            finally:
                restore_auth_dir(tempdir, original_auth_dir)

    def test_transcript_session_key_file_is_sanitized(self):
        with tempfile.TemporaryDirectory() as transcript_dir:
            app, tempdir, original_auth_dir = load_app_module(
                {"LITE_TRANSCRIPT_ENABLED": "1", "LITE_TRANSCRIPT_DIR": transcript_dir}
            )
            try:
                canned = {
                    "id": "resp_sanitized",
                    "status": "completed",
                    "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hello"}]}],
                    "usage": {"input_tokens": 12, "output_tokens": 4},
                }
                with run_test_server(app, fetch_response=canned) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/messages",
                        data=json.dumps(
                            {
                                "model": "claude-opus-4-6",
                                "max_tokens": 256,
                                "session_id": "team/A:1?bad",
                                "messages": [{"role": "user", "content": "hello"}],
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30):
                        pass
                files, _ = read_transcript_records(transcript_dir)
                self.assertEqual(len(files), 1)
                self.assertEqual(files[0].name, "team_A_1_bad.jsonl")
            finally:
                restore_auth_dir(tempdir, original_auth_dir)

    def test_health_reports_transcript_config(self):
        with tempfile.TemporaryDirectory() as transcript_dir:
            app, tempdir, original_auth_dir = load_app_module(
                {"LITE_TRANSCRIPT_ENABLED": "1", "LITE_TRANSCRIPT_DIR": transcript_dir}
            )
            try:
                with run_test_server(app) as base_url:
                    with urllib.request.urlopen(f"{base_url}/health", timeout=30) as resp:
                        payload = json.load(resp)
                self.assertTrue(payload["transcripts_enabled"])
                self.assertEqual(payload["transcript_dir"], transcript_dir)
            finally:
                restore_auth_dir(tempdir, original_auth_dir)


if __name__ == "__main__":
    unittest.main()
