import errno
import json
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from tests.test_app import load_app_module, make_test_jwt, restore_auth_dir, run_test_server


def sse_body(events):
    frames = []
    for name, payload in events:
        frames.append(f"event: {name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n")
    return "".join(frames)


class RuntimeFeatureTests(unittest.TestCase):
    def test_quota_warning_state_treats_integer_percent_as_percent_not_ratio(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            warnings = app.update_quota_warning_state("acct.json", {"used_percent": 4})
            self.assertEqual(warnings, [])
            warnings = app.update_quota_warning_state("acct.json", {"used_percent": 95})
            self.assertEqual(warnings[0]["warning_type"], "quota_high")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_chat_payload_supports_legacy_functions_and_json_schema(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            payload = app.build_responses_payload_from_chat(
                {
                    "model": "gpt-5.4",
                    "messages": [{"role": "user", "content": "Return JSON"}],
                    "functions": [
                        {
                            "name": "lookup",
                            "description": "Query data",
                            "parameters": {
                                "type": "object",
                                "properties": {"q": {"type": "string"}},
                            },
                        }
                    ],
                    "function_call": {"name": "lookup"},
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "answer",
                            "schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}},
                        },
                    },
                }
            )
            self.assertEqual(payload["tools"][0]["name"], "lookup")
            self.assertEqual(payload["tool_choice"], {"type": "function", "name": "lookup"})
            self.assertEqual(payload["text"]["format"]["type"], "json_schema")
            self.assertFalse(payload["text"]["format"]["schema"]["additionalProperties"])
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_anthropic_payload_maps_thinking_budget(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            _, payload = app.build_responses_payload_from_anthropic(
                {
                    "model": "claude-opus-4-6",
                    "max_tokens": 256,
                    "thinking": {"budget_tokens": 5000},
                    "messages": [{"role": "user", "content": "hello"}],
                }
            )
            self.assertEqual(payload["reasoning"]["effort"], "high")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_chat_completions_streams_upstream_deltas(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            upstream_body = sse_body(
                [
                    ("response.created", {"type": "response.created", "response": {"id": "resp_1"}}),
                    ("response.output_text.delta", {"type": "response.output_text.delta", "delta": "Hel"}),
                    ("response.output_text.delta", {"type": "response.output_text.delta", "delta": "lo"}),
                    (
                        "response.completed",
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "resp_1",
                                "status": "completed",
                                "output": [
                                    {
                                        "type": "message",
                                        "role": "assistant",
                                        "content": [{"type": "output_text", "text": "Hello"}],
                                    }
                                ],
                                "usage": {"input_tokens": 4, "output_tokens": 1},
                            },
                        },
                    ),
                ]
            )
            with run_test_server(app, upstream_body=upstream_body) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1/chat/completions",
                    data=json.dumps(
                        {
                            "model": "gpt-5.4",
                            "stream": True,
                            "stream_options": {"include_usage": True},
                            "messages": [{"role": "user", "content": "hello"}],
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    body = resp.read().decode("utf-8")
            self.assertIn('"content": "Hel"', body)
            self.assertIn('"content": "lo"', body)
            self.assertIn('"usage"', body)
            self.assertIn("[DONE]", body)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_messages_streams_reasoning_and_tool_events_from_upstream(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            upstream_body = sse_body(
                [
                    ("response.created", {"type": "response.created", "response": {"id": "resp_1"}}),
                    ("response.reasoning_summary_text.delta", {"type": "response.reasoning_summary_text.delta", "delta": "Thinking"}),
                    (
                        "response.output_item.added",
                        {
                            "type": "response.output_item.added",
                            "output_index": 0,
                            "item": {"type": "function_call", "call_id": "toolu_1", "name": "lookup"},
                        },
                    ),
                    (
                        "response.function_call_arguments.delta",
                        {"type": "response.function_call_arguments.delta", "call_id": "toolu_1", "delta": "{\"q\":\"cache\"}"},
                    ),
                    (
                        "response.completed",
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "resp_1",
                                "status": "completed",
                                "output": [{"type": "function_call", "call_id": "toolu_1", "name": "lookup", "arguments": "{\"q\":\"cache\"}"}],
                                "usage": {"input_tokens": 10, "output_tokens": 3},
                            },
                        },
                    ),
                ]
            )
            with run_test_server(app, upstream_body=upstream_body) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1/messages",
                    data=json.dumps(
                        {
                            "model": "claude-opus-4-6",
                            "max_tokens": 64,
                            "stream": True,
                            "thinking": {"enabled": True, "budget_tokens": 5000},
                            "messages": [{"role": "user", "content": "hello"}],
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    body = resp.read().decode("utf-8")
            self.assertIn("thinking_delta", body)
            self.assertIn("input_json_delta", body)
            self.assertIn("message_stop", body)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_messages_stream_uses_buffered_sse_for_bearer_clients(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            upstream_body = sse_body(
                [
                    (
                        "response.completed",
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "resp_buffered",
                                "status": "completed",
                                "output": [
                                    {
                                        "type": "message",
                                        "role": "assistant",
                                        "content": [{"type": "output_text", "text": "CLAUDE-COMPAT"}],
                                    }
                                ],
                                "usage": {
                                    "input_tokens": 12,
                                    "output_tokens": 3,
                                    "input_tokens_details": {"cached_tokens": 4},
                                },
                            },
                        },
                    )
                ]
            )
            with run_test_server(app, upstream_body=upstream_body, account_name="acct-buffered.json") as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1/messages",
                    data=json.dumps(
                        {
                            "model": "claude-opus-4-6",
                            "max_tokens": 64,
                            "stream": True,
                            "messages": [{"role": "user", "content": "hello"}],
                        }
                    ).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        "anthropic-version": "2023-06-01",
                        "Authorization": "Bearer test-oauth-token",
                    },
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    body = resp.read().decode("utf-8")
                    content_length = resp.headers.get("Content-Length")
            self.assertIn("message_start", body)
            self.assertIn("CLAUDE-COMPAT", body)
            self.assertIn("message_stop", body)
            self.assertTrue(content_length)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_candidates_skip_rate_limited_accounts(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            (Path(tempdir.name) / "acct-a.json").write_text(
                json.dumps({"tokens": {"access_token": "tok-a", "refresh_token": "ref-a"}}),
                encoding="utf-8",
            )
            (Path(tempdir.name) / "acct-b.json").write_text(
                json.dumps({"tokens": {"access_token": "tok-b", "refresh_token": "ref-b"}}),
                encoding="utf-8",
            )
            app.pool.reload()
            app.STATE_DB.upsert_account("acct-a.json", auth_file="acct-a.json", status="rate_limited", plan_type="team")
            app.STATE_DB.upsert_account("acct-b.json", auth_file="acct-b.json", status="active", plan_type="team")
            candidates = [account.name for account in app.pool.candidates(model_name="gpt-5.4")]
            self.assertEqual(candidates, ["acct-b.json"])
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_responses_endpoint_sets_account_and_cache_headers_and_recent_requests(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            upstream_body = sse_body(
                [
                    (
                        "response.completed",
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "resp_cache",
                                "status": "completed",
                                "model": "gpt-5.4",
                                "output": [
                                    {
                                        "type": "message",
                                        "role": "assistant",
                                        "content": [{"type": "output_text", "text": "cached"}],
                                    }
                                ],
                                "usage": {
                                    "input_tokens": 100,
                                    "output_tokens": 12,
                                    "input_tokens_details": {"cached_tokens": 40},
                                },
                            },
                        },
                    )
                ]
            )
            with run_test_server(app, upstream_body=upstream_body, account_name="acct-a.json") as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1/responses",
                    data=json.dumps({"model": "gpt-5.4", "input": "hello"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.load(resp)
                    account_header = resp.headers.get("X-Codex2gpt-Account")
                    cached_tokens_header = resp.headers.get("X-Codex2gpt-Cached-Tokens")
                    cache_rate_header = resp.headers.get("X-Codex2gpt-Cache-Hit-Rate")
                with urllib.request.urlopen(f"{base_url}/admin/recent-requests", timeout=30) as resp:
                    recent = json.load(resp)
            self.assertEqual(payload["id"], "resp_cache")
            self.assertEqual(account_header, "acct-a.json")
            self.assertEqual(cached_tokens_header, "40")
            self.assertEqual(cache_rate_header, "40.00")
            self.assertEqual(recent["data"][0]["account_name"], "acct-a.json")
            self.assertEqual(recent["data"][0]["cache_hit_rate"], 40.0)
            self.assertEqual(recent["data"][0]["cached_tokens"], 40)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_response_metric_headers_keep_zero_cached_tokens(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            handler = app.Handler.__new__(app.Handler)
            handler._current_account_name = lambda: "acct-zero.json"
            headers = handler._response_metric_headers(
                {
                    "input_tokens": 49,
                    "output_tokens": 5,
                    "input_tokens_details": {"cached_tokens": 0},
                }
            )
            self.assertEqual(headers["X-Codex2gpt-Account"], "acct-zero.json")
            self.assertEqual(headers["X-Codex2gpt-Prompt-Tokens"], "49")
            self.assertEqual(headers["X-Codex2gpt-Cached-Tokens"], "0")
            self.assertEqual(headers["X-Codex2gpt-Cache-Hit-Rate"], "0.00")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_gemini_generate_content_endpoint(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            upstream_body = sse_body(
                [
                    (
                        "response.completed",
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "resp_gemini",
                                "status": "completed",
                                "output": [
                                    {
                                        "type": "message",
                                        "role": "assistant",
                                        "content": [{"type": "output_text", "text": "Hello Gemini"}],
                                    }
                                ],
                                "usage": {"input_tokens": 6, "output_tokens": 2},
                            },
                        },
                    )
                ]
            )
            with run_test_server(app, upstream_body=upstream_body) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1beta/models/gemini-2.5-pro:generateContent",
                    data=json.dumps(
                        {
                            "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.load(resp)
            self.assertEqual(payload["candidates"][0]["content"]["parts"][0]["text"], "Hello Gemini")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_gemini_stream_generate_content_endpoint(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            upstream_body = sse_body(
                [
                    ("response.output_text.delta", {"type": "response.output_text.delta", "delta": "Hi"}),
                    (
                        "response.completed",
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "resp_gemini",
                                "status": "completed",
                                "output": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "Hi"}]}],
                                "usage": {"input_tokens": 4, "output_tokens": 1},
                            },
                        },
                    ),
                ]
            )
            with run_test_server(app, upstream_body=upstream_body) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/v1beta/models/gemini-2.5-pro:streamGenerateContent",
                    data=json.dumps({"contents": [{"role": "user", "parts": [{"text": "hello"}]}]}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    body = resp.read().decode("utf-8")
            self.assertIn('"text": "Hi"', body)
            self.assertIn('"usageMetadata"', body)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_admin_rotation_settings_and_proxy_crud(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            with run_test_server(app) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/admin/rotation-settings",
                    data=json.dumps(
                        {
                            "rotation_mode": "round_robin",
                            "responses_transport": "http",
                            "plans": {"gpt-5.4": ["plus"]},
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    updated = json.load(resp)
                self.assertEqual(updated["rotation_mode"], "round_robin")
                self.assertEqual(updated["responses_transport"], "http")

                req = urllib.request.Request(
                    f"{base_url}/api/proxies",
                    data=json.dumps({"proxy_id": "proxy-a", "name": "Proxy A", "url": "http://127.0.0.1:8080"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30):
                    pass

                with urllib.request.urlopen(f"{base_url}/api/proxies", timeout=30) as resp:
                    proxies = json.load(resp)
            self.assertEqual(proxies["data"][0]["proxy_id"], "proxy-a")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_dashboard_root_serves_static_page(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            with run_test_server(app) as base_url:
                with urllib.request.urlopen(f"{base_url}/", timeout=30) as resp:
                    body = resp.read().decode("utf-8")
            self.assertIn("Codex2gpt 控制台", body)
            self.assertIn("轮换模式", body)
            self.assertIn("运行测试", body)
            self.assertIn("代理设置", body)
            self.assertIn("用量统计", body)
            self.assertIn("API 接入", body)
            self.assertIn("添加账号", body)
            self.assertIn("Codex App", body)
            self.assertIn("最近请求", body)
            self.assertIn("language-select", body)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_auth_status_reports_transport_warning_and_counts(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            app.STATE_DB.set_quota_warnings(
                "acct.json",
                [{"level": "warning", "warning_type": "quota_high", "message": "high"}],
            )
            with run_test_server(app) as base_url:
                with urllib.request.urlopen(f"{base_url}/auth/status", timeout=30) as resp:
                    payload = json.load(resp)
            self.assertIn(payload["responses_transport"], {"auto", "http", "websocket"})
            self.assertIn("websocket_transport_available", payload)
            self.assertEqual(payload["warnings"]["total"], 1)
            self.assertIn("account_statuses", payload)
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_pkce_callback_exchanges_code_and_persists_account(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            def fake_exchange(code, verifier, redirect_uri):
                self.assertEqual(code, "auth-code")
                self.assertTrue(verifier)
                return {
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                    "id_token": make_test_jwt(
                        {
                            "email": "test@example.com",
                            "sub": "user_123",
                            "https://api.openai.com/auth": {"chatgpt_account_id": "acct_123"},
                        }
                    ),
                }

            original_exchange = app.exchange_pkce_code
            app.exchange_pkce_code = fake_exchange
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/auth/login-start",
                        data=b"{}",
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        started = json.load(resp)
                    with urllib.request.urlopen(
                        f"{base_url}/auth/callback?state={started['state']}&code=auth-code",
                        timeout=30,
                    ) as resp:
                        body = resp.read().decode("utf-8")
                self.assertIn("OAuth login complete", body)
                saved = Path(tempdir.name) / "test_example.com.json"
                self.assertTrue(saved.exists())
                payload = json.loads(saved.read_text(encoding="utf-8"))
                self.assertEqual(payload["auth_mode"], "chatgpt")
                self.assertEqual(payload["tokens"]["account_id"], "acct_123")
                self.assertTrue(payload["last_refresh"].endswith("Z"))
            finally:
                app.exchange_pkce_code = original_exchange
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_import_normalizes_legacy_account_and_codex_app_select_writes_auth_file(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            id_token = make_test_jwt(
                {
                    "email": "imported@example.com",
                    "sub": "user-imported",
                    "https://api.openai.com/auth": {
                        "chatgpt_account_id": "acct_imported",
                        "chatgpt_plan_type": "team",
                    },
                }
            )
            with run_test_server(app) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/auth/accounts/import",
                    data=json.dumps(
                        {
                            "accounts": [
                                {
                                    "filename": "legacy-account.json",
                                    "access_token": "access-token",
                                    "refresh_token": "refresh-token",
                                    "id_token": id_token,
                                    "account_id": "acct_imported",
                                    "last_refresh": "2026-03-19T12:57:06.735503",
                                }
                            ]
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    imported = json.load(resp)
                self.assertEqual(imported["count"], 1)

                legacy_path = Path(tempdir.name) / "legacy-account.json"
                stored = json.loads(legacy_path.read_text(encoding="utf-8"))
                self.assertEqual(stored["auth_mode"], "chatgpt")
                self.assertEqual(stored["tokens"]["access_token"], "access-token")
                self.assertEqual(stored["tokens"]["account_id"], "acct_imported")
                self.assertTrue(stored["last_refresh"].endswith("Z"))

                req = urllib.request.Request(
                    f"{base_url}/auth/codex-app/select",
                    data=json.dumps({"entry_id": "legacy-account.json"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    selected = json.load(resp)
                self.assertTrue(selected["ok"])

                with urllib.request.urlopen(f"{base_url}/auth/accounts", timeout=30) as resp:
                    accounts = json.load(resp)
            codex_auth_path = Path(tempdir.name) / ".codex" / "auth.json"
            written = json.loads(codex_auth_path.read_text(encoding="utf-8"))
            self.assertEqual(written["auth_mode"], "chatgpt")
            self.assertEqual(written["tokens"]["account_id"], "acct_imported")
            self.assertEqual(accounts["codex_app"]["current_entry_id"], "legacy-account.json")
            self.assertTrue(accounts["data"][0]["is_codex_app_current"])
            self.assertTrue(accounts["data"][0]["is_codex_app_reserved"])
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_candidates_deprioritize_current_codex_app_account_until_last_fallback(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            token_a = make_test_jwt(
                {
                    "email": "a@example.com",
                    "sub": "user-a",
                    "https://api.openai.com/auth": {"chatgpt_account_id": "acct_a"},
                }
            )
            token_b = make_test_jwt(
                {
                    "email": "b@example.com",
                    "sub": "user-b",
                    "https://api.openai.com/auth": {"chatgpt_account_id": "acct_b"},
                }
            )
            app.write_codex_auth_file(
                str(Path(tempdir.name) / "acct-a.json"),
                {"access_token": "tok-a", "refresh_token": "ref-a", "id_token": token_a, "account_id": "acct_a"},
            )
            app.write_codex_auth_file(
                str(Path(tempdir.name) / "acct-b.json"),
                {"access_token": "tok-b", "refresh_token": "ref-b", "id_token": token_b, "account_id": "acct_b"},
            )
            app.write_codex_auth_file(
                app.CODEX_AUTH_PATH,
                {"access_token": "tok-a", "refresh_token": "ref-a", "id_token": token_a, "account_id": "acct_a"},
            )
            app.pool.reload()
            app.sync_accounts_with_state()
            app.STATE_DB.upsert_account("acct-a.json", auth_file="acct-a.json", status="active", plan_type="team")
            app.STATE_DB.upsert_account("acct-b.json", auth_file="acct-b.json", status="active", plan_type="team")

            candidates = [account.name for account in app.pool.candidates(model_name="gpt-5.4")]
            self.assertEqual(candidates, ["acct-b.json", "acct-a.json"])

            app.set_account_status("acct-b.json", "rate_limited")
            fallback_candidates = [account.name for account in app.pool.candidates(model_name="gpt-5.4")]
            self.assertEqual(fallback_candidates, ["acct-a.json"])
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_login_start_returns_fixed_localhost_callback_and_server_status(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            original_start = app.start_oauth_callback_server
            app.start_oauth_callback_server = lambda: {
                "ok": True,
                "error": "",
                "redirect_uri": "http://localhost:1455/auth/callback",
            }
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/auth/login-start",
                        data=b"{}",
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        payload = json.load(resp)
                self.assertEqual(payload["redirect_uri"], "http://localhost:1455/auth/callback")
                self.assertTrue(payload["callback_server"]["ok"])
                self.assertIn("redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback", payload["authorize_url"])
            finally:
                app.start_oauth_callback_server = original_start
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_auth_code_relay_exchanges_code_and_persists_account(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            app.RUNTIME_SETTINGS["oauth_pkce"] = {
                "state": "relay-state",
                "verifier": "relay-verifier",
                "redirect_uri": "http://localhost:1455/auth/callback",
                "created_at": app.now_iso(),
            }
            app.save_runtime_settings(app.RUNTIME_SETTINGS)

            def fake_exchange(code, verifier, redirect_uri):
                self.assertEqual(code, "relay-code")
                self.assertEqual(verifier, "relay-verifier")
                self.assertEqual(redirect_uri, "http://localhost:1455/auth/callback")
                return {
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                    "id_token": (
                        "header."
                        + "eyJlbWFpbCI6InJlbGF5QGV4YW1wbGUuY29tIiwic3ViIjoidXNlcl9yZWxheSJ9"
                        + ".sig"
                    ),
                }

            original_exchange = app.exchange_pkce_code
            app.exchange_pkce_code = fake_exchange
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/auth/code-relay",
                        data=json.dumps(
                            {
                                "callbackUrl": "http://localhost:1455/auth/callback?code=relay-code&state=relay-state"
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        payload = json.load(resp)
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["filename"], "relay_example.com.json")
                self.assertTrue((Path(tempdir.name) / "relay_example.com.json").exists())
            finally:
                app.exchange_pkce_code = original_exchange
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_proxy_health_check_endpoint_updates_state(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            class FakeJsonResponse:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def read(self, size=-1):
                    return json.dumps({"ip": "1.2.3.4"}).encode("utf-8")

            original_urlopen = app.urlopen_with_optional_proxy
            app.urlopen_with_optional_proxy = lambda request, proxy_url=None, timeout=120: FakeJsonResponse()
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/api/proxies",
                        data=json.dumps({"proxy_id": "proxy-a", "name": "Proxy A", "url": "http://127.0.0.1:8080"}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30):
                        pass
                    req = urllib.request.Request(
                        f"{base_url}/api/proxies/health-check",
                        data=b"{}",
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        payload = json.load(resp)
                self.assertEqual(payload["data"][0]["health"]["exit_ip"], "1.2.3.4")
                self.assertEqual(payload["data"][0]["status"], "active")
            finally:
                app.urlopen_with_optional_proxy = original_urlopen
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_quota_refresh_updates_accounts_and_warnings(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            account_file = Path(tempdir.name) / "acct.json"
            account_file.write_text(
                json.dumps({"tokens": {"access_token": "tok", "refresh_token": "ref"}}),
                encoding="utf-8",
            )
            app.pool.reload()
            app.sync_accounts_with_state()

            original_fetch_quota = app.fetch_account_quota
            app.fetch_account_quota = lambda account: {
                "plan_type": "plus",
                "rate_limit": {
                    "allowed": True,
                    "limit_reached": False,
                    "primary_window": {"used_percent": 0.96, "reset_at": "tomorrow", "limit_window_seconds": 300},
                },
            }
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/admin/quota-refresh",
                        data=b"{}",
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        refreshed = json.load(resp)
                    with urllib.request.urlopen(f"{base_url}/auth/accounts", timeout=30) as resp:
                        accounts = json.load(resp)
                    with urllib.request.urlopen(f"{base_url}/auth/quota/warnings", timeout=30) as resp:
                        warnings = json.load(resp)
                self.assertEqual(refreshed["results"][0]["entry_id"], "acct.json")
                self.assertEqual(accounts["data"][0]["plan_type"], "plus")
                self.assertEqual(warnings["data"][0]["warning"]["warning_type"], "quota_high")
            finally:
                app.fetch_account_quota = original_fetch_quota
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_fetch_account_quota_falls_back_when_curl_returns_empty_body(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            account_file = Path(tempdir.name) / "acct.json"
            account_file.write_text(
                json.dumps({"tokens": {"access_token": "tok", "refresh_token": "ref"}}),
                encoding="utf-8",
            )
            app.pool.reload()
            account = app.pool.accounts[0]

            class EmptyCurlResponse:
                status = 200
                headers = {}

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def read(self, size=-1):
                    return b""

                def close(self):
                    return None

            class DirectJsonResponse:
                status = 200
                headers = {}

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def read(self, size=-1):
                    return json.dumps(
                        {
                            "plan_type": "team",
                            "rate_limit": {
                                "allowed": True,
                                "limit_reached": False,
                                "primary_window": {"used_percent": 72, "reset_at": 123, "limit_window_seconds": 18000},
                                "secondary_window": {"used_percent": 77, "reset_at": 456, "limit_window_seconds": 604800},
                            },
                        }
                    ).encode("utf-8")

            original_backend = app.current_transport_backend
            original_urlopen = app.urlopen_with_optional_proxy
            original_direct = app.urlopen_direct_with_optional_proxy
            app.current_transport_backend = lambda: "curl_impersonate"
            app.urlopen_with_optional_proxy = lambda request, proxy_url=None, timeout=30: EmptyCurlResponse()
            app.urlopen_direct_with_optional_proxy = lambda request, proxy_url=None, timeout=30: DirectJsonResponse()
            try:
                payload = app.fetch_account_quota(account)
                self.assertEqual(payload["plan_type"], "team")
                self.assertEqual(payload["rate_limit"]["primary_window"]["used_percent"], 72)
            finally:
                app.current_transport_backend = original_backend
                app.urlopen_with_optional_proxy = original_urlopen
                app.urlopen_direct_with_optional_proxy = original_direct
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_relay_provider_crud_endpoints(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            with run_test_server(app) as base_url:
                req = urllib.request.Request(
                    f"{base_url}/api/relay-providers",
                    data=json.dumps(
                        {
                            "provider_id": "relay-a",
                            "name": "Relay A",
                            "base_url": "https://relay.example.com",
                            "api_key": "relay-key",
                            "format": "responses",
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=30):
                    pass
                with urllib.request.urlopen(f"{base_url}/api/relay-providers", timeout=30) as resp:
                    providers = json.load(resp)
                req = urllib.request.Request(f"{base_url}/api/relay-providers/relay-a", method="DELETE")
                with urllib.request.urlopen(req, timeout=30) as resp:
                    deleted = json.load(resp)
            self.assertEqual(providers["data"][0]["provider_id"], "relay-a")
            self.assertTrue(deleted["deleted"])
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_responses_endpoint_can_use_openai_chat_relay_provider(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            app.STATE_DB.upsert_relay_provider(
                "relay-openai",
                base_url="https://relay.example.com",
                api_key="relay-key",
                format="openai_chat",
                enabled=True,
                name="Relay OpenAI",
            )
            captured = {}

            class FakeRelayResponse:
                status = 200
                headers = {}

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def read(self, size=-1):
                    return (
                        b'data: {"id":"chatcmpl_1","model":"gpt-5.4","choices":[{"delta":{"content":"Hi"}}]}\n\n'
                        b'data: {"id":"chatcmpl_1","model":"gpt-5.4","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":1}}\n\n'
                        b"data: [DONE]\n\n"
                    )

            original_urlopen = app.urlopen_with_optional_proxy

            def fake_urlopen(request, proxy_url=None, timeout=120):
                captured["url"] = request.full_url
                captured["body"] = json.loads(request.data.decode("utf-8"))
                return FakeRelayResponse()

            app.urlopen_with_optional_proxy = fake_urlopen
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/responses",
                        data=json.dumps({"model": "gpt-5.4", "input": "hello", "stream": True}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        body = resp.read().decode("utf-8")
                self.assertEqual(captured["url"], "https://relay.example.com/v1/chat/completions")
                self.assertEqual(captured["body"]["messages"][1]["content"], "hello")
                self.assertIn("response.output_text.delta", body)
                self.assertIn("response.completed", body)
            finally:
                app.urlopen_with_optional_proxy = original_urlopen
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_runtime_status_reports_transport_and_jobs(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            with run_test_server(app) as base_url:
                with urllib.request.urlopen(f"{base_url}/admin/runtime-status", timeout=30) as resp:
                    payload = json.load(resp)
            self.assertIn(payload["transport_backend"], {"direct", "curl_impersonate"})
            self.assertIn(payload["responses_transport"], {"auto", "http", "websocket"})
            self.assertIn("websocket_transport_available", payload)
            self.assertIn("jobs", payload["background"])
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_runtime_job_endpoint_runs_fingerprint_refresh(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            original_refresh = app.refresh_fingerprint_cache
            app.refresh_fingerprint_cache = lambda force=False: {"app_version": "9.9.9", "transport_backend": "direct"}
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/admin/runtime-jobs/run",
                        data=json.dumps({"job": "fingerprint_refresh"}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        payload = json.load(resp)
                self.assertEqual(payload["app_version"], "9.9.9")
            finally:
                app.refresh_fingerprint_cache = original_refresh
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_urlopen_uses_curl_transport_when_selected(self):
        app, tempdir, original_auth_dir = load_app_module({"LITE_TRANSPORT_BACKEND": "curl_impersonate"})
        try:
            called = {}

            class FakeCurlResponse:
                status = 200
                headers = {}

                def read(self, size=-1):
                    return b"ok"

                def close(self):
                    return None

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            original_find = app.find_curl_impersonate_binary
            original_curl = app.curl_impersonate_request
            app.find_curl_impersonate_binary = lambda: "/usr/bin/curl"

            def fake_curl(url, **kwargs):
                called["url"] = url
                called["kwargs"] = kwargs
                return FakeCurlResponse()

            app.curl_impersonate_request = fake_curl
            try:
                request = urllib.request.Request("https://example.com", headers={"Accept": "application/json"}, method="GET")
                with app.urlopen_with_optional_proxy(request, proxy_url="http://127.0.0.1:8080", timeout=5) as response:
                    body = response.read().decode("utf-8")
                self.assertEqual(body, "ok")
                self.assertEqual(called["url"], "https://example.com")
                self.assertEqual(called["kwargs"]["proxy_url"], "http://127.0.0.1:8080")
            finally:
                app.find_curl_impersonate_binary = original_find
                app.curl_impersonate_request = original_curl
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_cookie_capture_prunes_expired_entries_and_keeps_live_values(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            class FakeHeaders:
                def get_all(self, name):
                    if name.lower() == "set-cookie":
                        return [
                            "cf_clearance=live-cookie; Path=/; Max-Age=3600; HttpOnly; Secure",
                            "session=; Max-Age=0; Path=/",
                        ]
                    return []

            app.COOKIE_STORE["acct.json"] = {
                "expired": {"value": "stale", "expires_at": "2000-01-01T00:00:00+00:00"},
                "session": {"value": "remove-me"},
            }
            app.capture_set_cookie_headers("acct.json", FakeHeaders())
            header = app.account_cookie_header("acct.json")
            self.assertEqual(header, "cf_clearance=live-cookie")
            self.assertNotIn("expired", app.COOKIE_STORE["acct.json"])
            self.assertNotIn("session", app.COOKIE_STORE["acct.json"])
            self.assertEqual(app.COOKIE_STORE["acct.json"]["cf_clearance"]["value"], "live-cookie")
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_session_reuses_previous_response_id_and_websocket_beta_header(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            account_file = Path(tempdir.name) / "acct.json"
            account_file.write_text(json.dumps({"tokens": {"access_token": "tok", "refresh_token": "ref"}}), encoding="utf-8")
            app.pool.reload()
            app.sync_accounts_with_state()
            captured = []

            class FakeResponse:
                status = 200
                headers = {}

                def __init__(self, body):
                    self._body = body
                    self._offset = 0

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

            responses = [
                (
                    'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"one"}]}],"usage":{"input_tokens":1,"output_tokens":1}}}\n\n'
                ).encode("utf-8"),
                (
                    'data: {"type":"response.completed","response":{"id":"resp_2","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"two"}]}],"usage":{"input_tokens":1,"output_tokens":1}}}\n\n'
                ).encode("utf-8"),
            ]
            original_urlopen = app.urlopen_with_optional_proxy
            original_ws_connect = app.websocket_sync_connect

            def fake_urlopen(request, proxy_url=None, timeout=120):
                captured.append(
                    {
                        "url": request.full_url,
                        "headers": dict(request.header_items()),
                        "body": json.loads(request.data.decode("utf-8")),
                        "proxy_url": proxy_url,
                    }
                )
                return FakeResponse(responses.pop(0))

            app.urlopen_with_optional_proxy = fake_urlopen
            app.websocket_sync_connect = lambda: (_ for _ in ()).throw(ImportError("disabled for http transport test"))
            try:
                with run_test_server(app) as base_url:
                    for text in ("hello", "again"):
                        req = urllib.request.Request(
                            f"{base_url}/v1/responses",
                            data=json.dumps({"model": "gpt-5.4", "input": text, "client_id": "client-1"}).encode("utf-8"),
                            headers={"Content-Type": "application/json"},
                            method="POST",
                        )
                        with urllib.request.urlopen(req, timeout=30) as resp:
                            payload = json.load(resp)
                        self.assertIn(payload["id"], {"resp_1", "resp_2"})
                first_headers = {key.lower(): value for key, value in captured[0]["headers"].items()}
                self.assertEqual(first_headers["openai-beta"], "responses_websockets=2026-02-06")
                self.assertNotIn("previous_response_id", captured[0]["body"])
                self.assertEqual(captured[1]["body"]["previous_response_id"], "resp_1")
            finally:
                app.websocket_sync_connect = original_ws_connect
                app.urlopen_with_optional_proxy = original_urlopen
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_upstream_retries_direct_when_proxy_fails(self):
        app, tempdir, original_auth_dir = load_app_module({"LITE_GLOBAL_PROXY_URL": "http://127.0.0.1:8080"})
        try:
            account_file = Path(tempdir.name) / "acct.json"
            account_file.write_text(json.dumps({"tokens": {"access_token": "tok", "refresh_token": "ref"}}), encoding="utf-8")
            app.pool.reload()
            app.sync_accounts_with_state()
            calls = []

            class FakeResponse:
                status = 200
                headers = {}

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def read(self, size=-1):
                    return (
                        'data: {"type":"response.completed","response":{"id":"resp_proxy","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1}}}\n\n'
                    ).encode("utf-8")

            original_urlopen = app.urlopen_with_optional_proxy
            original_ws_connect = app.websocket_sync_connect

            def fake_urlopen(request, proxy_url=None, timeout=120):
                calls.append(proxy_url)
                if proxy_url:
                    raise urllib.error.HTTPError(request.full_url, 502, "bad gateway", {}, None)
                return FakeResponse()

            app.urlopen_with_optional_proxy = fake_urlopen
            app.websocket_sync_connect = lambda: (_ for _ in ()).throw(ImportError("disabled for proxy fallback test"))
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/responses",
                        data=json.dumps({"model": "gpt-5.4", "input": "hello"}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        payload = json.load(resp)
                self.assertEqual(payload["id"], "resp_proxy")
                self.assertEqual(calls, ["http://127.0.0.1:8080", None])
            finally:
                app.websocket_sync_connect = original_ws_connect
                app.urlopen_with_optional_proxy = original_urlopen
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_upstream_request_falls_back_to_direct_when_curl_stream_is_empty(self):
        app, tempdir, original_auth_dir = load_app_module({"LITE_TRANSPORT_BACKEND": "curl_impersonate"})
        try:
            class EmptyCurlResponse:
                status = 200
                headers = {}

                def read(self, size=-1):
                    return b""

                def close(self):
                    return None

            class DirectResponse:
                status = 200
                headers = {}

                def __init__(self):
                    self._body = b"data: test\n\n"
                    self._offset = 0

                def read(self, size=-1):
                    if size is None or size < 0:
                        size = len(self._body) - self._offset
                    chunk = self._body[self._offset : self._offset + size]
                    self._offset += len(chunk)
                    return chunk

                def close(self):
                    return None

            original_backend = app.current_transport_backend
            original_urlopen = app.urlopen_with_optional_proxy
            original_direct = app.urlopen_direct_with_optional_proxy
            app.current_transport_backend = lambda: "curl_impersonate"
            app.urlopen_with_optional_proxy = lambda request, proxy_url=None, timeout=120: EmptyCurlResponse()
            app.urlopen_direct_with_optional_proxy = lambda request, proxy_url=None, timeout=120: DirectResponse()
            try:
                request = urllib.request.Request("https://example.com/stream", method="GET")
                response = app.upstream_request_with_transport_fallback(request, account_name="acct.json")
                self.assertEqual(response.read().decode("utf-8"), "data: test\n\n")
            finally:
                app.current_transport_backend = original_backend
                app.urlopen_with_optional_proxy = original_urlopen
                app.urlopen_direct_with_optional_proxy = original_direct
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_upstream_request_falls_back_to_direct_when_curl_write_breaks_pipe(self):
        app, tempdir, original_auth_dir = load_app_module({"LITE_TRANSPORT_BACKEND": "curl_impersonate"})
        try:
            class DirectResponse:
                status = 200
                headers = {}

                def __init__(self):
                    self._body = b"data: test\n\n"
                    self._offset = 0

                def read(self, size=-1):
                    if size is None or size < 0:
                        size = len(self._body) - self._offset
                    chunk = self._body[self._offset : self._offset + size]
                    self._offset += len(chunk)
                    return chunk

                def close(self):
                    return None

            original_backend = app.current_transport_backend
            original_urlopen = app.urlopen_with_optional_proxy
            original_direct = app.urlopen_direct_with_optional_proxy
            app.current_transport_backend = lambda: "curl_impersonate"

            def fake_urlopen(request, proxy_url=None, timeout=120):
                raise BrokenPipeError(errno.EPIPE, "Broken pipe")

            app.urlopen_with_optional_proxy = fake_urlopen
            app.urlopen_direct_with_optional_proxy = lambda request, proxy_url=None, timeout=120: DirectResponse()
            try:
                request = urllib.request.Request("https://example.com/stream", method="POST", data=b"hello")
                response = app.upstream_request_with_transport_fallback(request, account_name="acct.json")
                self.assertEqual(response.read().decode("utf-8"), "data: test\n\n")
            finally:
                app.current_transport_backend = original_backend
                app.urlopen_with_optional_proxy = original_urlopen
                app.urlopen_direct_with_optional_proxy = original_direct
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_responses_endpoint_succeeds_when_curl_stream_is_empty(self):
        app, tempdir, original_auth_dir = load_app_module({"LITE_TRANSPORT_BACKEND": "curl_impersonate"})
        try:
            account_file = Path(tempdir.name) / "acct.json"
            account_file.write_text(json.dumps({"tokens": {"access_token": "tok", "refresh_token": "ref"}}), encoding="utf-8")
            app.pool.reload()
            app.sync_accounts_with_state()

            class EmptyCurlResponse:
                status = 200
                headers = {}

                def read(self, size=-1):
                    return b""

                def close(self):
                    return None

            class DirectResponse:
                status = 200
                headers = {}

                def __init__(self):
                    self._body = (
                        'event: response.completed\n'
                        'data: {"type":"response.completed","response":{"id":"resp_direct","status":"completed","model":"gpt-5.4","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":10,"output_tokens":2,"input_tokens_details":{"cached_tokens":5}}}}\n\n'
                    ).encode("utf-8")
                    self._offset = 0

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

                def close(self):
                    return None

            original_backend = app.current_transport_backend
            original_urlopen = app.urlopen_with_optional_proxy
            original_direct = app.urlopen_direct_with_optional_proxy
            original_ws_connect = app.websocket_sync_connect
            app.current_transport_backend = lambda: "curl_impersonate"
            app.urlopen_with_optional_proxy = lambda request, proxy_url=None, timeout=120: EmptyCurlResponse()
            app.urlopen_direct_with_optional_proxy = lambda request, proxy_url=None, timeout=120: DirectResponse()
            app.websocket_sync_connect = lambda: (_ for _ in ()).throw(ImportError("disabled"))
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/responses",
                        data=json.dumps({"model": "gpt-5.4", "input": "hello"}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        payload = json.load(resp)
                        account_header = resp.headers.get("X-Codex2gpt-Account")
                        cache_rate = resp.headers.get("X-Codex2gpt-Cache-Hit-Rate")
                self.assertEqual(payload["id"], "resp_direct")
                self.assertEqual(account_header, "acct.json")
                self.assertEqual(cache_rate, "50.00")
            finally:
                app.current_transport_backend = original_backend
                app.urlopen_with_optional_proxy = original_urlopen
                app.urlopen_direct_with_optional_proxy = original_direct
                app.websocket_sync_connect = original_ws_connect
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_responses_endpoint_prefers_websocket_transport(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            account_file = Path(tempdir.name) / "acct.json"
            account_file.write_text(json.dumps({"tokens": {"access_token": "tok", "refresh_token": "ref"}}), encoding="utf-8")
            app.pool.reload()
            app.sync_accounts_with_state()
            captured = {}

            class FakeWebSocket:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def send(self, raw):
                    captured["request"] = json.loads(raw)

                def recv(self):
                    events = getattr(self, "_events", None)
                    if events is None:
                        self._events = iter(
                            [
                                json.dumps({"type": "response.created", "response": {"id": "resp_ws"}}),
                                json.dumps(
                                    {
                                        "type": "response.completed",
                                        "response": {
                                            "id": "resp_ws",
                                            "status": "completed",
                                            "output": [
                                                {
                                                    "type": "message",
                                                    "role": "assistant",
                                                    "content": [{"type": "output_text", "text": "from websocket"}],
                                                }
                                            ],
                                            "usage": {"input_tokens": 1, "output_tokens": 1},
                                        },
                                    }
                                ),
                            ]
                        )
                        events = self._events
                    try:
                        return next(events)
                    except StopIteration:
                        return None

            original_ws_connect = app.websocket_sync_connect
            original_urlopen = app.urlopen_with_optional_proxy

            def fake_ws_connect():
                def connect(url, **kwargs):
                    captured["url"] = url
                    captured["kwargs"] = kwargs
                    return FakeWebSocket()

                return connect

            app.websocket_sync_connect = fake_ws_connect
            app.urlopen_with_optional_proxy = lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("HTTP upstream should not be used when websocket transport succeeds")
            )
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/responses",
                        data=json.dumps({"model": "gpt-5.4", "input": "hello"}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        payload = json.load(resp)
                self.assertEqual(payload["id"], "resp_ws")
                self.assertEqual(captured["url"], "wss://chatgpt.com/backend-api/codex/responses")
                self.assertEqual(captured["request"]["type"], "response.create")
                self.assertEqual(captured["request"]["input"][0]["content"][0]["text"], "hello")
                self.assertEqual(captured["request"]["model"], "gpt-5.4")
            finally:
                app.websocket_sync_connect = original_ws_connect
                app.urlopen_with_optional_proxy = original_urlopen
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_admin_test_connection_prefers_account_quota_diagnostic(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            account_file = Path(tempdir.name) / "acct.json"
            account_file.write_text(json.dumps({"tokens": {"access_token": "tok", "refresh_token": "ref"}}), encoding="utf-8")
            app.pool.reload()
            app.sync_accounts_with_state()
            original_fetch_quota = app.fetch_account_quota
            app.fetch_account_quota = lambda account: {
                "plan_type": "plus",
                "rate_limit": {
                    "allowed": True,
                    "limit_reached": False,
                    "primary_window": {"used_percent": 0.25, "reset_at": "later", "limit_window_seconds": 300},
                },
            }
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/admin/test-connection",
                        data=b"{}",
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        payload = json.load(resp)
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["target"], "account")
                self.assertEqual(payload["account_id"], "acct.json")
                self.assertEqual(payload["quota"]["plan_type"], "plus")
            finally:
                app.fetch_account_quota = original_fetch_quota
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_admin_test_connection_can_target_proxy(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            class FakeJsonResponse:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def read(self, size=-1):
                    return json.dumps({"ip": "1.2.3.4"}).encode("utf-8")

            original_urlopen = app.urlopen_with_optional_proxy
            app.urlopen_with_optional_proxy = lambda request, proxy_url=None, timeout=120: FakeJsonResponse()
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/api/proxies",
                        data=json.dumps({"proxy_id": "proxy-a", "name": "Proxy A", "url": "http://127.0.0.1:8080"}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30):
                        pass
                    req = urllib.request.Request(
                        f"{base_url}/admin/test-connection",
                        data=json.dumps({"proxy_id": "proxy-a"}).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        payload = json.load(resp)
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["target"], "proxy")
                self.assertEqual(payload["proxy_id"], "proxy-a")
            finally:
                app.urlopen_with_optional_proxy = original_urlopen
        finally:
            restore_auth_dir(tempdir, original_auth_dir)

    def test_responses_websocket_failure_falls_back_to_http(self):
        app, tempdir, original_auth_dir = load_app_module()
        try:
            account_file = Path(tempdir.name) / "acct.json"
            account_file.write_text(json.dumps({"tokens": {"access_token": "tok", "refresh_token": "ref"}}), encoding="utf-8")
            app.pool.reload()
            app.sync_accounts_with_state()
            captured = {}

            class FakeResponse:
                status = 200
                headers = {}

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def read(self, size=-1):
                    return (
                        'data: {"type":"response.completed","response":{"id":"resp_http","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"fallback"}]}],"usage":{"input_tokens":1,"output_tokens":1}}}\n\n'
                    ).encode("utf-8")

            original_ws_connect = app.websocket_sync_connect
            original_urlopen = app.urlopen_with_optional_proxy

            app.websocket_sync_connect = lambda: (lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ws unavailable")))

            def fake_urlopen(request, proxy_url=None, timeout=120):
                captured["body"] = json.loads(request.data.decode("utf-8"))
                return FakeResponse()

            app.urlopen_with_optional_proxy = fake_urlopen
            try:
                with run_test_server(app) as base_url:
                    req = urllib.request.Request(
                        f"{base_url}/v1/responses",
                        data=json.dumps(
                            {
                                "model": "gpt-5.4",
                                "input": "hello",
                                "previous_response_id": "resp_prev",
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        payload = json.load(resp)
                self.assertEqual(payload["id"], "resp_http")
                self.assertNotIn("previous_response_id", captured["body"])
            finally:
                app.websocket_sync_connect = original_ws_connect
                app.urlopen_with_optional_proxy = original_urlopen
        finally:
            restore_auth_dir(tempdir, original_auth_dir)


if __name__ == "__main__":
    unittest.main()
