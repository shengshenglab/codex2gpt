import tempfile
import unittest
from pathlib import Path

from codex2gpt.state_db import RuntimeStateStore


class RuntimeStateStoreTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "runtime" / "state.sqlite3"
        self.store = RuntimeStateStore(str(self.db_path))

    def tearDown(self):
        self.store.close()
        self.tempdir.cleanup()

    def test_initializes_database_path_and_tables(self):
        self.assertTrue(self.db_path.exists())
        self.assertEqual(self.store.list_accounts(), [])
        self.assertEqual(self.store.list_proxies(), [])
        self.assertEqual(self.store.list_relay_providers(), [])

    def test_upsert_and_list_accounts(self):
        self.store.upsert_account(
            "acct-1",
            auth_file="/tmp/oauth-01.json",
            email="a@example.com",
            user_id="user-1",
            account_id="chatgpt-1",
            plan_type="plus",
            status="active",
            refresh_token="refresh-1",
            proxy_id="proxy-1",
            metadata={"region": "us"},
            quota={"used_percent": 15},
            usage={"request_count": 3},
        )
        self.store.upsert_account(
            "acct-2",
            auth_file="/tmp/oauth-02.json",
            email="b@example.com",
            status="rate_limited",
        )
        self.store.upsert_account(
            "acct-1",
            status="banned",
            last_error="403 upstream",
            quota={"used_percent": 100},
        )

        account = self.store.get_account("acct-1")
        assert account is not None
        self.assertEqual(account["status"], "banned")
        self.assertEqual(account["email"], "a@example.com")
        self.assertEqual(account["metadata"]["region"], "us")
        self.assertEqual(account["quota"]["used_percent"], 100)
        self.assertEqual(account["proxy_id"], "proxy-1")

        accounts = self.store.list_accounts()
        self.assertEqual([item["entry_id"] for item in accounts], ["acct-1", "acct-2"])

    def test_proxy_crud_and_assignments(self):
        self.store.upsert_proxy(
            "proxy-1",
            name="Tokyo",
            url="http://proxy.example:8080",
            status="active",
            health={"exit_ip": "1.1.1.1", "latency_ms": 120},
        )
        self.store.upsert_proxy("proxy-2", name="Direct", url="", status="disabled")
        self.store.assign_proxy("acct-1", "proxy-1")
        self.store.assign_proxy("acct-2", "direct")

        proxy = self.store.get_proxy("proxy-1")
        assert proxy is not None
        self.assertEqual(proxy["health"]["exit_ip"], "1.1.1.1")
        self.assertEqual(self.store.get_proxy_assignment("acct-1"), "proxy-1")
        self.assertEqual(
            self.store.list_proxy_assignments(),
            [
                {"account_id": "acct-1", "proxy_id": "proxy-1"},
                {"account_id": "acct-2", "proxy_id": "direct"},
            ],
        )

        self.store.delete_proxy_assignment("acct-2")
        self.assertIsNone(self.store.get_proxy_assignment("acct-2"))
        self.store.delete_proxy("proxy-2")
        self.assertEqual([item["proxy_id"] for item in self.store.list_proxies()], ["proxy-1"])

    def test_relay_provider_crud(self):
        self.store.upsert_relay_provider(
            "relay-1",
            name="backup",
            base_url="https://relay.example/v1",
            api_key="secret",
            format="openai_chat",
            enabled=True,
            metadata={"priority": 2},
        )
        self.store.upsert_relay_provider(
            "relay-2",
            name="passthrough",
            base_url="https://relay2.example/v1",
            api_key="secret-2",
            format="responses",
            enabled=False,
        )

        relay = self.store.get_relay_provider("relay-1")
        assert relay is not None
        self.assertEqual(relay["metadata"]["priority"], 2)
        self.assertEqual(
            [item["provider_id"] for item in self.store.list_relay_providers(enabled_only=True)],
            ["relay-1"],
        )

        self.store.delete_relay_provider("relay-2")
        self.assertEqual(
            [item["provider_id"] for item in self.store.list_relay_providers()],
            ["relay-1"],
        )

    def test_quota_warnings_replace_and_filter(self):
        self.store.set_quota_warnings(
            "acct-1",
            [
                {"level": "warning", "warning_type": "primary", "message": "80% reached"},
                {"level": "critical", "warning_type": "secondary", "message": "95% reached"},
            ],
        )
        self.store.set_quota_warnings(
            "acct-2",
            [{"level": "warning", "warning_type": "primary", "message": "82% reached"}],
        )

        warnings = self.store.list_quota_warnings()
        self.assertEqual(len(warnings), 3)
        critical = self.store.list_quota_warnings(level="critical")
        self.assertEqual(len(critical), 1)
        self.assertEqual(critical[0]["account_id"], "acct-1")

        self.store.set_quota_warnings("acct-1", [{"level": "warning", "warning_type": "primary", "message": "84%"}])
        acct1 = [item for item in self.store.list_quota_warnings() if item["account_id"] == "acct-1"]
        self.assertEqual(len(acct1), 1)
        self.assertEqual(acct1[0]["message"], "84%")

    def test_dashboard_session_lifecycle(self):
        self.store.create_dashboard_session(
            "sess-1",
            expires_at="2030-01-01T00:00:00+00:00",
            remote_addr="10.0.0.1",
            created_at="2029-01-01T00:00:00+00:00",
        )
        self.assertTrue(self.store.validate_dashboard_session("sess-1", now_ts="2029-06-01T00:00:00+00:00"))
        self.assertFalse(self.store.validate_dashboard_session("sess-1", now_ts="2031-01-01T00:00:00+00:00"))

        removed = self.store.cleanup_expired_dashboard_sessions(now_ts="2031-01-01T00:00:00+00:00")
        self.assertEqual(removed, 1)
        self.assertFalse(self.store.validate_dashboard_session("sess-1"))

        self.store.create_dashboard_session("sess-2", expires_at="2030-01-02T00:00:00+00:00")
        self.store.delete_dashboard_session("sess-2")
        self.assertFalse(self.store.validate_dashboard_session("sess-2"))

    def test_usage_summary_and_history(self):
        self.store.append_usage_snapshot(
            "acct-1",
            captured_at="2026-03-24T00:00:00+00:00",
            input_tokens=10,
            output_tokens=4,
            request_count=1,
        )
        self.store.append_usage_snapshot(
            "acct-1",
            captured_at="2026-03-24T01:00:00+00:00",
            input_tokens=30,
            output_tokens=10,
            request_count=3,
        )
        self.store.append_usage_snapshot(
            "acct-2",
            captured_at="2026-03-24T01:00:00+00:00",
            input_tokens=5,
            output_tokens=2,
            request_count=1,
        )
        self.store.append_usage_snapshot(
            "acct-1",
            captured_at="2026-03-24T02:00:00+00:00",
            input_tokens=7,
            output_tokens=3,
            request_count=1,
        )

        summary = self.store.get_usage_summary()
        self.assertEqual(summary["account_count"], 2)
        self.assertEqual(summary["total_input_tokens"], 12)
        self.assertEqual(summary["total_output_tokens"], 5)
        self.assertEqual(summary["total_request_count"], 2)

        raw = self.store.get_usage_history(hours=48, granularity="raw")
        self.assertEqual(len(raw), 3)
        self.assertEqual(raw[0]["input_tokens"], 10)
        self.assertEqual(raw[1]["input_tokens"], 25)
        self.assertEqual(raw[2]["input_tokens"], 7)

        hourly = self.store.get_usage_history(hours=48, granularity="hourly")
        self.assertEqual(len(hourly), 3)
        self.assertEqual(hourly[1]["input_tokens"], 25)

        daily = self.store.get_usage_history(hours=48, granularity="daily")
        self.assertEqual(len(daily), 1)
        self.assertEqual(daily[0]["input_tokens"], 42)
        self.assertEqual(daily[0]["request_count"], 5)


if __name__ == "__main__":
    unittest.main()

