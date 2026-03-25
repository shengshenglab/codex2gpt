"""Persistent runtime state storage built on sqlite3."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _ensure_iso(value: str | None) -> str:
    if value:
        return value
    return _utcnow_iso()


def _loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, separators=(",", ":"))


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


class RuntimeStateStore:
    """Stores runtime state in a single sqlite database."""

    def __init__(self, db_path: str):
        self.db_path = os.path.abspath(db_path)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA synchronous = NORMAL")
            self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                entry_id TEXT PRIMARY KEY,
                auth_file TEXT,
                email TEXT,
                user_id TEXT,
                account_id TEXT,
                plan_type TEXT,
                status TEXT NOT NULL,
                refresh_token TEXT,
                proxy_id TEXT,
                last_error TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                quota_json TEXT NOT NULL DEFAULT '{}',
                usage_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS proxies (
                proxy_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                status TEXT NOT NULL,
                health_json TEXT NOT NULL DEFAULT '{}',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS proxy_assignments (
                account_id TEXT PRIMARY KEY,
                proxy_id TEXT NOT NULL,
                assigned_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS relay_providers (
                provider_id TEXT PRIMARY KEY,
                name TEXT,
                base_url TEXT NOT NULL,
                api_key TEXT NOT NULL,
                format TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS quota_warnings (
                warning_id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                level TEXT NOT NULL,
                warning_type TEXT NOT NULL,
                message TEXT NOT NULL,
                warning_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_quota_warnings_account
            ON quota_warnings(account_id);

            CREATE TABLE IF NOT EXISTS dashboard_sessions (
                session_id TEXT PRIMARY KEY,
                remote_addr TEXT,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_dashboard_sessions_expires_at
            ON dashboard_sessions(expires_at);

            CREATE TABLE IF NOT EXISTS usage_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                input_tokens INTEGER NOT NULL,
                output_tokens INTEGER NOT NULL,
                request_count INTEGER NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_usage_snapshots_account_time
            ON usage_snapshots(account_id, captured_at);
            """
        )
        self._conn.commit()

    def upsert_account(self, entry_id: str, **fields: Any) -> dict[str, Any]:
        now = _utcnow_iso()
        existing = self.get_account(entry_id)
        created_at = existing["created_at"] if existing else now
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO accounts (
                    entry_id, auth_file, email, user_id, account_id, plan_type, status,
                    refresh_token, proxy_id, last_error, metadata_json, quota_json,
                    usage_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(entry_id) DO UPDATE SET
                    auth_file = excluded.auth_file,
                    email = excluded.email,
                    user_id = excluded.user_id,
                    account_id = excluded.account_id,
                    plan_type = excluded.plan_type,
                    status = excluded.status,
                    refresh_token = excluded.refresh_token,
                    proxy_id = excluded.proxy_id,
                    last_error = excluded.last_error,
                    metadata_json = excluded.metadata_json,
                    quota_json = excluded.quota_json,
                    usage_json = excluded.usage_json,
                    updated_at = excluded.updated_at
                """,
                (
                    entry_id,
                    fields.get("auth_file", existing["auth_file"] if existing else None),
                    fields.get("email", existing["email"] if existing else None),
                    fields.get("user_id", existing["user_id"] if existing else None),
                    fields.get("account_id", existing["account_id"] if existing else None),
                    fields.get("plan_type", existing["plan_type"] if existing else None),
                    fields.get("status", existing["status"] if existing else "active"),
                    fields.get("refresh_token", existing["refresh_token"] if existing else None),
                    fields.get("proxy_id", existing["proxy_id"] if existing else None),
                    fields.get("last_error", existing["last_error"] if existing else None),
                    _dumps(fields.get("metadata", existing["metadata"] if existing else {})),
                    _dumps(fields.get("quota", existing["quota"] if existing else {})),
                    _dumps(fields.get("usage", existing["usage"] if existing else {})),
                    created_at,
                    now,
                ),
            )
            self._conn.commit()
        return self.get_account(entry_id) or {}

    def get_account(self, entry_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM accounts WHERE entry_id = ?",
                (entry_id,),
            ).fetchone()
        return self._row_to_account(row) if row else None

    def list_accounts(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM accounts ORDER BY entry_id ASC"
            ).fetchall()
        return [self._row_to_account(row) for row in rows]

    def _row_to_account(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "entry_id": row["entry_id"],
            "auth_file": row["auth_file"],
            "email": row["email"],
            "user_id": row["user_id"],
            "account_id": row["account_id"],
            "plan_type": row["plan_type"],
            "status": row["status"],
            "refresh_token": row["refresh_token"],
            "proxy_id": row["proxy_id"],
            "last_error": row["last_error"],
            "metadata": _loads(row["metadata_json"], {}),
            "quota": _loads(row["quota_json"], {}),
            "usage": _loads(row["usage_json"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def delete_account(self, entry_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM accounts WHERE entry_id = ?", (entry_id,))
            self._conn.execute("DELETE FROM proxy_assignments WHERE account_id = ?", (entry_id,))
            self._conn.execute("DELETE FROM quota_warnings WHERE account_id = ?", (entry_id,))
            self._conn.commit()

    def upsert_proxy(self, proxy_id: str, *, name: str, url: str, status: str = "active", health: dict[str, Any] | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        now = _utcnow_iso()
        existing = self.get_proxy(proxy_id)
        created_at = existing["created_at"] if existing else now
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO proxies (
                    proxy_id, name, url, status, health_json, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(proxy_id) DO UPDATE SET
                    name = excluded.name,
                    url = excluded.url,
                    status = excluded.status,
                    health_json = excluded.health_json,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    proxy_id,
                    name,
                    url,
                    status,
                    _dumps(health if health is not None else (existing["health"] if existing else {})),
                    _dumps(metadata if metadata is not None else (existing["metadata"] if existing else {})),
                    created_at,
                    now,
                ),
            )
            self._conn.commit()
        return self.get_proxy(proxy_id) or {}

    def get_proxy(self, proxy_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM proxies WHERE proxy_id = ?", (proxy_id,)).fetchone()
        return self._row_to_proxy(row) if row else None

    def list_proxies(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM proxies ORDER BY proxy_id ASC").fetchall()
        return [self._row_to_proxy(row) for row in rows]

    def _row_to_proxy(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "proxy_id": row["proxy_id"],
            "name": row["name"],
            "url": row["url"],
            "status": row["status"],
            "health": _loads(row["health_json"], {}),
            "metadata": _loads(row["metadata_json"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def delete_proxy(self, proxy_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM proxies WHERE proxy_id = ?", (proxy_id,))
            self._conn.execute("DELETE FROM proxy_assignments WHERE proxy_id = ?", (proxy_id,))
            self._conn.commit()

    def assign_proxy(self, account_id: str, proxy_id: str) -> None:
        assigned_at = _utcnow_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO proxy_assignments (account_id, proxy_id, assigned_at)
                VALUES (?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    proxy_id = excluded.proxy_id,
                    assigned_at = excluded.assigned_at
                """,
                (account_id, proxy_id, assigned_at),
            )
            self._conn.commit()

    def get_proxy_assignment(self, account_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT proxy_id FROM proxy_assignments WHERE account_id = ?",
                (account_id,),
            ).fetchone()
        return row["proxy_id"] if row else None

    def list_proxy_assignments(self) -> list[dict[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT account_id, proxy_id FROM proxy_assignments ORDER BY account_id ASC"
            ).fetchall()
        return [{"account_id": row["account_id"], "proxy_id": row["proxy_id"]} for row in rows]

    def delete_proxy_assignment(self, account_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM proxy_assignments WHERE account_id = ?", (account_id,))
            self._conn.commit()

    def upsert_relay_provider(self, provider_id: str, *, base_url: str, api_key: str, format: str, enabled: bool = True, name: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        now = _utcnow_iso()
        existing = self.get_relay_provider(provider_id)
        created_at = existing["created_at"] if existing else now
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO relay_providers (
                    provider_id, name, base_url, api_key, format, enabled, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider_id) DO UPDATE SET
                    name = excluded.name,
                    base_url = excluded.base_url,
                    api_key = excluded.api_key,
                    format = excluded.format,
                    enabled = excluded.enabled,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    provider_id,
                    name,
                    base_url,
                    api_key,
                    format,
                    1 if enabled else 0,
                    _dumps(metadata if metadata is not None else (existing["metadata"] if existing else {})),
                    created_at,
                    now,
                ),
            )
            self._conn.commit()
        return self.get_relay_provider(provider_id) or {}

    def get_relay_provider(self, provider_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM relay_providers WHERE provider_id = ?",
                (provider_id,),
            ).fetchone()
        return self._row_to_relay_provider(row) if row else None

    def list_relay_providers(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM relay_providers"
        params: tuple[Any, ...] = ()
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY provider_id ASC"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_relay_provider(row) for row in rows]

    def _row_to_relay_provider(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "provider_id": row["provider_id"],
            "name": row["name"],
            "base_url": row["base_url"],
            "api_key": row["api_key"],
            "format": row["format"],
            "enabled": bool(row["enabled"]),
            "metadata": _loads(row["metadata_json"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def delete_relay_provider(self, provider_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM relay_providers WHERE provider_id = ?", (provider_id,))
            self._conn.commit()

    def set_quota_warnings(self, account_id: str, warnings: Iterable[dict[str, Any]]) -> None:
        created_at = _utcnow_iso()
        with self._lock:
            self._conn.execute("DELETE FROM quota_warnings WHERE account_id = ?", (account_id,))
            for warning in warnings:
                self._conn.execute(
                    """
                    INSERT INTO quota_warnings (account_id, level, warning_type, message, warning_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        account_id,
                        str(warning.get("level") or "warning"),
                        str(warning.get("warning_type") or "unknown"),
                        str(warning.get("message") or ""),
                        _dumps(dict(warning)),
                        created_at,
                    ),
                )
            self._conn.commit()

    def list_quota_warnings(self, level: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM quota_warnings"
        params: list[Any] = []
        if level:
            sql += " WHERE level = ?"
            params.append(level)
        sql += " ORDER BY account_id ASC, warning_id ASC"
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [
            {
                "warning_id": row["warning_id"],
                "account_id": row["account_id"],
                "level": row["level"],
                "warning_type": row["warning_type"],
                "message": row["message"],
                "warning": _loads(row["warning_json"], {}),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def create_dashboard_session(self, session_id: str, *, expires_at: str, remote_addr: str | None = None, created_at: str | None = None) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO dashboard_sessions (session_id, remote_addr, created_at, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    remote_addr = excluded.remote_addr,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at
                """,
                (session_id, remote_addr, _ensure_iso(created_at), expires_at),
            )
            self._conn.commit()

    def validate_dashboard_session(self, session_id: str, now_ts: str | None = None) -> bool:
        now = _parse_iso(now_ts) or datetime.now(timezone.utc)
        with self._lock:
            row = self._conn.execute(
                "SELECT expires_at FROM dashboard_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        if not row:
            return False
        expires_at = _parse_iso(row["expires_at"])
        return expires_at is not None and expires_at > now

    def delete_dashboard_session(self, session_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM dashboard_sessions WHERE session_id = ?", (session_id,))
            self._conn.commit()

    def cleanup_expired_dashboard_sessions(self, now_ts: str | None = None) -> int:
        cutoff = (_parse_iso(now_ts) or datetime.now(timezone.utc)).isoformat(timespec="seconds")
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM dashboard_sessions WHERE expires_at <= ?",
                (cutoff,),
            )
            self._conn.commit()
        return int(cursor.rowcount or 0)

    def append_usage_snapshot(self, account_id: str, *, input_tokens: int, output_tokens: int, request_count: int, captured_at: str | None = None, metadata: dict[str, Any] | None = None) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO usage_snapshots (
                    account_id, captured_at, input_tokens, output_tokens, request_count, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    account_id,
                    _ensure_iso(captured_at),
                    int(input_tokens),
                    int(output_tokens),
                    int(request_count),
                    _dumps(metadata or {}),
                ),
            )
            self._conn.commit()

    def get_usage_summary(self) -> dict[str, Any]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT latest.account_id, latest.captured_at, latest.input_tokens, latest.output_tokens, latest.request_count
                FROM usage_snapshots AS latest
                WHERE latest.snapshot_id = (
                    SELECT candidate.snapshot_id
                    FROM usage_snapshots AS candidate
                    WHERE candidate.account_id = latest.account_id
                    ORDER BY candidate.captured_at DESC, candidate.snapshot_id DESC
                    LIMIT 1
                )
                ORDER BY latest.account_id ASC
                """
            ).fetchall()
        return {
            "account_count": len(rows),
            "total_input_tokens": sum(int(row["input_tokens"]) for row in rows),
            "total_output_tokens": sum(int(row["output_tokens"]) for row in rows),
            "total_request_count": sum(int(row["request_count"]) for row in rows),
            "data_points": len(rows),
        }

    def get_usage_history(self, *, hours: int | None = 24, granularity: str = "raw") -> list[dict[str, Any]]:
        if granularity not in {"raw", "hourly", "daily"}:
            raise ValueError("granularity must be raw, hourly, or daily")
        params: list[Any] = []
        sql = """
            SELECT account_id, captured_at, input_tokens, output_tokens, request_count
            FROM usage_snapshots
        """
        if hours is not None:
            cutoff = datetime.now(timezone.utc).timestamp() - (hours * 3600)
            sql += " WHERE strftime('%s', captured_at) >= ?"
            params.append(int(cutoff))
        sql += " ORDER BY account_id ASC, captured_at ASC, snapshot_id ASC"
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        deltas = self._build_deltas(rows)
        return self._bucketize(deltas, granularity)

    def _build_deltas(self, rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
        deltas: list[dict[str, Any]] = []
        previous: dict[str, dict[str, int]] = {}
        for row in rows:
            current = {
                "input_tokens": int(row["input_tokens"]),
                "output_tokens": int(row["output_tokens"]),
                "request_count": int(row["request_count"]),
            }
            prior = previous.get(row["account_id"])
            if prior is None:
                delta = current
            else:
                delta = {
                    key: current[key] - prior.get(key, 0)
                    for key in current
                }
                for key, value in tuple(delta.items()):
                    if value < 0:
                        delta[key] = current[key]
            previous[row["account_id"]] = current
            deltas.append(
                {
                    "account_id": row["account_id"],
                    "timestamp": row["captured_at"],
                    **delta,
                }
            )
        return deltas

    def _bucketize(self, rows: Iterable[dict[str, Any]], granularity: str) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            timestamp = _parse_iso(row["timestamp"])
            if timestamp is None:
                continue
            if granularity == "raw":
                bucket = timestamp.replace(microsecond=0)
            elif granularity == "hourly":
                bucket = timestamp.replace(minute=0, second=0, microsecond=0)
            else:
                bucket = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            key = bucket.isoformat(timespec="seconds")
            if key not in grouped:
                grouped[key] = {
                    "timestamp": key,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "request_count": 0,
                }
            grouped[key]["input_tokens"] += int(row["input_tokens"])
            grouped[key]["output_tokens"] += int(row["output_tokens"])
            grouped[key]["request_count"] += int(row["request_count"])
        return [grouped[key] for key in sorted(grouped)]
