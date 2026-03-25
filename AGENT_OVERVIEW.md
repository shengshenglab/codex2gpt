# Codex2gpt Agent Overview

This document gives agents a fast, accurate map of the repository before making changes.

## One-Screen Summary

`codex2gpt` is a single-process Python local gateway that fronts Codex-backed accounts and exposes multiple API styles from one service:

- OpenAI Chat Completions
- Anthropic Messages
- Gemini generateContent
- native Responses API

It also serves a lightweight dashboard for account management, proxy routing, usage visibility, and recent-request inspection.

## Main Goals

- provide one local endpoint for several client protocols
- reuse multiple local OAuth accounts behind a routing layer
- preserve stable request identity for cache reuse
- keep operations simple with one Python service and one static dashboard

## Where The Logic Lives

### Main service

- [app.py](/Users/xysheng/Desktop/codex/codex2gpt/app.py)

This is the primary control center. It currently handles:

- HTTP routing
- protocol translation
- inference endpoints
- admin endpoints
- OAuth add-account flow
- account selection and rotation
- proxy and relay dispatch
- usage accounting
- transcript recording
- periodic runtime jobs

### Helper modules

- [codex2gpt/events.py](/Users/xysheng/Desktop/codex/codex2gpt/codex2gpt/events.py)
- [codex2gpt/schema_utils.py](/Users/xysheng/Desktop/codex/codex2gpt/codex2gpt/schema_utils.py)
- [codex2gpt/state_db.py](/Users/xysheng/Desktop/codex/codex2gpt/codex2gpt/state_db.py)
- [codex2gpt/protocols/gemini.py](/Users/xysheng/Desktop/codex/codex2gpt/codex2gpt/protocols/gemini.py)
- [codex2gpt/protocols/relay.py](/Users/xysheng/Desktop/codex/codex2gpt/codex2gpt/protocols/relay.py)

### Dashboard

- [web/index.html](/Users/xysheng/Desktop/codex/codex2gpt/web/index.html)
- [web/app.js](/Users/xysheng/Desktop/codex/codex2gpt/web/app.js)
- [web/styles.css](/Users/xysheng/Desktop/codex/codex2gpt/web/styles.css)

The frontend is static and served by the same Python process.

### Tests

- [tests/test_app.py](/Users/xysheng/Desktop/codex/codex2gpt/tests/test_app.py)
- [tests/test_state_db.py](/Users/xysheng/Desktop/codex/codex2gpt/tests/test_state_db.py)
- [tests/test_gemini_protocol.py](/Users/xysheng/Desktop/codex/codex2gpt/tests/test_gemini_protocol.py)
- [tests/test_runtime_features.py](/Users/xysheng/Desktop/codex/codex2gpt/tests/test_runtime_features.py)
- [tests/test_relay_protocol.py](/Users/xysheng/Desktop/codex/codex2gpt/tests/test_relay_protocol.py)

## Runtime State

Mutable operational data lives under `runtime/`, including:

- `runtime/accounts/*.json`
- `runtime/state.sqlite3`
- `runtime/settings.json`
- `runtime/cookies.json`
- `runtime/fingerprint-cache.json`
- `runtime/transcripts/`

Treat these as runtime artifacts, not source files.

## Supported Protocol Surface

- `POST /v1/responses`
- `POST /v1/chat/completions`
- `POST /v1/messages`
- `POST /v1/messages/count_tokens`
- `POST /v1beta/models/{model}:generateContent`
- `POST /v1beta/models/{model}:streamGenerateContent`
- `GET /v1/models`
- `GET /v1/models/catalog`

## Routing Model

Requests flow through a session-aware account pool.

Important behavior:

- rotation mode can be `least_used`, `round_robin`, or `sticky`
- stable request identity improves upstream cache reuse
- accounts marked `rate_limited`, `expired`, or `banned` are skipped
- recent request records keep the actual serving account

## Dashboard Scope

The built-in dashboard currently covers:

- account overview
- add-account OAuth flow
- switching the saved account used by Codex App through `~/.codex/auth.json`
- runtime settings
- recent requests
- proxy health and assignments
- relay provider status
- usage charts and snapshots
- warning banners
- manual runtime-job triggers

## Important Caveats

- Anthropic compatibility is alias-based, not arbitrary Claude passthrough
- current Anthropic aliases are `claude-opus-4-6 -> gpt-5.4` and `claude-sonnet-4-6 -> gpt-5.3-codex`
- transcripts are operational records and are not automatically redacted
- `app.py` is still the main integration point, so cross-cutting edits often land there

## Recommended Reading Order

1. [README.md](/Users/xysheng/Desktop/codex/codex2gpt/README.md)
2. [AGENT_INTEGRATION.md](/Users/xysheng/Desktop/codex/codex2gpt/AGENT_INTEGRATION.md)
3. [app.py](/Users/xysheng/Desktop/codex/codex2gpt/app.py)
4. [codex2gpt/state_db.py](/Users/xysheng/Desktop/codex/codex2gpt/codex2gpt/state_db.py)
5. [tests/test_runtime_features.py](/Users/xysheng/Desktop/codex/codex2gpt/tests/test_runtime_features.py)
