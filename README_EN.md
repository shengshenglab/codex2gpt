# codex2gpt

[中文](./README.md)

`codex2gpt` is a local Python gateway that exposes OpenAI-compatible, Anthropic-compatible, Gemini-compatible, and native `Responses API` endpoints from one service.

It is designed for local or private-network use when you want one stable entry point for Codex-backed accounts, proxy routing, request history, and a lightweight operations dashboard.

## What This Repository Is

This project mainly does four things:

- translates multiple client protocols into the Codex backend shape
- manages multiple local OAuth accounts and routes traffic across them
- switches which login account Codex App uses without repeated re-login
- provides a lightweight dashboard for accounts, proxies, and usage
- gives agents, scripts, and internal tools one local API surface

Currently supported endpoints:

- OpenAI-compatible: `POST /v1/chat/completions`
- Anthropic-compatible: `POST /v1/messages`
- Gemini-compatible: `POST /v1beta/models/{model}:generateContent`
- Native endpoint: `POST /v1/responses`

## Screenshots

Account overview:

![Account overview screenshot](./docs/images/account.png)

This page shows account status, the current Codex App account, quota windows, request volume, and proxy assignment status.

Add account flow:

![Add account screenshot](./docs/images/login.png)

This page starts the browser OAuth flow and also supports pasting the callback URL manually when automatic callback handling does not finish cleanly.

## Good Fit For

- running multiple Codex accounts behind one local API
- reusing existing OpenAI, Anthropic, or Gemini client integrations with minimal changes
- checking account state, quota windows, proxies, and recent requests from one dashboard
- switching which saved account Codex App uses without logging in again every time
- providing a stable local proxy layer for agents, scripts, or internal tools

## Installation

Recommended shortcut inside Codex:

```text
Directly download and start this repository: https://github.com/shengshenglab/codex2gpt.git
```

Manual setup also works:

```bash
git clone https://github.com/shengshenglab/codex2gpt.git
cd codex2gpt
./run.sh start
```

## Before First Start

- Python 3.11+ is required
- on first start, `./run.sh start` tries to import `~/.codex/auth.json`
- if that file does not exist yet, startup will fail and ask you to log in to Codex first, then run `./run.sh add-auth oauth-01`

The smoothest path is:

1. log in to Codex on the same machine
2. confirm that `~/.codex/auth.json` exists
3. run `./run.sh start`

## Usage

### 1. Start the service

```bash
./run.sh start
```

Useful commands:

```bash
./run.sh status
./run.sh stop
./run.sh restart
./run.sh add-auth oauth-02
```

### 2. Open the dashboard

- Dashboard: [http://127.0.0.1:18100/](http://127.0.0.1:18100/)
- Health: [http://127.0.0.1:18100/health](http://127.0.0.1:18100/health)

### 3. Manage accounts

- if `~/.codex/auth.json` already exists, `run.sh` imports it into `runtime/accounts/oauth-01.json` on first start
- after logging in with another Codex account locally, you can run `./run.sh add-auth oauth-02`
- once the service is running, you can also add accounts from the dashboard through the browser OAuth flow
- the dashboard can switch the account used by Codex App. After your accounts are added once, you can just click the target account in the management UI instead of logging in and out repeatedly
- under the hood, this writes the selected account back to `~/.codex/auth.json` for Codex App to use

### 4. Call the APIs

List models:

```bash
curl http://127.0.0.1:18100/v1/models
```

Call the native `Responses API`:

```bash
curl http://127.0.0.1:18100/v1/responses \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt-5.4",
    "input": "Reply with exactly OK.",
    "stream": false
  }'
```

Call OpenAI Chat Completions:

```bash
curl http://127.0.0.1:18100/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt-5.4",
    "messages": [
      { "role": "user", "content": "Say hello." }
    ],
    "stream": false
  }'
```

Call Anthropic Messages:

```bash
curl http://127.0.0.1:18100/v1/messages \
  -H 'Content-Type: application/json' \
  -H 'anthropic-version: 2023-06-01' \
  -d '{
    "model": "claude-opus-4-6",
    "max_tokens": 256,
    "messages": [
      { "role": "user", "content": "Say hello." }
    ],
    "stream": false
  }'
```

Call the Gemini-compatible endpoint:

```bash
curl http://127.0.0.1:18100/v1beta/models/gpt-5.4:generateContent \
  -H 'Content-Type: application/json' \
  -d '{
    "contents": [
      {
        "role": "user",
        "parts": [{ "text": "Say hello." }]
      }
    ]
  }'
```

## Core Features

- multi-protocol compatibility for existing clients
- multi-account rotation with `least_used`, `round_robin`, and `sticky`
- structured outputs and tool calling support
- built-in dashboard for account, proxy, usage, and recent-request visibility
- runtime state persisted locally for recovery and troubleshooting

## Runtime Data

The service stores local operational state under `runtime/`, including:

- `runtime/accounts/*.json`
- `runtime/state.sqlite3`
- `runtime/settings.json`
- `runtime/cookies.json`
- `runtime/fingerprint-cache.json`
- `runtime/transcripts/`

These are runtime artifacts, not source files you normally edit or commit.

## Docs For Agents

- Repo overview: [`AGENT_OVERVIEW.md`](./AGENT_OVERVIEW.md)
- API integration guide: [`AGENT_INTEGRATION.md`](./AGENT_INTEGRATION.md)

## Repository Layout

```text
.
├── app.py
├── codex2gpt/
│   ├── events.py
│   ├── protocols/
│   ├── schema_utils.py
│   └── state_db.py
├── run.sh
├── runtime/
├── tests/
└── web/
```
