#!/bin/sh
set -eu

BASE_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
RUNTIME_DIR="$BASE_DIR/runtime"
ACCOUNTS_DIR="$RUNTIME_DIR/accounts"
ENV_FILE="$RUNTIME_DIR/lite.env"
PID_FILE="$RUNTIME_DIR/server.pid"
LOG_FILE="$RUNTIME_DIR/server.log"

python_bin() {
  command -v python3
}

detect_reasoning_effort() {
  "$(python_bin)" - <<'PY'
from pathlib import Path
import tomllib

config = Path.home() / ".codex" / "config.toml"
value = "medium"
try:
    data = tomllib.loads(config.read_text(encoding="utf-8"))
    current = str(data.get("model_reasoning_effort", "")).strip()
    if current:
        value = current
except Exception:
    pass
print(value)
PY
}

detect_text_verbosity() {
  "$(python_bin)" - <<'PY'
from pathlib import Path
import json

cache = Path.home() / ".codex" / "models_cache.json"
value = "high"
preferred = {"gpt-5.4", "gpt-5.3-codex"}
try:
    data = json.loads(cache.read_text(encoding="utf-8"))
    for item in data.get("models", []):
        if item.get("slug") in preferred:
            current = str(item.get("default_verbosity") or "").strip()
            if current:
                value = current
                break
except Exception:
    pass
print(value)
PY
}

detect_context_window() {
  "$(python_bin)" - <<'PY'
from pathlib import Path
import json
import tomllib

config = Path.home() / ".codex" / "config.toml"
value = None
try:
    data = tomllib.loads(config.read_text(encoding="utf-8"))
    current = data.get("model_context_window")
    if current:
        value = int(current)
except Exception:
    pass

if value is None:
    sessions = Path.home() / ".codex" / "sessions"
    try:
        for path in sorted(sessions.rglob("*.jsonl"), reverse=True):
            for line in path.read_text(encoding="utf-8").splitlines():
                if '"model_context_window"' not in line:
                    continue
                payload = json.loads(line)
                current = payload.get("payload", {}).get("model_context_window")
                if current:
                    value = int(current)
                    raise SystemExit(print(value))
    except SystemExit:
        raise
    except Exception:
        pass

if value is None:
    value = 258400
print(value)
PY
}

detect_auto_compact_token_limit() {
  "$(python_bin)" - <<'PY'
from pathlib import Path
import tomllib

config = Path.home() / ".codex" / "config.toml"
context_window = 258400
auto_compact = None
try:
    data = tomllib.loads(config.read_text(encoding="utf-8"))
    current_window = data.get("model_context_window")
    if current_window:
        context_window = int(current_window)
    current_compact = data.get("model_auto_compact_token_limit")
    if current_compact:
        auto_compact = int(current_compact)
except Exception:
    pass

if auto_compact is None:
    auto_compact = (context_window * 9) // 10
print(auto_compact)
PY
}

ensure_layout() {
  mkdir -p "$RUNTIME_DIR" "$ACCOUNTS_DIR"
}

create_env_if_missing() {
  if [ -f "$ENV_FILE" ]; then
    return 0
  fi
  DEFAULT_REASONING_EFFORT="$(detect_reasoning_effort)"
  DEFAULT_TEXT_VERBOSITY="$(detect_text_verbosity)"
  DEFAULT_CONTEXT_WINDOW="$(detect_context_window)"
  DEFAULT_AUTO_COMPACT_TOKEN_LIMIT="$(detect_auto_compact_token_limit)"
  umask 077
  {
    printf 'LITE_HOST=127.0.0.1\n'
    printf 'LITE_PORT=18100\n'
    printf 'LITE_MODEL=gpt-5.4\n'
    printf 'LITE_MODELS=gpt-5.4,gpt-5.3-codex\n'
    printf 'LITE_REASONING_EFFORT=%s\n' "$DEFAULT_REASONING_EFFORT"
    printf 'LITE_TEXT_VERBOSITY=%s\n' "$DEFAULT_TEXT_VERBOSITY"
    printf 'LITE_MODEL_CONTEXT_WINDOW=%s\n' "$DEFAULT_CONTEXT_WINDOW"
    printf 'LITE_MODEL_AUTO_COMPACT_TOKEN_LIMIT=%s\n' "$DEFAULT_AUTO_COMPACT_TOKEN_LIMIT"
    printf 'LITE_AUTH_DIR=%s\n' "$ACCOUNTS_DIR"
    printf 'LITE_API_KEY=\n'
  } >"$ENV_FILE"
}

load_env() {
  create_env_if_missing
  set -a
  . "$ENV_FILE"
  set +a
}

is_running() {
  PID="$(resolve_running_pid || true)"
  [ -n "$PID" ]
}

port_pid() {
  if [ -z "${LITE_PORT:-}" ]; then
    return 1
  fi
  lsof -tiTCP:"$LITE_PORT" -sTCP:LISTEN 2>/dev/null | head -n 1
}

resolve_running_pid() {
  if [ -f "$PID_FILE" ]; then
    PID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
      printf '%s\n' "$PID"
      return 0
    fi
  fi

  PID="$(port_pid || true)"
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    printf '%s\n' "$PID"
    return 0
  fi
  return 1
}

sync_pid_file() {
  PID="$(resolve_running_pid || true)"
  if [ -n "$PID" ]; then
    echo "$PID" >"$PID_FILE"
    return 0
  fi
  rm -f "$PID_FILE"
  return 1
}

wait_for_stop() {
  i=0
  while [ "$i" -lt 30 ]; do
    if ! resolve_running_pid >/dev/null 2>&1; then
      rm -f "$PID_FILE"
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

import_default_auth_if_needed() {
  if find "$ACCOUNTS_DIR" -maxdepth 1 -name '*.json' | grep -q .; then
    return 0
  fi
  if [ -f "$HOME/.codex/auth.json" ]; then
    cp "$HOME/.codex/auth.json" "$ACCOUNTS_DIR/oauth-01.json"
    echo "imported $HOME/.codex/auth.json -> $ACCOUNTS_DIR/oauth-01.json"
  fi
}

wait_for_health() {
  i=0
  while [ "$i" -lt 30 ]; do
    if curl -fsS "http://${LITE_HOST}:${LITE_PORT}/health" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

start() {
  ensure_layout
  load_env
  import_default_auth_if_needed
  if ! find "$ACCOUNTS_DIR" -maxdepth 1 -name '*.json' | grep -q .; then
    echo "no oauth json found in $ACCOUNTS_DIR"
    echo "login first, then run: ./run.sh add-auth oauth-01"
    return 1
  fi
  if is_running; then
    sync_pid_file >/dev/null 2>&1 || true
    echo "already running: pid $(cat "$PID_FILE")"
    return 0
  fi
  rm -f "$PID_FILE"
  PID="$(
    BASE_DIR="$BASE_DIR" \
    LOG_FILE="$LOG_FILE" \
    LITE_HOST="$LITE_HOST" \
    LITE_PORT="$LITE_PORT" \
    LITE_MODEL="$LITE_MODEL" \
    LITE_MODELS="${LITE_MODELS:-}" \
    LITE_REASONING_EFFORT="${LITE_REASONING_EFFORT:-medium}" \
    LITE_TEXT_VERBOSITY="${LITE_TEXT_VERBOSITY:-high}" \
    LITE_MODEL_CONTEXT_WINDOW="${LITE_MODEL_CONTEXT_WINDOW:-258400}" \
    LITE_MODEL_AUTO_COMPACT_TOKEN_LIMIT="${LITE_MODEL_AUTO_COMPACT_TOKEN_LIMIT:-232560}" \
    LITE_AUTH_DIR="$LITE_AUTH_DIR" \
    LITE_API_KEY="$LITE_API_KEY" \
    "$(python_bin)" -c '
import os
import subprocess
import sys

base_dir = os.environ["BASE_DIR"]
log_path = os.environ["LOG_FILE"]
python_bin = sys.executable
script = os.path.join(base_dir, "app.py")
with open(log_path, "ab", buffering=0) as log_file:
    proc = subprocess.Popen(
        [python_bin, script],
        cwd=base_dir,
        stdin=subprocess.DEVNULL,
        stdout=log_file,
        stderr=log_file,
        start_new_session=True,
        close_fds=True,
        env=os.environ.copy(),
    )
print(proc.pid)
'
  )"
  echo "$PID" >"$PID_FILE"
  if ! wait_for_health; then
    echo "start failed, check $LOG_FILE"
    rm -f "$PID_FILE"
    return 1
  fi
  echo "started"
  echo "base_url=http://${LITE_HOST}:${LITE_PORT}/v1"
  echo "default_model=${LITE_MODEL}"
  if [ -n "${LITE_MODELS:-}" ]; then
    echo "models=${LITE_MODELS}"
  fi
  echo "default_reasoning_effort=${LITE_REASONING_EFFORT:-medium}"
  echo "default_text_verbosity=${LITE_TEXT_VERBOSITY:-high}"
  echo "default_model_context_window=${LITE_MODEL_CONTEXT_WINDOW:-258400}"
  echo "default_model_auto_compact_token_limit=${LITE_MODEL_AUTO_COMPACT_TOKEN_LIMIT:-232560}"
  if [ -n "${LITE_API_KEY}" ]; then
    echo "api_key=${LITE_API_KEY}"
  else
    echo "api_key=disabled"
  fi
}

stop() {
  ensure_layout
  load_env
  PID="$(resolve_running_pid || true)"
  if [ -n "$PID" ]; then
    kill "$PID" 2>/dev/null || true
    if ! wait_for_stop; then
      echo "stop failed, process still listening on port $LITE_PORT"
      return 1
    fi
    echo "stopped"
    return 0
  fi
  rm -f "$PID_FILE"
  echo "already stopped"
  return 0
}

restart() {
  stop >/dev/null 2>&1 || true
  start
}

status() {
  ensure_layout
  load_env
  if sync_pid_file >/dev/null 2>&1; then
    echo "service=running"
    echo "pid=$(cat "$PID_FILE")"
  else
    echo "service=stopped"
  fi
  if curl -fsS "http://${LITE_HOST}:${LITE_PORT}/health" >/dev/null 2>&1; then
    echo "health=ok"
  else
    echo "health=down"
  fi
  echo "accounts_dir=$ACCOUNTS_DIR"
  find "$ACCOUNTS_DIR" -maxdepth 1 -name '*.json' -exec basename {} \; | sort
}

add_auth() {
  ensure_layout
  NAME="${2:-}"
  if [ ! -f "$HOME/.codex/auth.json" ]; then
    echo "missing $HOME/.codex/auth.json"
    return 1
  fi
  if [ -z "$NAME" ]; then
    COUNT="$(find "$ACCOUNTS_DIR" -maxdepth 1 -name '*.json' | wc -l | tr -d ' ')"
    NAME="$(printf 'oauth-%02d' "$((COUNT + 1))")"
  fi
  cp "$HOME/.codex/auth.json" "$ACCOUNTS_DIR/$NAME.json"
  echo "saved=$ACCOUNTS_DIR/$NAME.json"
  return 0
}

usage() {
  cat <<'EOF'
Usage:
  ./run.sh start
  ./run.sh stop
  ./run.sh restart
  ./run.sh status
  ./run.sh add-auth [name]
EOF
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) restart ;;
  status) status ;;
  add-auth) add_auth "$@" ;;
  *) usage; exit 1 ;;
esac
