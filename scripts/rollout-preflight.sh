#!/usr/bin/env bash
set -euo pipefail

base_url="${TASK_SPACE_ROLLOUT_URL:-${TASK_SPACE_BROWSER_URL:-}}"
if [[ -z "$base_url" ]]; then
  echo "TASK_SPACE_ROLLOUT_URL or TASK_SPACE_BROWSER_URL is required" >&2
  exit 2
fi
base_url="${base_url%/}"

if [[ "$base_url" != http://* && "$base_url" != https://* ]]; then
  echo "rollout URL must use http:// or https://" >&2
  exit 2
fi

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/task-space-rollout.XXXXXX")"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

request() {
  local name="$1"
  local url="$2"
  shift 2
  curl --silent --show-error --max-time 15 \
    --dump-header "$tmp_dir/$name.headers" --output "$tmp_dir/$name.body" \
    "$@" "$url"
}

header() {
  local name="$1"
  local key="$2"
  awk -v key="$key" 'tolower($0) ~ "^" tolower(key) ":" {
    sub(/^[^:]*:[[:space:]]*/, ""); gsub(/[[:space:]]+$/, ""); print; exit
  }' \
    "$tmp_dir/$name.headers"
}

status() {
  local name="$1"
  awk 'NR == 1 { print $2; exit }' "$tmp_dir/$name.headers"
}

assert_status() {
  local name="$1"
  local expected="$2"
  local actual
  actual="$(status "$name")"
  if [[ "$actual" != "$expected" ]]; then
    echo "$name expected HTTP $expected, got HTTP ${actual:-unknown}" >&2
    exit 1
  fi
}

request readyz "$base_url/readyz"
assert_status readyz 200
if ! grep -Eq '"status"[[:space:]]*:[[:space:]]*"ready"' "$tmp_dir/readyz.body"; then
  echo "readyz did not report ready" >&2
  exit 1
fi
if [[ "$(header readyz Cache-Control)" != "no-store" ]]; then
  echo "readyz must be no-store" >&2
  exit 1
fi

request app "$base_url/app"
assert_status app 200
if ! grep -qi '<html' "$tmp_dir/app.body"; then
  echo "/app did not return an HTML document" >&2
  exit 1
fi

request session "$base_url/auth/session"
if [[ "$(status session)" != "401" ]]; then
  echo "unauthenticated /auth/session must fail closed with HTTP 401" >&2
  exit 1
fi
if [[ "$(header session X-Task-Space-Error-Code)" != "SESSION_REQUIRED" ]]; then
  echo "unauthenticated /auth/session did not return SESSION_REQUIRED" >&2
  exit 1
fi
if [[ "$(header session Cache-Control)" != "no-store" ]]; then
  echo "unauthenticated /auth/session must be no-store" >&2
  exit 1
fi

if [[ -n "${TASK_SPACE_METRICS_URL:-}" ]]; then
  if [[ -z "${TASK_SPACE_METRICS_TOKEN:-}" ]]; then
    echo "TASK_SPACE_METRICS_TOKEN is required with TASK_SPACE_METRICS_URL" >&2
    exit 2
  fi
  printf 'Authorization: Bearer %s\n' "$TASK_SPACE_METRICS_TOKEN" \
    > "$tmp_dir/metrics.authorization"
  request metrics "$TASK_SPACE_METRICS_URL" \
    --header "@$tmp_dir/metrics.authorization"
  assert_status metrics 200
  if ! grep -q '^task_space_' "$tmp_dir/metrics.body"; then
    echo "metrics endpoint did not return Task Space Prometheus metrics" >&2
    exit 1
  fi
fi

echo "rollout preflight passed for $base_url"
