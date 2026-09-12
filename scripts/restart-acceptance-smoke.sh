#!/usr/bin/env bash
set -euo pipefail

# This is intentionally opt-in because it restarts the local acceptance
# containers. It does not target a production host unless the caller points
# the variables at one explicitly.
if [[ "${TASK_SPACE_CHAOS_CONFIRM:-}" != "I_UNDERSTAND_LOCAL_RESTART_TEST" ]]; then
  printf '%s\n' 'Refusing to restart containers without TASK_SPACE_CHAOS_CONFIRM=I_UNDERSTAND_LOCAL_RESTART_TEST.' >&2
  exit 2
fi

api_container="${TASK_SPACE_API_CONTAINER:-task-space-chaos-acceptance}"
database_container="${TASK_SPACE_DATABASE_CONTAINER:-deploy-postgres-1}"
api_url="${TASK_SPACE_API_URL:-http://127.0.0.1:3300}"
deadline_seconds="${TASK_SPACE_CHAOS_TIMEOUT_SECONDS:-45}"

request_status() {
  local url="$1"
  curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "$url"
}

wait_ready() {
  local deadline=$((SECONDS + deadline_seconds))
  local status
  while (( SECONDS < deadline )); do
    status="$(request_status "${api_url}/readyz" || true)"
    if [[ "$status" == "200" ]]; then
      return 0
    fi
    sleep 1
  done
  printf 'API did not become ready within %ss\n' "$deadline_seconds" >&2
  return 1
}

assert_protected_boundary() {
  local headers_file
  headers_file="$(mktemp)"
  trap 'rm -f "$headers_file"' RETURN
  local status
  status="$(curl -sS -D "$headers_file" -o /dev/null --max-time 5 "${api_url}/auth/session")"
  [[ "$status" == "401" ]]
  grep -qi '^cache-control: no-store' "$headers_file"
}

wait_ready
assert_protected_boundary

docker restart "$api_container" >/dev/null
wait_ready
assert_protected_boundary

docker restart "$database_container" >/dev/null
wait_ready
assert_protected_boundary

printf 'API and PostgreSQL restart recovery passed for %s and %s\n' \
  "$api_container" "$database_container"
