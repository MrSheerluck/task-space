#!/usr/bin/env bash
set -euo pipefail

# Abruptly terminate the disposable local API and PostgreSQL containers, then
# restart them and verify the protected service recovers. This is deliberately
# scoped to named local containers; production process-kill experiments need
# the deployment owner's change window and observability controls.
if [[ "${TASK_SPACE_CHAOS_CONFIRM:-}" != "I_UNDERSTAND_LOCAL_KILL_TEST" ]]; then
  printf '%s\n' 'Refusing to kill containers without TASK_SPACE_CHAOS_CONFIRM=I_UNDERSTAND_LOCAL_KILL_TEST.' >&2
  exit 2
fi

api_container="${TASK_SPACE_KILL_API_CONTAINER:-task-space-chaos-acceptance}"
database_container="${TASK_SPACE_KILL_DATABASE_CONTAINER:-deploy-postgres-1}"
api_url="${TASK_SPACE_KILL_API_URL:-http://127.0.0.1:3300}"
deadline_seconds="${TASK_SPACE_KILL_TIMEOUT_SECONDS:-60}"

request_status() {
  local url="$1"
  curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "$url"
}

wait_ready() {
  local deadline=$((SECONDS + deadline_seconds))
  local http_status
  while (( SECONDS < deadline )); do
    http_status="$(request_status "${api_url}/readyz" || true)"
    if [[ "$http_status" == "200" ]]; then
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
  local http_status
  http_status="$(curl -sS -D "$headers_file" -o /dev/null --max-time 5 "${api_url}/auth/session")"
  [[ "$http_status" == "401" ]]
  grep -qi '^cache-control: no-store' "$headers_file"
}

wait_ready
assert_protected_boundary

docker kill "$api_container" >/dev/null
docker start "$api_container" >/dev/null
wait_ready
assert_protected_boundary

docker kill "$database_container" >/dev/null
docker start "$database_container" >/dev/null
wait_ready
assert_protected_boundary

printf 'Abrupt process termination recovery passed for %s and %s\n' \
  "$api_container" "$database_container"
