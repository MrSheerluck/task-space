#!/usr/bin/env bash
set -euo pipefail

# Run the real frontend and protected sync router against a disposable,
# in-network PostgreSQL account. The server binary uses a deterministic test
# verifier and is never part of the production image.

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container_name="task-space-browser-acceptance"
image_name="task-space-browser-acceptance:current"
account_id="browser-acceptance-$(date +%s)-$$"
session_cookie="browser-acceptance-session"
db_password="${POSTGRES_PASSWORD:-change-me-local-only}"
browser_image="mcr.microsoft.com/playwright/python:v1.47.0-jammy"
browser_url="http://127.0.0.1:3301/app"
server_url="http://127.0.0.1:3301"
export DOCKER_CONFIG="${DOCKER_CONFIG:-${TMPDIR:-/tmp}/task-space-docker-config}"
mkdir -p "$DOCKER_CONFIG"

cleanup() {
  docker rm -f "$container_name" >/dev/null 2>&1 || true
  docker run --rm --network deploy_default \
    --env PGPASSWORD="$db_password" \
    postgres:17-alpine \
    psql --host postgres --username taskspace --dbname taskspace \
      --command "DELETE FROM spaces WHERE account_id = '$account_id'; DELETE FROM billing_events WHERE account_id = '$account_id'; DELETE FROM billing_entitlements WHERE account_id = '$account_id';" \
      >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker build \
  --file "$repo_dir/deploy/browser-acceptance.Dockerfile" \
  --tag "$image_name" \
  "$repo_dir"

docker run --detach --name "$container_name" \
  --network deploy_default \
  --publish 127.0.0.1:3301:3301 \
  --env DATABASE_URL="postgres://taskspace:${db_password}@postgres:5432/taskspace" \
  --env PORT=3301 \
  --env TASK_SPACE_BROWSER_TEST_ACCOUNT="$account_id" \
  "$image_name" >/dev/null

for _ in $(seq 1 60); do
  if curl --silent --show-error --fail --max-time 2 "$server_url/readyz" >/dev/null; then
    break
  fi
  sleep 1
done
curl --silent --show-error --fail --max-time 5 "$server_url/readyz" >/dev/null

session_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  --header "Cookie: task_space_session=$session_cookie" \
  --max-time 5 "$server_url/auth/session")"
if [[ "$session_status" != "200" ]]; then
  echo "browser acceptance session boundary returned HTTP $session_status" >&2
  exit 1
fi

docker run --rm --network host \
  --volume "$repo_dir:/workspace" \
  --workdir /workspace \
  --env TASK_SPACE_BROWSER_URL="$browser_url" \
  --env TASK_SPACE_SESSION_COOKIE="$session_cookie" \
  --env TASK_SPACE_AUTHENTICATED_ACCOUNT_ID="$account_id" \
  "$browser_image" \
  bash -lc 'python3 -m pip install --quiet playwright==1.47.0 && python3 scripts/browser-sync-smoke.py --url "$TASK_SPACE_BROWSER_URL" --session-cookie "$TASK_SPACE_SESSION_COOKIE" --isolated-profiles'

docker run --rm --network host \
  --volume "$repo_dir:/workspace" \
  --workdir /workspace \
  --env TASK_SPACE_BROWSER_URL="$browser_url" \
  --env TASK_SPACE_SESSION_COOKIE="$session_cookie" \
  --env TASK_SPACE_AUTHENTICATED_ACCOUNT_ID="$account_id" \
  "$browser_image" \
  bash -lc 'python3 -m pip install --quiet playwright==1.47.0 && python3 scripts/browser-sync-smoke.py --url "$TASK_SPACE_BROWSER_URL" --session-cookie "$TASK_SPACE_SESSION_COOKIE" --engine webkit'

docker run --rm --network host \
  --volume "$repo_dir:/workspace" \
  --workdir /workspace \
  --env TASK_SPACE_BROWSER_URL="$browser_url" \
  --env TASK_SPACE_SESSION_COOKIE="$session_cookie" \
  --env TASK_SPACE_AUTHENTICATED_ACCOUNT_ID="$account_id" \
  "$browser_image" \
  bash -lc 'python3 -m pip install --quiet playwright==1.47.0 && python3 scripts/browser-sync-smoke.py --url "$TASK_SPACE_BROWSER_URL" --session-cookie "$TASK_SPACE_SESSION_COOKIE" --mobile'

echo "authenticated browser acceptance passed for account $account_id"
