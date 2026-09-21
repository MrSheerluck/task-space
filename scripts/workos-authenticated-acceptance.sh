#!/usr/bin/env bash
set -euo pipefail

# Exercise the real WorkOS password session through the deployed protected
# API and frontend. This creates only a disposable verified test user and
# removes that user plus its local account data on exit. It is intentionally
# separate from browser-authenticated-acceptance.sh, whose deterministic
# verifier keeps the local browser gate repeatable without provider access.

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${TASK_SPACE_WORKOS_ENV_FILE:-$repo_dir/.env}"
app_origin="${TASK_SPACE_WORKOS_URL:-https://taskspace-dev.smbl.dev}"
browser_url="${TASK_SPACE_WORKOS_BROWSER_URL:-$app_origin/app}"
db_network="${TASK_SPACE_WORKOS_DB_NETWORK:-deploy_default}"
db_password="${POSTGRES_PASSWORD:-change-me-local-only}"
browser_image="${TASK_SPACE_WORKOS_BROWSER_IMAGE:-mcr.microsoft.com/playwright/python:v1.47.0-jammy}"
browser_add_host="${TASK_SPACE_WORKOS_BROWSER_ADD_HOST:-}"
tmp_dir="$(mktemp -d -t task-space-workos-acceptance.XXXXXX)"
user_id=""
account_id=""

cleanup() {
  if [[ -n "$user_id" && -n "${WORKOS_API_KEY:-}" ]]; then
    "$curl_bin" -sS -o /dev/null --max-time 20 -X DELETE \
      -H "Authorization: Bearer $WORKOS_API_KEY" \
      "${WORKOS_ISSUER%/}/user_management/users/$user_id" || true
  fi
  if [[ -n "$account_id" && -n "${docker_bin:-}" ]]; then
    "$docker_bin" run --rm --network "$db_network" \
      --env PGPASSWORD="$db_password" \
      postgres:17-alpine \
      psql --host postgres --username taskspace --dbname taskspace \
      --command "DELETE FROM spaces WHERE account_id = '$account_id'; DELETE FROM billing_events WHERE account_id = '$account_id'; DELETE FROM billing_entitlements WHERE account_id = '$account_id'; DELETE FROM billing_webhook_inbox WHERE webhook_id LIKE 'evt-task-space-workos-acceptance-%';" \
      >/dev/null 2>&1 || true
  fi
  /bin/rm -rf "$tmp_dir"
}
trap cleanup EXIT

if [[ ! -f "$env_file" ]]; then
  echo "WorkOS acceptance requires $env_file" >&2
  exit 1
fi

original_path="$PATH"
set -a
# shellcheck disable=SC1090
source "$env_file"
set +a
# Keep an env-file PATH assignment from hiding required host tools.
PATH="$original_path"
export PATH

for variable in WORKOS_API_KEY WORKOS_CLIENT_ID WORKOS_ISSUER \
  DODO_PAYMENTS_WEBHOOK_KEY DODO_PRO_MONTHLY_PRODUCT_ID; do
  if [[ -z "${!variable:-}" ]]; then
    echo "WorkOS acceptance requires $variable" >&2
    exit 1
  fi
done

curl_bin="$(command -v curl || true)"
docker_bin="$(command -v docker || true)"
openssl_bin="$(command -v openssl || true)"
base64_bin="$(command -v base64 || true)"
xxd_bin="$(command -v xxd || true)"
python_bin="$(command -v python3 || true)"
if [[ -z "$curl_bin" || -z "$docker_bin" || -z "$openssl_bin" || -z "$base64_bin" || -z "$xxd_bin" || -z "$python_bin" ]]; then
  echo "WorkOS acceptance requires curl, docker, openssl, base64, xxd, and python3" >&2
  exit 1
fi

session_cookie_name="${WORKOS_SESSION_COOKIE:-task_space_session}"
email="task-space-sync-acceptance-$(date +%s)-$$@example.com"
password="TsA-$(date +%s)-$($openssl_bin rand -hex 12)!"

create_http_status="$($curl_bin -sS -o "$tmp_dir/create.json" -w '%{http_code}' --max-time 20 \
  -X POST "${WORKOS_ISSUER%/}/user_management/users" \
  -H "Authorization: Bearer $WORKOS_API_KEY" \
  -H 'Content-Type: application/json' \
  --data-raw "{\"email\":\"$email\",\"password\":\"$password\",\"email_verified\":true}")"
if [[ "$create_http_status" != 2* ]]; then
  echo "WorkOS disposable user creation returned HTTP $create_http_status" >&2
  exit 1
fi

user_id="$($python_bin - "$tmp_dir/create.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    response = json.load(handle)
print(response.get("id") or response.get("user", {}).get("id") or "")
PY
)"
if [[ -z "$user_id" ]]; then
  echo "WorkOS disposable user response did not contain a user id" >&2
  exit 1
fi
account_id="$user_id"

auth_http_status="$($curl_bin -sS -o "$tmp_dir/auth.json" -w '%{http_code}' --max-time 20 \
  -X POST "${WORKOS_ISSUER%/}/user_management/authenticate" \
  -H 'Content-Type: application/json' \
  --data-raw "{\"client_id\":\"$WORKOS_CLIENT_ID\",\"client_secret\":\"$WORKOS_API_KEY\",\"grant_type\":\"password\",\"email\":\"$email\",\"password\":\"$password\"}")"
if [[ "$auth_http_status" != "200" ]]; then
  echo "WorkOS password authentication returned HTTP $auth_http_status" >&2
  exit 1
fi

access_token="$($python_bin - "$tmp_dir/auth.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    response = json.load(handle)
token = response.get("access_token") or ""
if not token or not response.get("refresh_token"):
    raise SystemExit("WorkOS response did not contain both session tokens")
print(token)
PY
)"

session_http_status="$($curl_bin -sS -o "$tmp_dir/session.json" -w '%{http_code}' --max-time 20 \
  -H "Cookie: $session_cookie_name=$access_token" \
  "$app_origin/auth/session")"
if [[ "$session_http_status" != "200" ]]; then
  echo "deployed WorkOS session boundary returned HTTP $session_http_status" >&2
  exit 1
fi
session_account_id="$($python_bin - "$tmp_dir/session.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    response = json.load(handle)
print(response.get("account_id") or "")
PY
)"
if [[ "$session_account_id" != "$account_id" ]]; then
  echo "deployed WorkOS session resolved to an unexpected account" >&2
  exit 1
fi

# Grant the disposable WorkOS account the same server-owned entitlement path
# used by real Dodo webhook delivery. The API never trusts the browser for
# this state; the signed webhook is posted to the public deployment.
event_id="evt-task-space-workos-acceptance-$(date +%s)-$$"
subscription_id="sub-task-space-workos-acceptance-$(date +%s)-$$"
timestamp="$(date +%s)"
printf '{"type":"subscription.active","data":{"product_id":"%s","subscription_id":"%s","customer":{"customer_id":"cus-task-space-workos-acceptance"},"metadata":{"account_id":"%s"},"next_billing_date":"2030-01-01T00:00:00Z","cancel_at_next_billing_date":false}}' \
  "$DODO_PRO_MONTHLY_PRODUCT_ID" "$subscription_id" "$account_id" > "$tmp_dir/webhook.json"
printf '%s.%s.' "$event_id" "$timestamp" > "$tmp_dir/signing-input"
cat "$tmp_dir/webhook.json" >> "$tmp_dir/signing-input"
secret_b64="${DODO_PAYMENTS_WEBHOOK_KEY#whsec_}"
secret_hex="$($base64_bin --decode <<< "$secret_b64" | $xxd_bin -p -c 100000)"
signature="$($openssl_bin dgst -sha256 -mac HMAC -macopt "hexkey:$secret_hex" -binary "$tmp_dir/signing-input" | $base64_bin | tr -d '\n')"

webhook_http_status="$($curl_bin -sS -o "$tmp_dir/webhook.json.response" -w '%{http_code}' --max-time 20 \
  -X POST "$app_origin/webhooks/dodo" \
  -H 'Content-Type: application/json' \
  -H "webhook-id: $event_id" \
  -H "webhook-timestamp: $timestamp" \
  -H "webhook-signature: v1,$signature" \
  --data-binary "@$tmp_dir/webhook.json")"
if [[ "$webhook_http_status" != "200" ]]; then
  echo "deployed Dodo entitlement webhook returned HTTP $webhook_http_status" >&2
  exit 1
fi

entitlement_http_status="$($curl_bin -sS -o "$tmp_dir/entitlement.json" -w '%{http_code}' --max-time 20 \
  -H "Cookie: $session_cookie_name=$access_token" \
  "$app_origin/account/entitlement")"
if [[ "$entitlement_http_status" != "200" ]] || ! rg -q '"sync_enabled"[[:space:]]*:[[:space:]]*true' "$tmp_dir/entitlement.json"; then
  echo "deployed entitlement boundary did not expose active sync (HTTP $entitlement_http_status)" >&2
  exit 1
fi

browser_run_args=(run --rm --network host)
if [[ -n "$browser_add_host" ]]; then
  browser_run_args+=(--add-host "$browser_add_host")
fi
"$docker_bin" "${browser_run_args[@]}" \
  --volume "$repo_dir:/workspace" \
  --workdir /workspace \
  --env TASK_SPACE_BROWSER_URL="$browser_url" \
  --env TASK_SPACE_SESSION_COOKIE="$access_token" \
  --env TASK_SPACE_AUTHENTICATED_ACCOUNT_ID="$account_id" \
  "$browser_image" \
  bash -lc 'python3 -m pip install --quiet playwright==1.47.0 && python3 scripts/browser-sync-smoke.py --url "$TASK_SPACE_BROWSER_URL" --session-cookie "$TASK_SPACE_SESSION_COOKIE" --isolated-profiles'

echo "WorkOS-authenticated acceptance passed: provider session, protected entitlement, and isolated browser convergence"
