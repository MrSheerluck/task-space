#!/usr/bin/env bash
set -euo pipefail

# Exercise the configured Dodo test-mode account without printing credentials or
# provider payloads. This is intentionally a provider/webhook gate, not a
# substitute for the separate WorkOS-authenticated browser gate.

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${TASK_SPACE_PROVIDER_ENV_FILE:-$repo_dir/.env}"
provider_url="${TASK_SPACE_PROVIDER_URL:-https://taskspace-dev.smbl.dev}"
test_account="${TASK_SPACE_PROVIDER_TEST_ACCOUNT:-task-space-provider-acceptance}"
db_network="${TASK_SPACE_PROVIDER_DB_NETWORK:-deploy_default}"
db_password="${POSTGRES_PASSWORD:-change-me-local-only}"
tmp_dir="$(mktemp -d -t task-space-provider-acceptance.XXXXXX)"

cleanup() {
  if command -v docker >/dev/null 2>&1; then
    docker run --rm --network "$db_network" \
      --env PGPASSWORD="$db_password" \
      postgres:17-alpine \
      psql --host postgres --username taskspace --dbname taskspace \
      --command "DELETE FROM spaces WHERE account_id = '$test_account'; DELETE FROM billing_events WHERE account_id = '$test_account'; DELETE FROM billing_entitlements WHERE account_id = '$test_account'; DELETE FROM billing_webhook_inbox WHERE webhook_id LIKE 'evt-task-space-provider-acceptance-%';" \
      >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

if [[ ! -f "$env_file" ]]; then
  echo "provider acceptance requires $env_file" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$env_file"
set +a

if [[ "${DODO_PAYMENTS_ENVIRONMENT:-}" != "test_mode" ]]; then
  echo "provider acceptance requires DODO_PAYMENTS_ENVIRONMENT=test_mode" >&2
  exit 1
fi
for variable in DODO_PAYMENTS_API_KEY DODO_PAYMENTS_WEBHOOK_KEY \
  DODO_PRO_MONTHLY_PRODUCT_ID DODO_PAYMENTS_RETURN_URL; do
  if [[ -z "${!variable:-}" ]]; then
    echo "provider acceptance requires $variable" >&2
    exit 1
  fi
done

curl_bin="$(command -v curl || true)"
openssl_bin="$(command -v openssl || true)"
base64_bin="$(command -v base64 || true)"
xxd_bin="$(command -v xxd || true)"
if [[ -z "$curl_bin" || -z "$openssl_bin" || -z "$base64_bin" || -z "$xxd_bin" ]]; then
  echo "provider acceptance requires curl, openssl, base64, and xxd" >&2
  exit 1
fi

product_code="$($curl_bin -sS -o "$tmp_dir/product.json" -w '%{http_code}' \
  --max-time 20 \
  -H "Authorization: Bearer $DODO_PAYMENTS_API_KEY" \
  "https://test.dodopayments.com/products/$DODO_PRO_MONTHLY_PRODUCT_ID")"
if [[ "$product_code" != "200" ]]; then
  echo "Dodo test product lookup returned HTTP $product_code" >&2
  exit 1
fi

idempotency_key="task-space-provider-acceptance-$(date +%s)-$$"
checkout_code="$($curl_bin -sS -o "$tmp_dir/checkout.json" -w '%{http_code}' \
  --max-time 20 \
  -X POST "https://test.dodopayments.com/checkouts" \
  -H "Authorization: Bearer $DODO_PAYMENTS_API_KEY" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $idempotency_key" \
  --data "{\"product_cart\":[{\"product_id\":\"$DODO_PRO_MONTHLY_PRODUCT_ID\",\"quantity\":1}],\"metadata\":{\"account_id\":\"$test_account\"},\"return_url\":\"$DODO_PAYMENTS_RETURN_URL\"}")"
if [[ "$checkout_code" != "200" ]] || ! rg -q '"checkout_url"' "$tmp_dir/checkout.json"; then
  echo "Dodo test checkout did not return a checkout URL (HTTP $checkout_code)" >&2
  exit 1
fi

event_id="evt-task-space-provider-acceptance-$(date +%s)-$$"
timestamp="$(date +%s)"
printf '{"type":"subscription.active","data":{"product_id":"%s","subscription_id":"sub_task_space_acceptance","customer":{"customer_id":"cus_task_space_acceptance"},"metadata":{"account_id":"%s"},"next_billing_date":"2030-01-01T00:00:00Z","cancel_at_next_billing_date":false}}' \
  "$DODO_PRO_MONTHLY_PRODUCT_ID" "$test_account" > "$tmp_dir/webhook.json"
printf '%s.%s.' "$event_id" "$timestamp" > "$tmp_dir/signing-input"
cat "$tmp_dir/webhook.json" >> "$tmp_dir/signing-input"
secret_b64="${DODO_PAYMENTS_WEBHOOK_KEY#whsec_}"
secret_hex="$($base64_bin --decode <<< "$secret_b64" | $xxd_bin -p -c 100000)"
signature="$($openssl_bin dgst -sha256 -mac HMAC -macopt "hexkey:$secret_hex" -binary "$tmp_dir/signing-input" | $base64_bin | tr -d '\n')"

webhook_code="$($curl_bin -sS -o "$tmp_dir/webhook-response.json" -w '%{http_code}' \
  --max-time 20 \
  -X POST "$provider_url/webhooks/dodo" \
  -H 'Content-Type: application/json' \
  -H "webhook-id: $event_id" \
  -H "webhook-timestamp: $timestamp" \
  -H "webhook-signature: v1,$signature" \
  --data-binary "@$tmp_dir/webhook.json")"
duplicate_code="$($curl_bin -sS -o "$tmp_dir/webhook-duplicate-response.json" -w '%{http_code}' \
  --max-time 20 \
  -X POST "$provider_url/webhooks/dodo" \
  -H 'Content-Type: application/json' \
  -H "webhook-id: $event_id" \
  -H "webhook-timestamp: $timestamp" \
  -H "webhook-signature: v1,$signature" \
  --data-binary "@$tmp_dir/webhook.json")"
if [[ "$webhook_code" != "200" || "$duplicate_code" != "200" ]]; then
  echo "Dodo webhook acceptance failed (first HTTP $webhook_code, duplicate HTTP $duplicate_code)" >&2
  exit 1
fi

echo "provider acceptance passed: product HTTP $product_code, checkout HTTP $checkout_code, webhook HTTP $webhook_code, duplicate HTTP $duplicate_code"
