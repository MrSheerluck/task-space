# Sync, auth, and billing operations runbook

This repository intentionally keeps synchronization correct when transport,
session, billing, or one API instance is unavailable. Production correctness
still depends on applying migrations and running the provider/database checks
below before enabling the `sync` branch for real accounts.

## Configuration gate

Set `TASK_SPACE_ALLOWED_ORIGINS` to a comma-separated list of exact browser
origins (scheme, host, and optional port; no paths). In live mode the Dodo
return URL and webhook endpoint must use HTTPS. Keep test and live Dodo keys,
product IDs, webhook secrets, and databases separate. Do not log `.env` or
provider payloads.

`DODO_PAYMENTS_ENVIRONMENT` is mandatory and must be exactly `test_mode` or
`live_mode`; the server intentionally refuses to choose a default.

Set `TASK_SPACE_METRICS_TOKEN` to a separate operator-only secret to enable
GET `/internal/metrics`; leave it unset to disable the endpoint. Scrape it
over the private management path and never put account IDs, webhook payloads,
or tokens into metric labels.

The server defaults sync-event and billing-history retention to 90 days. Tune
`SYNC_EVENT_RETENTION_SECONDS` only after confirming the longest supported
offline recovery window. Entitlement retention is intentionally independent
from event retention and must be reviewed before changing it.

## Deploy order

1. Back up PostgreSQL and record the migration version.
2. Before applying `0019_billing_identifier_constraints.sql`, check for
   existing oversized provider identifiers and stop for remediation if any
   rows are returned:

   ```sql
   SELECT provider, provider_event_id, account_id
   FROM billing_events
   WHERE length(provider) > 256
      OR length(provider_event_id) > 256
      OR length(account_id) > 256;
   ```

   Also check the inbox identifiers covered by the same migration and the
   closed entitlement states added by `0015_billing_constraints.sql`:

   ```sql
   SELECT provider, webhook_id
   FROM billing_webhook_inbox
   WHERE length(provider) NOT BETWEEN 1 AND 256
      OR length(webhook_id) NOT BETWEEN 1 AND 256
      OR length(event_type) NOT BETWEEN 1 AND 256
      OR octet_length(payload) > 524288;

   SELECT provider, provider_event_id, provider_payment_id
   FROM billing_events
   WHERE provider_payment_id IS NOT NULL
     AND length(provider_payment_id) > 256;

   SELECT account_id, status, access_mode
   FROM billing_entitlements
   WHERE status NOT IN ('free', 'pending', 'active', 'past_due', 'canceled', 'ended')
      OR access_mode NOT IN (
           'read_write', 'grace_read_write', 'paused_not_entitled',
           'paused_payment', 'paused_dispute', 'paused_expired'
         );
   ```

3. Apply migrations through `0019_billing_identifier_constraints.sql` before deploying code.
4. Deploy the server with reconciliation and cleanup workers enabled.
5. Confirm startup can reach PostgreSQL and WorkOS JWKS, then verify that Dodo
   test/live configuration matches the selected environment.
6. Exercise an internal account through checkout, webhook delivery, expiry,
  and reactivation before staged rollout.

The migration set is additive apart from the intentional replacement of the
older `last_event_id` bound in migration 0019. Rollback means stopping the new
code and restoring the previous binary; do not drop the new columns or tables
while any new binary may still be running.

## Sync incident handling

- `SYNC_NOT_ENTITLED` or `SYNC_PAYMENT_PAUSED`: local changes remain in the
  browser. Check the entitlement row and provider subscription, then repair
  the provider state or wait for reconciliation. Never delete the local outbox.
- `MUTATION_ID_REUSED`: inspect the request hash and device diagnostics. Do
  not manually replay with the same mutation ID; a new client mutation must be
  generated after resolving the conflict.
- `SYNC_CURSOR_RESET_REQUIRED`: this is expected after retention expiry. The
  client must list metadata and reconcile every non-deleted space; it must not
  discard local generations.
- `SYNC_PAYLOAD_TOO_LARGE`: preserve the local document, export it, and use
  compaction or a supported schema migration before retrying. Do not increase
  limits blindly.
- Rising 5xx/429 responses: check PostgreSQL locks, transaction latency,
  snapshot sizes, and the per-account limiter before changing retry settings.

The browser indicator is authoritative for user communication: “offline”,
“sign-in expired”, and “payment attention” all mean edits are still local.

## Billing incident handling

Every verified webhook is persisted in `billing_webhook_inbox` before it can
change an entitlement. `processing` rows older than 15 minutes are returned to
`pending` by the cleanup worker. `needs_reconciliation` rows require provider
resource lookup or operator review; they must not be acknowledged as an
entitlement grant without an account mapping.

`billing_events` is the provider-event idempotency ledger. If the same provider
event ID arrives with different bytes, stop processing and investigate a
provider or signing problem. Out-of-order events are retained but cannot roll
back a newer entitlement timestamp.

For refunds and disputes, resolve the provider subscription/customer to the
server account, retrieve current provider state, and recompute entitlement.
Partial refunds preserve unrelated access; full refunds and lost disputes
pause sync but retain data through the configured retention window.

## Verification commands

Run locally before a rollout:

```text
cargo fmt --all
cargo check --workspace
cargo check -p task-web --target wasm32-unknown-unknown
cargo test --workspace --quiet
git diff --check
```

With the metrics token configured, verify the protected scrape endpoint:

```text
curl -fsS -H "X-Metrics-Token: $TASK_SPACE_METRICS_TOKEN" \
  https://api.example.com/internal/metrics
```

Then run PostgreSQL migration tests against both an empty database and a
legacy fixture, followed by browser tests for offline edits, lost responses,
SSE replay/reset, account switching, access-token expiry, downgrade/recovery,
refunds, disputes, and multi-tab coordination. These provider/database/chaos
checks cannot be proven by a source-only local run.
