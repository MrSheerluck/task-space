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

The API applies bounded rate limits per authenticated account and per client IP
for sync, metadata, entitlement, diagnostics, and billing routes. The auth
surface also has bounded client-IP budgets for sign-in, sign-up, verification,
password reset, refresh, callback, and logout. The Nginx proxy must remain the
trusted source of `X-Real-IP` and `X-Forwarded-For`; do not pass arbitrary
client-supplied forwarding headers through to the API.

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

3. Apply migrations through `0022_sync_mutation_claims.sql` before deploying
   code. This includes `0020_crdt_compaction.sql`,
   `0021_space_stable_ids.sql`, and the durable mutation-claim ledger in
   `0022_sync_mutation_claims.sql`. Migration 0021 must backfill and index the
   canonical UUID identity, and 0022 must backfill the long-lived mutation
   claims before any `/sync/v2` client is allowed to reconcile.
4. Deploy the server with reconciliation and cleanup workers enabled.
5. Confirm startup can reach PostgreSQL and WorkOS JWKS, then verify that Dodo
   test/live configuration matches the selected environment.
6. Configure the private Prometheus files under `deploy/observability`, mount
   the metrics token with mode `0600`, and verify `/healthz`, `/readyz`, and a
   successful metrics scrape before putting the API behind traffic.
7. Exercise an internal account through checkout, webhook delivery, expiry,
   and reactivation before staged rollout.

The migration set is additive apart from the intentional replacement of the
older `last_event_id` bound in migration 0019. Rollback means stopping the new
code and restoring the previous binary; do not drop the new columns or tables
while any new binary may still be running.

## Sync incident handling

- `SYNC_NOT_ENTITLED` or `SYNC_PAYMENT_PAUSED`: local changes remain in the
  browser. Check the entitlement row and provider subscription, then repair
  the provider state or wait for reconciliation. Never delete the local outbox.
- `MUTATION_ID_REUSED`: inspect the durable request hash and device
  diagnostics. The claim ledger intentionally outlives replay-event pruning.
  Do not manually replay with the same mutation ID; a new client mutation must
  be generated after resolving the conflict.
- `SYNC_CURSOR_RESET_REQUIRED`: this is expected after retention expiry. The
  client must list metadata and reconcile every non-deleted space; it must not
  discard local generations.
- `SYNC_PAYLOAD_TOO_LARGE`: preserve the local document, export it, and use
  compaction or a supported schema migration before retrying. Do not increase
  limits blindly.
- Rising 5xx/429 responses: check PostgreSQL locks, transaction latency,
  snapshot sizes, and the per-account/IP limiter before changing retry
  settings. Rate-limited API responses are `429` with `Retry-After`; auth
  endpoints use a 60-second retry hint and sync/billing endpoints use a
  shorter bounded retry hint. Do not disable the limiter to mask a capacity
  problem.

The browser indicator is authoritative for user communication: “offline”,
“sign-in expired”, and “payment attention” all mean edits are still local.

The API exposes `/healthz` for process liveness and `/readyz` for liveness
plus a PostgreSQL `SELECT 1` check. Both are `no-store`; use `/readyz` for
traffic readiness and keep `/internal/metrics` on the private management
network.

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

Create and verify a database backup before applying migrations:

```text
DATABASE_URL=postgres://... TASK_SPACE_BACKUP_DIR=/secure/backups \
  ./scripts/backup-postgres.sh
./scripts/verify-postgres-backup.sh /secure/backups/task-space-<timestamp>.dump
```

Restore only into an explicitly named maintenance target after stopping API
traffic. The restore script requires an explicit confirmation variable and
rechecks the checksum when one is present:

```text
TASK_SPACE_RESTORE_DATABASE_URL=postgres://... \
TASK_SPACE_RESTORE_CONFIRM=I_UNDERSTAND_RESTORE_IS_DESTRUCTIVE \
  ./scripts/restore-postgres.sh /secure/backups/task-space-<timestamp>.dump
```

After a restore, run migrations, the PostgreSQL sync smoke test, and a fresh
authenticated reconciliation before reopening traffic.

The repeatable local rehearsal performs that sequence without using the live
Compose volume as a restore target:

```text
make backup-restore-acceptance
```

It creates and checksum-verifies a custom-format dump, restores it into a
disposable PostgreSQL container, and runs the durable PostgreSQL sync test
against the restored database. Production restore rehearsal still requires the
maintenance-target and owner controls described above.

The local acceptance rehearsal backs up the live Compose database and restores
it into a separate disposable PostgreSQL target. It verifies the backup and
restore scripts without modifying `postgres_data`; production restores still
require the maintenance-target and owner approvals described above.

Run the real PostgreSQL sync smoke tests from the same network as the database
after migrations and before rollout. The repository gate starts a disposable
Rust runner on the Compose network because the database is intentionally not
published to the host:

```text
make postgres-acceptance
```

The test uses a random account and removes it when complete. It covers space
registration, stable-ID authorization, CRDT reconcile/retry, pull, durable
event replay, metadata mutation, and changed-payload rejection after replay
history pruning. The same gate also runs the HTTP acceptance test, which
exercises two independently authenticated account devices concurrently through
reconcile, verifies the merged pull, and checks CSRF and cross-account
isolation, followed by the multi-instance PostgreSQL notification test and
deterministic reconciliation transaction-abort/retry checks.

```text
TASK_SPACE_COMPOSE_NETWORK=deploy_default \
  ./scripts/run-postgres-acceptance.sh
```

With the metrics token configured, verify the protected scrape endpoint:

```text
curl -fsS -H "X-Metrics-Token: $TASK_SPACE_METRICS_TOKEN" \
  https://api.example.com/internal/metrics
```

Prometheus uses the standard Bearer form configured in
`deploy/observability/prometheus.yml`; manual checks may use
`X-Metrics-Token` as shown above. Load the alert rules from
`deploy/observability/task-space-alerts.yml` and exercise at least one alert
route in staging before enabling paging.

Before deployment, run the repository-level syntax gate (it uses a pinned
Prometheus image and an empty temporary credentials file):

```text
make observability-check
```

Run the local notification-path integration gate as well:

```text
make alert-delivery
```

It starts disposable Prometheus and Alertmanager containers, exposes a
synthetic readiness-failure metric, and verifies that the resulting
`TaskSpaceReadinessFailures` alert reaches a webhook receiver with its owner
and severity labels. This validates repository wiring only; production paging
still requires a staging/production receiver delivery and acknowledgement.

Before allowing traffic through a new origin or image, run the deployment
preflight. It checks database-backed readiness, HTML delivery, the unauthenticated
fail-closed session boundary, and optionally the private metrics scrape without
printing credentials:

```text
TASK_SPACE_ROLLOUT_URL=https://app.example.com \
  make rollout-preflight
```

Set `TASK_SPACE_METRICS_URL` and `TASK_SPACE_METRICS_TOKEN` as well when the
private metrics endpoint should be checked by the same preflight. A `401` from
`/auth/session` is expected and is considered a pass only when it carries
`SESSION_REQUIRED` and `Cache-Control: no-store`.

Then run PostgreSQL migration tests against both an empty database and a
legacy fixture, followed by browser tests for offline edits, lost responses,
SSE replay/reset, account switching, access-token expiry, downgrade/recovery,
refunds, disputes, and multi-tab coordination. These provider/database/chaos
checks cannot be proven by a source-only local run.

The repeatable disposable migration gate is:

```text
make migration-acceptance
```

For a larger timing rehearsal, override the synthetic fixture size without
touching the deployment database:

```text
make capacity-acceptance
```

It migrates a clean database, upgrades a pre-0020 legacy fixture, verifies the
stable-space-ID and mutation-claim backfills, and runs the durable PostgreSQL
sync acceptance test against both targets without touching the Compose data
volume.

The local browser regression gate can be run after building the frontend:

```text
make browser-sync
```

To exercise the deployed tunnel instead of the local frontend, set
`TASK_SPACE_BROWSER_URL` to the full `/app` origin:

```text
TASK_SPACE_BROWSER_URL=https://adnate-anesthetically-jenice.ngrok-free.dev/app \
  make browser-sync
```

or, when Chrome must be selected explicitly on macOS:

```text
TASK_SPACE_BROWSER_EXECUTABLE='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' \
  python3 scripts/browser-sync-smoke.py
```

This gate verifies three same-browser tabs editing while offline, IndexedDB
outbox recovery after a fresh-tab reload, normal BroadcastChannel delivery,
the storage-event fallback with BroadcastChannel and Web Locks disabled, and
the fail-closed repair state after a deliberately corrupt canonical snapshot.
It is a local transport/durability gate; run the authenticated Chrome,
Helium, WebKit, phone, provider, and chaos scenarios separately before
calling cross-device sync production-ready.

If the board reports `local data needs repair`, click `export local backup`
before restoring or clearing the device. That download is a recovery artifact:
it contains the active principal's raw IndexedDB snapshot, outbox, inbox, and
app-scoped local-storage metadata. The regular `export` file is a clean
workspace projection and is not sufficient for a corrupt-storage incident.

The same local gate can be run in WebKit after installing the Playwright
runtime with `python3 -m playwright install webkit`:

```text
python3 scripts/browser-sync-smoke.py --engine webkit
```

Exercise the same flow at a narrow mobile viewport with:

```text
make browser-mobile
```

This covers mobile layout, local durability, tab fallback, and repair export;
a physical phone still needs the authenticated same-account acceptance run.

Helium can be selected on macOS with:

```text
TASK_SPACE_BROWSER_EXECUTABLE='/Applications/Helium.app/Contents/MacOS/Helium' \
  python3 scripts/browser-sync-smoke.py
```

When the URL is a free ngrok endpoint, a fresh Helium profile may show
ngrok's one-time `ERR_NGROK_6024` warning page instead of the application.
Choose “Visit Site” once in that profile before judging the app render, or
use the smoke harness with the ngrok URL; it sends the documented
`ngrok-skip-browser-warning` header for test traffic. This interstitial is
served by ngrok before Nginx and is not an application blank-screen failure.

For an entitled authenticated acceptance run, supply a real session cookie
from the target browser profile. The script then requires the signed-in UI,
an `account:*` IndexedDB principal, and an empty outbox after reconnect:

```text
TASK_SPACE_SESSION_COOKIE='REDACTED' \
  python3 scripts/browser-sync-smoke.py \
  --url https://app.example.com/app
```

Use a disposable test account and never commit or paste the cookie into logs.

For a repeatable authenticated browser gate without provider credentials, use
the disposable acceptance server. It seeds one entitled account in the Compose
PostgreSQL database, serves the real frontend and protected sync router, and
uses an in-process verifier that exists only in the acceptance binary:

```text
make browser-authenticated
```

The gate runs two isolated Chromium profiles, WebKit regular/fallback, and the
390x844 mobile flow. It removes the generated account and spaces when the run
finishes. Run it only against the disposable local Compose database; it is not
a production authentication mode.

For a repeatable Dodo test-mode provider check, use:

```text
make provider-acceptance
```

This reads the local test-mode provider configuration without printing
credentials, verifies the configured product, creates a test checkout, sends a
correctly signed subscription webhook through the public origin, replays the
same webhook ID, verifies both HTTP responses, and removes the disposable
entitlement/event rows on exit. It verifies provider checkout and webhook
idempotency; it does not replace the separate WorkOS user-session, phone, or
production alert/chaos gates.

For the real WorkOS-authenticated deployment gate, use a configured test-mode
WorkOS and Dodo environment:

```text
make workos-authenticated
```

The gate creates a disposable verified WorkOS user, authenticates through the
real password session endpoint, posts a signed disposable entitlement webhook,
checks `/auth/session` and `/account/entitlement` through the public origin,
and runs two isolated browser profiles against the real frontend and protected
sync API. It deletes the WorkOS user and local account rows on exit. It never
prints session tokens or provider credentials. Set `TASK_SPACE_WORKOS_URL` and
`TASK_SPACE_WORKOS_BROWSER_URL` when the deployment origin is not the default
ngrok URL.

Keep disposable acceptance services off the production API DNS alias. The
production proxy must resolve `task-space-api` to exactly one API service; a
test container must use a distinct container name/alias (for example,
`task-space-chaos-acceptance`) or a private network. Duplicate service aliases
can split authenticated requests across different binaries and produce
intermittent protocol and convergence failures.

To test two isolated browser profiles against the same account, add
`--isolated-profiles`:

```text
TASK_SPACE_SESSION_COOKIE='REDACTED' \
  TASK_SPACE_BROWSER_EXECUTABLE='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' \
  python3 scripts/browser-sync-smoke.py \
  --url https://app.example.com/app \
  --isolated-profiles
```

This mode edits one profile offline while the other stays online, reconnects,
waits for both outboxes to drain, and reloads both profiles to verify the
merged document.

To test Chrome and Helium simultaneously against the same account, use:

```text
TASK_SPACE_SESSION_COOKIE='REDACTED' \
  python3 scripts/browser-sync-smoke.py \
  --url https://app.example.com/app \
  --cross-browser
```

Set `TASK_SPACE_HELIUM_EXECUTABLE` when Helium is installed outside its default
macOS path. This mode requires a disposable entitled session because separate
browsers have separate local stores; the server is the convergence boundary.

For the staged rollout, use this order and record the deployment revision with
each checkpoint:

1. Apply additive migrations and run the clean, legacy, sized migration gates.
2. Run the preflight, observability syntax gate, database acceptance, and
   authenticated internal-account browser run.
3. Enable the new client for internal accounts, then a small canary cohort,
   then bounded account-percentage cohorts. Keep the legacy endpoints and
   compatibility aliases available throughout the rollback window.
4. At every cohort, compare reconcile failures, cursor resets, mutation
   conflicts, pending-outbox age, cross-account denials, and account-safe
   convergence diagnostics against the previous cohort.
5. Freeze rollout and preserve diagnostics on any unexplained divergence,
   cross-account access, data loss, or unacknowledged-data growth. Roll back
   the application binary/client cohort while leaving additive schema objects
   in place; never drop columns, claims, snapshots, or event history during
   rollback.
6. Retire the legacy path only after the support window has elapsed with no
   unresolved divergence, after a restore rehearsal, and after the incident
   commander signs off on the final cleanup migration.

The guarded local restart gate exercises API and PostgreSQL restart recovery:

```text
make restart-chaos
```

It requires the explicit local confirmation in the Make target, checks
`/readyz`, and verifies the protected unauthenticated boundary remains `401`
with `Cache-Control: no-store` after each restart.

The abrupt local process-termination gate uses `SIGKILL` against the named
disposable acceptance API and PostgreSQL containers, starts them again, and
repeats the same readiness and protected-boundary assertions:

```text
make process-kill-chaos
```

This is evidence for local crash recovery only. Production transaction-boundary
kill experiments, alert delivery, and restore acceptance still require a
deployment change window and the production observability/rollback controls.
