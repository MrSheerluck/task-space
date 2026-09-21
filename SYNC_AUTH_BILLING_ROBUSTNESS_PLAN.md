# Sync, Auth, and Billing Robustness Plan

Status: local implementation hardening complete for the current numeric-ID schema; global-ID migration, production database/provider, and chaos verification pending  
Target branch: `sync`  
Scope: browser client, shared CRDT contracts, Rust API, PostgreSQL, WorkOS sessions, Dodo Payments, tests, operations, and rollout

Local code now covers protocol-v2 reconciliation, principal-scoped atomic
browser records, durable event replay/reset, metadata tombstones, server-side
entitlement enforcement, signed webhook inbox processing, provider-resource
billing reconciliation, refund/dispute lifecycle mapping, retry classification,
payload/snapshot limits, retention cleanup, explicit CORS/CSRF origins, and a
manual sync/recovery indicator. The remaining status is intentional: the
database/provider/chaos gates require an approved PostgreSQL, WorkOS, Dodo, and
browser test environment and cannot be proven from source-only checks.

Local verification on this branch is green: workspace checks, the WASM web
check, workspace tests, full workspace clippy with warnings denied, the
browser Chromium/WebKit smoke gates, PostgreSQL sync/HTTP acceptance tests,
backup/restore rehearsal, restart recovery, Prometheus configuration checks,
and `git diff --check`. Provider-backed browser/device, production alert
delivery, and long-running fault-injection evidence still require the
deployment acceptance environment described below.

The server now exposes bounded request and webhook counters through the
operator-token-protected `/internal/metrics` endpoint. Provider mode is
explicit, outbound provider response bodies are bounded, JWT `nbf` and
identity claims are checked, equal-timestamp billing events have a stable
provider-id tie-breaker, environment-matched Dodo API keys, and malformed
browser outbox records remain visible as actionable sync errors. WorkOS and
Dodo provider bodies are streamed with a hard 512 KiB limit. A production
scraper, dashboard, and alert policy still need to be installed and exercised
outside the source-only workspace.
The authenticated sync envelope is capped at 4 MiB so the JSON/base64
representation can carry the documented decoded update and state-vector caps
without turning the body parser into an unbounded allocation path.
Guest storage now uses a stable, validated `guest:<installation_id>` principal
with localStorage, sessionStorage, and cookie fallbacks; legacy unscoped guest
records are read only for one-time migration (with legacy outbox rows removed
only after server acknowledgement), and late account-namespace responses are
discarded after a principal switch.
The documented Dodo `pending` subscription state is explicitly non-entitling
and is represented through reconciliation instead of being treated as an
unknown retryable webhook.
Atomic CRDT snapshot writes also carry an in-tab ordering sequence and
generation guard, so an older asynchronous IndexedDB write or late reconcile
response cannot overwrite a newer local edit.
The JSON workspace cache uses the same per-principal write ordering for safe
reloads during rapid edit bursts.
All authenticated sync, SSE, entitlement, checkout, and portal routes now
share bounded per-account request windows with a capped key registry.
SSE errors now distinguish an offline browser from an expired session before
attempting refresh, keeping the indicator and retry policy truthful.
The browser also refreshes server-owned entitlement state independently of the
sync loop, allowing webhook-driven payment recovery to resume without reload.
SSE reconnects and retention-triggered cursor resets now schedule an
active-first pull plus an all-space reconciliation. Billing transitions are
serialized per account as well as per provider/event key, so first-ever and
concurrent events cannot overwrite one another; retries return a normal
duplicate result instead of a database uniqueness failure. Sparse
past-due/payment-failed events anchor the server grace window to the verified
event time when period metadata is absent.
Provider-resource recovery refuses to guess when legacy identifiers map to
multiple accounts; those signed webhooks are retained for reconciliation.

## 1. Outcome

Ship a local-first system in which:

- local editing never depends on network, authentication, or billing availability;
- an eligible account eventually converges across devices after arbitrary offline periods;
- retries, duplicate delivery, process restarts, missed events, and multiple server instances cannot lose or duplicate user changes;
- data from one account can never be shown or uploaded to another account accidentally;
- the server, not the browser, enforces authentication, entitlement, and space limits;
- billing changes take effect according to an explicit lifecycle policy based only on verified provider state;
- losing paid sync access pauses network synchronization but never deletes local data;
- every important failure is visible to the user and observable by operators.

This plan treats SSE as a latency optimization. Durable reconciliation is the source of correctness.

## 2. Non-negotiable invariants

1. **Local-first:** every accepted edit is committed locally before the UI reports it as saved.
2. **Self-healing:** the client can reconstruct an upload from its current CRDT document; correctness never depends on retaining every historical queue item.
3. **At-least-once transport, exactly-once effect:** requests may repeat, while idempotency keys and CRDT application prevent duplicate effects.
4. **Account isolation:** the authenticated principal comes only from verified server-side session data. Request bodies never select an account.
5. **Local account isolation:** guest data and each authenticated account use different IndexedDB namespaces.
6. **Server-side authorization:** every sync read, write, metadata operation, and event subscription checks the current entitlement.
7. **No event-stream dependency:** missing every SSE event must still converge through reconciliation.
8. **Deletion is durable:** deletes remain tombstones until every supported recovery window has passed; an ordinary retry cannot resurrect them.
9. **Billing is webhook-owned:** checkout return parameters never grant access.
10. **No destructive downgrade:** cancellation, payment failure, logout, or an expired session preserves local and server data according to retention policy.

## 3. Target architecture

### 3.1 Browser storage model

Replace the independent CRDT snapshot and per-update queue with one atomic record per principal and space:

```text
principal namespace
  guest:<installation_id>
  account:<account_id>
    workspace metadata cache
    space:<space_id>
      snapshot
      last_server_state_vector
      local_generation
      acknowledged_generation
      in_flight_request (request id + exact encoded payload)
      last_event_cursor
      last_error
```

A local edit updates the CRDT snapshot and increments `local_generation` in one IndexedDB transaction. A space is dirty whenever `local_generation != acknowledged_generation`.

The coordinator computes an upload from the current document relative to `last_server_state_vector`. If an older outbox entry is lost or corrupt, the next reconciliation still reconstructs the complete missing state.

### 3.2 Sync coordinator state machine

Use one coordinator instance per browser tab, with a cross-tab IndexedDB lease so only one tab performs network sync for a principal. Other tabs continue saving locally and notify the leader with `BroadcastChannel`.

States:

```text
disabled_guest
checking_session
disabled_not_entitled
starting
online_idle
syncing
offline_waiting
auth_refreshing
paused_auth
paused_billing
degraded
stopped
```

Required triggers:

- local generation changed;
- browser `online` event;
- SSE document or metadata hint;
- SSE open/reopen;
- tab visibility regained;
- periodic safety reconciliation;
- authentication refreshed;
- entitlement version changed;
- explicit user retry.

Only one reconcile may run for a space at a time. Different spaces may reconcile with bounded concurrency.

### 3.3 Reconciliation protocol

Introduce version 2 under `/sync/v2` rather than silently changing the existing contract.

For each space, the client sends:

- protocol version;
- stable device ID;
- random UUID request/mutation ID;
- the client's current state vector;
- an update containing everything after the last server-acknowledged state vector;
- the local generation captured when the request was created.

Under a PostgreSQL row lock, the server:

1. validates session, entitlement, identifiers, limits, and payload size;
2. checks the mutation ID and request hash;
3. loads the durable server document;
4. applies the client update;
5. computes the server update missing from the client's submitted state vector;
6. saves the merged snapshot and durable account event in the same transaction;
7. returns the missing update, merged server state vector, event cursor, and entitlement version.

The reconcile event cursor is informational rather than an acknowledgement of
all account events up to that sequence. A document response does not carry
intervening metadata events, so the browser only persists an SSE cursor after
the corresponding ordered event has been reconciled and its local projection
has been durably written.

The client then:

1. applies the returned update;
2. atomically saves the merged snapshot and acknowledged server state vector;
3. advances `acknowledged_generation` only to the generation captured by the request;
4. immediately reconciles again if a newer local generation exists.

When a response is lost, the client retries the exact stored in-flight payload with the same mutation ID. A duplicate request must return a response sufficient to finish reconciliation. A changed payload with a reused mutation ID returns a permanent conflict.

On first sync, the acknowledged state vector is empty, so the upload contains the complete local CRDT state. This fixes migration of existing offline data.

### 3.4 Durable event delivery

Add an account-scoped `sync_events` table. Every committed document or metadata mutation appends an event in the same transaction.

- SSE events carry `id`, `kind`, `resource_id`, and entitlement version.
- The browser persists the latest applied cursor.
- Reconnect supplies `Last-Event-ID` or an equivalent cursor query.
- The server replays retained events before switching to live delivery.
- PostgreSQL `LISTEN/NOTIFY` may wake instances, but the table is the durable source.
- If a cursor is older than retention, return `reset_required`; the client lists metadata and reconciles every space.
- Periodic reconciliation remains enabled even while SSE is healthy.

This supports multiple API instances and closes gaps caused by process crashes or broadcast-buffer overflow.

## 4. Identity and account boundaries

### 4.1 Globally unique entity IDs

Replace sequential `u64` IDs for spaces, notes, and groups with a serialized UUID/ULID newtype. Mutation IDs must use cryptographically random UUIDs and must not depend on a localStorage counter.

Migration before production:

1. create a backup/export of the existing local workspace;
2. assign new IDs to every space, note, and group;
3. rewrite `active_space_id`, `group_id`, and all tombstone references;
4. rebuild each Yrs document using the migrated keys;
5. commit the new workspace and CRDT records atomically;
6. mark the migration complete and make it idempotent;
7. test interruption before and after every transaction boundary.

If production server data already exists, add a one-off server migration and keep a temporary legacy-ID lookup table. Do not mix numeric and global IDs indefinitely.

### 4.2 Principal-scoped browser data

Legacy browser storage was global, which could upload account A's local board into account B after an account switch. The implementation now uses strict namespaces:

- guest workspace belongs to a generated installation principal;
- authenticated cache belongs to the exact server `account_id`;
- changing WorkOS user or organization switches namespaces;
- sign-out stops sync before clearing cookies and returns to the guest namespace;
- cached account data is never automatically copied into a different principal.

First sign-in must present an explicit, one-time choice:

- **Sync this local workspace to the account**; or
- **Keep it separate and open the account workspace**.

Record the decision so refreshes cannot repeat the migration.

### 4.3 WorkOS validation and session lifecycle

Create a single auth/session service used by every protected route.

- validate JWT signature, issuer, expiration/not-before, client/application identity, and the documented audience when applicable;
- keep JWKS refresh-on-unknown-key behavior and cache bounds;
- derive `account_id` only from verified user/organization claims;
- centralize access-token refresh in the browser with a single-flight lock so concurrent requests cannot race refresh-token rotation;
- retry one request after a successful refresh, never loop indefinitely;
- clear both cookies and transition to `paused_auth` when refresh is terminally rejected;
- change logout to POST, stop the coordinator/SSE first, clear local session markers, clear cookies, and revoke the provider session when supported;
- reject cookie-authenticated mutations without an allowed `Origin`/Fetch Metadata policy or a CSRF token;
- use `Secure`, `HttpOnly`, `SameSite=Lax` or stricter, host-only cookies, explicit lifetimes, and `Cache-Control: no-store`;
- test user-to-user and organization-to-organization switching in the same browser.

## 5. Server-side entitlement enforcement

Define a server-owned `SyncAccess` result:

```text
read_write
grace_read_write(until)
paused_not_entitled
paused_payment
paused_dispute
paused_expired
```

Every `/sync/v2` route performs the access check after authentication. The check returns the entitlement version so the client can detect stale state.

Recommended policy:

| Billing state | Sync access | Data action |
|---|---|---|
| Free / checkout pending | Paused | Keep local data; no cloud sync |
| Active Pro | Read/write | Normal sync |
| Active + cancel at period end | Read/write until period end | Show scheduled end date |
| `on_hold` / past due | Configurable 7-day read/write grace | Prompt payment-method update |
| Grace expired | Paused payment | Preserve dirty local generations |
| Immediate cancellation | Paused | Preserve server data for retention window |
| Expired / terminal failure | Paused expired | Preserve server data for retention window |
| Dispute opened/challenged | Paused dispute | Preserve data; flag for review |
| Dispute won | Recompute from current subscription | Restore if otherwise eligible |
| Full successful refund | Paused unless another valid subscription exists | Preserve data |
| Partial refund | Recompute from remaining purchased items | Never revoke unrelated access |

Make grace duration and server retention explicit configuration. Recommended initial retention is 90 days after access ends. Never delete on the webhook request path.

Space quotas are checked transactionally when creating a new, non-deleted synced space. Existing spaces are never deleted because a plan changes. When over a future lower quota, block new creation and continue syncing existing eligible spaces according to product policy.

Use stable machine-readable API errors:

| HTTP | Code | Client action |
|---|---|---|
| 401 | `SESSION_REQUIRED` / `SESSION_EXPIRED` | Single refresh attempt, then pause auth |
| 403 | `SYNC_NOT_ENTITLED` / `SYNC_PAYMENT_PAUSED` | Preserve dirty state and pause |
| 404 | `SPACE_NOT_FOUND` | Refresh metadata; do not discard local state |
| 409 | `MUTATION_ID_REUSED` / `METADATA_VERSION_CONFLICT` | Permanent conflict workflow |
| 413 | `SYNC_PAYLOAD_TOO_LARGE` | Chunk/compact or surface actionable error |
| 422 | `INVALID_UPDATE` / `UNSUPPORTED_PROTOCOL` | Quarantine/report; do not retry unchanged |
| 429 | `RATE_LIMITED` | Honor `Retry-After` |
| 503 | `SYNC_UNAVAILABLE` | Exponential retry with jitter |

## 6. Space metadata synchronization

Do not use registration as an implicit rename/restore operation. Add explicit idempotent operations:

- create space;
- rename space;
- archive/unarchive space;
- delete space;
- explicitly restore a deleted space.

Each space has a server `metadata_version`, `deleted_at`, and last operation ID. Mutations use optimistic concurrency. On conflict, the client fetches current metadata and applies these rules:

- delete wins over rename/archive;
- restore is a separate explicit operation and never occurs during registration;
- two renames produce a visible conflict or a documented server-order winner;
- archive/unarchive uses the latest accepted operation;
- a locally created offline space is uploaded with its global ID when access returns.

Metadata changes append durable account events. Reconnect and periodic refresh list all metadata changes since the stored cursor. Newly created remote spaces become visible without reloading.

## 7. CRDT behavior and schema evolution

- Keep note/group fields as independent Yrs map entries so unrelated concurrent field edits merge.
- Add tests for same-field conflicts and document the deterministic winner as expected CRDT behavior.
- Keep note/group deletion markers inside the CRDT; restoration must be an explicit newer operation.
- Never derive a remote document from the JSON projection. Hydrate or create the `SpaceDoc` first, then reconcile it.
- Load/reconcile every space after startup with bounded concurrency, prioritizing the active space.
- Validate protocol version and returned space ID on every response/event.
- Treat `has_more` or its v2 replacement as mandatory, not advisory.
- Add a schema migration registry. A client must either migrate a known old document or stop with `CLIENT_UPGRADE_REQUIRED`; it must never silently reinterpret unknown data.
- Compact snapshots and prune historical updates only after a durable snapshot and retention/cursor safety check.

## 8. Retry and failure policy

The coordinator uses exponential backoff with jitter, a maximum interval, and online/visibility triggers that can wake it early.

Classify failures:

- **Transient:** offline, timeout, connection reset, 429, 502, 503, 504. Retry without changing the in-flight payload.
- **Authentication:** 401. Run one single-flight refresh, recreate SSE, and retry once.
- **Billing:** entitlement 403. Stop network attempts until entitlement refresh or a billing event changes the version.
- **Permanent payload:** invalid version/update/identifier or mutation conflict. Preserve the local snapshot, quarantine the request metadata, show a recoverable error, and emit diagnostics.
- **Missing space:** refresh metadata; create only if the local metadata outbox proves this client owns an unacknowledged creation.

Add request timeouts and cancellation. Never allow an older response to overwrite a newer local generation or a newly selected principal.

## 9. Billing and webhook hardening

### 9.1 Webhook ingestion

Keep signature verification over the exact raw request bytes and the timestamp freshness check. Expand ingestion to a durable inbox:

1. verify signature and required headers;
2. store the verified raw event, hash, provider, webhook ID, type, received time, and processing state;
3. enforce a unique `(provider, webhook_id)` key;
4. return 2xx only after durable persistence;
5. process asynchronously or in a bounded transaction;
6. update the processing record and entitlement transition atomically;
7. retry failed processing without losing the idempotency claim.

Dodo does not guarantee webhook ordering. Do not rely only on arrival order. Store provider resource timestamps/status and, for ambiguous or contradictory transitions, retrieve the current subscription before changing access. Add a scheduled reconciliation job that compares active local subscriptions with Dodo.

Unknown signed event types should be stored and acknowledged, then surfaced in metrics rather than silently disappearing.

### 9.2 Subscription lifecycle corrections

Handle all relevant states distinctly:

- `pending`: no entitlement;
- `active`: grant/continue access, including trials;
- `on_hold`: recoverable payment failure and grace policy;
- `cancelled` with `cancel_at_next_billing_date=true`: retain access until `next_billing_date`;
- immediate `cancelled`: revoke immediately;
- `failed`: terminal creation/mandate failure;
- `expired`: revoke at end of term;
- `subscription.renewed`: extend access and period metadata;
- `subscription.plan_changed`: inspect product ID, current status, and cancellation flag rather than assuming upgrade;
- `subscription.unpaused`/active recovery: restore access and wake paused clients.

The adapter now splits scheduled cancellation from immediate cancellation and
populates a server-owned `access_until`; provider/database lifecycle fixtures
still need to verify every transition.

### 9.3 Refunds and disputes

Subscribe to and store required refund/dispute events.

- `refund.succeeded`: retrieve the payment, determine full versus partial refund, and recompute entitlement from remaining valid purchases/subscriptions;
- `refund.failed`: leave entitlement unchanged and alert operations;
- `dispute.opened` or `dispute.challenged`: place sync access on dispute hold and gather evidence;
- `dispute.won`: remove the hold and recompute access;
- `dispute.lost`, `accepted`, or `expired`: keep access paused;
- `dispute.cancelled`: do not treat as a win; require reconciliation;
- resolve dispute customer/account through the provider resource because dispute webhook payloads may not contain a customer.

Do not delete synchronized data when access is revoked.

### 9.4 Checkout and portal

- create checkout only for an authenticated principal;
- derive account metadata server-side;
- validate product/interval using an allowlist;
- add a per-account idempotency key or pending-checkout record to avoid duplicate subscriptions;
- rate-limit checkout creation;
- never grant access from return URL parameters;
- continue polling entitlement after return, but show a bounded “payment processing” state;
- create portal sessions only for the customer ID associated with the authenticated account;
- audit checkout, portal, and entitlement transitions without logging tokens or full provider payloads.

### 9.5 Environment safety

- require an explicit `test_mode` or `live_mode`; fail closed on an invalid value;
- validate that API-key prefix, product IDs, webhook secret, and return URLs match the selected environment;
- keep test and live product IDs/webhook secrets separate;
- require HTTPS webhook and return URLs in production;
- add a startup readiness check for WorkOS JWKS, PostgreSQL migrations, and billing configuration.

## 10. Database work

Add migrations in small, reversible steps:

1. globally unique entity IDs and temporary legacy-ID mapping;
2. principal/account metadata versioning and durable space tombstones;
3. CRDT document acknowledged state/version and bounded update history;
4. durable `sync_events` with account/cursor indexes;
5. mutation request hash and replay information;
6. webhook inbox/processing state;
7. provider subscription/payment/refund/dispute references;
8. entitlement `access_mode`, `access_until`, `retention_until`, reason, and version;
9. checkout idempotency records;
10. cleanup/reconciliation job state.

The current additive set also stores the last provider event id as a
deterministic tie-break for equal-timestamp webhook transitions; migrations
`0018_billing_event_tie_break.sql` and `0019_billing_identifier_constraints.sql`
must be deployed before code that reads or writes those fields.

Database constraints must enforce non-empty IDs, payload bounds where practical, unique idempotency keys, valid state enums, non-negative quotas, and foreign-key ownership. Test every migration against an empty database and a legacy fixture.

## 11. User-visible behavior

Show one compact sync indicator with actionable detail:

- local only;
- checking account;
- syncing;
- synced at `<time>`;
- offline—changes saved locally;
- sign-in expired—changes saved locally;
- payment attention—changes saved locally;
- sync paused—upgrade/reactivate;
- sync error—retry/export diagnostics.

Never label data “synced” merely because IndexedDB saved successfully. Track local-save state and server-acknowledged state separately.

Provide:

- manual “sync now”;
- pending-space/change count;
- safe export at all times;
- conflict messaging for metadata conflicts;
- explicit account/guest workspace migration choice;
- billing recovery link for `on_hold`;
- period-end date for scheduled cancellation.

## 12. Security and abuse controls

- request-body limits for JSON, base64 updates, and space names;
- per-account and per-IP rate limits for auth, sync, checkout, and webhook endpoints;
- maximum active spaces and reasonable document size limits;
- bounded concurrency and transaction timeouts;
- strict CORS plus CSRF/Origin protection for cookie-authenticated writes;
- sanitized API errors and structured internal logs;
- no access/refresh tokens, raw passwords, or full sensitive webhook bodies in logs;
- backup encryption, least-privilege database credentials, and secret rotation procedures;
- account data export/deletion workflow with a delayed, auditable purge;
- fuzz/property tests for malformed base64, Yrs updates, JSON, cookies, JWT claims, and webhook headers.

## 13. Observability and operations

Metrics:

- reconcile attempts/success/failure/latency by failure class;
- dirty spaces and oldest dirty age reported by clients without document content;
- SSE connections, reconnects, replay count, cursor gaps, and reset-required count;
- mutation duplicates/conflicts and payload sizes;
- PostgreSQL lock/transaction latency and snapshot/update-log size;
- auth refresh success/failure and session rejection reasons;
- webhook verification failures, inbox lag, processing retries, unknown event types, and stale events;
- entitlement changes by reason;
- checkout/API failures and subscription reconciliation drift.

Alerts:

- webhook failure or processing lag beyond provider retry windows;
- growing count/age of dirty clients;
- elevated 401/403/409/5xx sync responses;
- SSE reconnect spike;
- database update-log or snapshot growth;
- mismatch between active provider subscriptions and local entitlements;
- test/live configuration mismatch.

Add correlation IDs spanning checkout, webhook, entitlement transition, and sync denial without exposing sensitive values.

## 14. Test matrix

### 14.1 Core and property tests

- first full snapshot to empty server;
- incremental update after acknowledged vector;
- duplicate mutation with identical payload;
- duplicate ID with changed payload;
- randomized two- and three-replica edit ordering until convergence;
- concurrent edits to different fields and the same field;
- concurrent create/create, edit/delete, delete/restore, archive/rename;
- large documents, empty updates, invalid updates, unknown schema versions;
- globally unique IDs across many simulated devices.

### 14.2 PostgreSQL/API integration tests

- atomic document + mutation + event commit;
- crash/rollback at every transaction boundary;
- concurrent pushes to the same space;
- duplicate requests from different API instances;
- cross-account read/write/list/event denial;
- free/past-due/expired/disputed account denial;
- active and grace-period access;
- max-space transaction races;
- event replay, cursor retention expiry, and reset-required;
- server restart and multi-instance delivery;
- payload/rate limits and stable error codes;
- migration from legacy database fixtures.

### 14.3 Browser end-to-end tests

- guest editing with no API available;
- first sign-in uploads all existing notes/groups/tombstones;
- second device downloads all spaces;
- continuous online edit appears remotely without reload;
- offline edits on both devices converge after reconnect;
- browser closes after local commit but before upload;
- server commits but response is lost;
- SSE misses events or disconnects for hours;
- inactive space changes and newly created remote spaces;
- create/rename/archive/delete/restore while offline;
- IndexedDB write failure, quota exhaustion, and corrupt legacy records;
- two tabs editing simultaneously and leader handoff;
- access-token expiry during push and SSE;
- refresh-token expiry, logout, user switch, and organization switch;
- downgrade while online, reactivation with queued changes, and no data loss;
- stale response arriving after principal or active-space change.

### 14.4 Billing tests

- checkout success, decline, abandonment, duplicate click, and provider outage;
- webhook missing headers, bad signature, stale timestamp, malformed JSON;
- duplicate webhook, same ID/different body, out-of-order and equal-time events;
- monthly/yearly products and unrelated product ignored;
- pending, active, renewed, plan changed, on-hold, recovered, scheduled cancellation, immediate cancellation, failed, and expired;
- checkout return before webhook and webhook before browser return;
- full/partial refund success and refund failure;
- every dispute transition, including customer lookup failure and retry;
- reconciliation repairs a missing webhook or contradictory local state;
- strict separation of test/live keys, products, customers, webhooks, and URLs.

### 14.5 Chaos and longevity tests

- randomized network drops, duplicate/reordered responses, and latency;
- API and PostgreSQL restart during reconciliation;
- multiple server instances receiving notifications concurrently;
- 30+ day offline device returning after event retention expires;
- high-frequency edits and hundred-space accounts;
- snapshot compaction while clients hold old state vectors;
- clock skew on browsers and servers;
- backup restore followed by client reconciliation.

## 15. Implementation sequence

### Phase 0 — Decisions and safety net

- Confirm grace period, retention duration, refund/dispute policy, and metadata conflict UX.
- Freeze v1 behavior and add current failure reproductions as tests.
- Add database backup/restore instructions and feature flags.

Exit: every known critical failure has a failing automated test or a documented manual fixture.

### Phase 1 — IDs and local account isolation

- Introduce global ID newtypes and idempotent local migration.
- Namespace IndexedDB by guest/account principal.
- Add explicit guest-to-account adoption flow.
- Add cross-tab coordinator lease.

Exit: two accounts and two tabs cannot collide or share data unintentionally.

### Phase 2 — Durable client record and coordinator

- Replace fragile update queue with snapshot + acknowledged vector + generations.
- Make snapshot/dirty state atomic.
- Implement state machine, retry classes, timeouts, and visible status.

Exit: a crash at any client-side point leaves a recoverable dirty document.

### Phase 3 — Sync v2 reconciliation

- Add contracts in `crates/core`.
- Implement PostgreSQL transactional reconcile and mutation replay.
- Bootstrap full pre-existing local state.
- Reconcile all spaces, active first.

Exit: first sync, continuous sync, offline sync, and lost-response tests converge.

### Phase 4 — Metadata and durable events

- Add explicit metadata APIs/tombstones/version conflicts.
- Add durable account event table, replay cursor, and multi-instance wakeups.
- Make SSE a hint and add periodic safety reconcile.

Exit: create/rename/archive/delete/restore and missed-event scenarios work without reload.

### Phase 5 — Auth hardening

- Centralize authentication/authorization middleware.
- Add strict claim validation, single-flight refresh, CSRF/Origin checks, and POST logout.
- Stop/restart coordinator on principal changes.

Exit: all auth lifecycle and cross-principal tests pass.

### Phase 6 — Billing enforcement and lifecycle

- Add server `SyncAccess`, access/grace/retention fields, and quota checks.
- Correct cancellation/on-hold mappings.
- Add durable webhook inbox and subscription reconciliation.
- Add refund/dispute handling and checkout idempotency.

Exit: browser tampering cannot bypass entitlement, and every billing transition produces the expected access mode.

### Phase 7 — Limits, compaction, and operations

- Add payload/rate/storage limits.
- Add safe CRDT history compaction and event retention.
- Add metrics, alerts, dashboards, and runbooks.

Exit: load, chaos, backup/restore, and retention-expiry tests pass.

### Phase 8 — Rollout

- Deploy schema additions before code that requires them.
- Enable v2 for internal/test accounts, then staged percentages.
- Dual-read telemetry only where needed; avoid dual-writing documents unless proven safe.
- Keep v1 rollback available during the observation window.
- Block general release until convergence, billing enforcement, and cross-account isolation gates pass.
- Remove v1 and temporary migration tables only after the support window and verified backups.

## 16. Suggested pull-request slices

1. Failure-reproduction tests and protocol-v2 types.
2. Global ID model and local migration.
3. Principal-scoped IndexedDB repositories.
4. Atomic CRDT sync record and coordinator state machine.
5. PostgreSQL reconcile transaction and idempotent replay.
6. Client reconcile/bootstrap/all-space recovery.
7. Metadata operations and tombstones.
8. Durable event replay and multi-instance SSE.
9. Auth middleware, refresh coordination, CSRF, and logout lifecycle.
10. Server entitlement policy and quota enforcement.
11. Dodo lifecycle corrections and durable webhook inbox.
12. Refund/dispute/reconciliation jobs and checkout idempotency.
13. Limits, compaction, observability, and operational runbooks.
14. Full browser chaos suite and staged-rollout controls.

Each pull request must include migrations, rollback notes, tests, user-visible error behavior, and metrics for its failure paths.

## 17. Definition of done

Sync is complete only when all of the following are true:

- all test-matrix cases are automated or have an approved manual runbook;
- existing guest data fully appears on a second device after first-time adoption;
- two devices can edit offline and converge without reload or data loss;
- no account, organization, or browser-profile switch can leak local data;
- all sync routes reject ineligible accounts server-side;
- cancellation, on-hold recovery, expiry, refunds, and disputes match the documented policy;
- SSE loss, API restarts, PostgreSQL restarts, and multi-instance deployment self-heal;
- dirty local data always survives auth/billing/network failures;
- metadata operations and deletions never resurrect accidentally;
- payload, quota, and abuse limits are enforced and observable;
- backup restore and rollback drills succeed;
- production has dashboards, alerts, reconciliation jobs, and owner-approved runbooks;
- v1 is retired only after migration and staged-rollout evidence shows no unresolved divergence.
