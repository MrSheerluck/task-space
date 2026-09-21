# Local-First Sync Master Plan

Status: authoritative architecture and implementation plan; local implementation and acceptance gates complete, external deployment gates pending  
Scope: local persistence, multi-tab synchronization, multi-browser and multi-device synchronization, offline recovery, CRDT conflict behavior, authentication, entitlement handling, observability, migration, testing, and rollout  
Relationship to existing documents: this is the authoritative plan for sync behavior. `SYNC_AUTH_BILLING_ROBUSTNESS_PLAN.md` and `SYNC_AUTH_BILLING_RUNBOOK.md` remain supporting references for detailed auth, billing, and operations work.

Implementation checkpoint (2026-09-10): the canonical CRDT/IndexedDB
pipeline, actual-update tab channel, coordinator fencing, `/sync/v2`
reconciliation, durable metadata-only space manifests, durable events/SSE
recovery, metadata tombstones, explicit schema negotiation, canonical UUID
entity migration, manifest-first outbox ordering, stable-identity wire checks,
retention-gated compaction,
diagnostics, and request correlation baseline are implemented in the
worktree. The outbox worker also honors bounded server `Retry-After` delays
while preserving its frozen mutation payload, and session refresh is serialized
across tabs with a shared lock plus a post-lock session check. The CRDT text
projection bridge preserves concurrent insertions even when a stale tab submits
a full replacement, and manifest failure no longer registers a blank local
placeholder as a remote space. Auth/session error responses are explicitly
`no-store`, and sync reads/space registration retry one coordinated session
refresh before pausing. Cross-tab snapshot mirrors now carry the originating
shared local generation; unversioned direct saves can only initialize an empty
record, while normal local edits are committed through the atomic outbox
transaction. The queue returns the generation actually assigned by IndexedDB,
so a tab that loses a write race broadcasts the committed generation rather
than its stale tab-local counter. This prevents a late per-tab write from
clobbering a newer sibling tab snapshot. The Definition of Done
below remains open until the browser/device/chaos gates and deployment
acceptance evidence are completed; the compatibility and runtime blockers are
called out in the handoff rather than being treated as passing tests.

Implementation checkpoint (2026-09-11): the observability contract now includes
bounded HTTP and reconciliation latency histograms, request/update/state-vector
size counters, categorized auth/CSRF/entitlement/mutation/metadata/cursor
failures, duplicate-or-no-event reconciliation counts, and retention/compaction
failure and deletion counters. The CRDT suite also includes deterministic
randomized duplicate/reordered delivery schedules across three replicas. These
are repository-level hardening and verification improvements; they do not close
the separately identified real-provider, phone, production-alert, or
production-process-termination-chaos gates.

Coordinator-liveness checkpoint (2026-09-11): Web Locks are now paired with a
renewable expiring local-storage lease. A suspended tab can no longer hold the
only coordinator indefinitely: another tab can take over after lease expiry,
and a resumed stale holder detects the fencing epoch, stops coordinating, and
releases its Web Lock. The browser harness now exercises both the blocked
unexpired-holder case and this takeover path in Chromium and WebKit.

Authentication-liveness checkpoint (2026-09-11): session refresh now uses an
expiring cross-tab storage lease first, with bounded `ifAvailable` Web Lock
fallback when storage is unavailable. A backgrounded tab can no longer leave
other tabs queued behind an unbounded refresh lock; the lease or fallback
attempt has a finite takeover/timeout path while preserving single-flight
refresh for normal profiles. The browser smoke harness now seeds an expired
refresh lease during bootstrap; the deployed Helium run passes with that
takeover path exercised.

Projection-race checkpoint (2026-09-11): the UI persistence effect now
compares the rendered board with the current canonical `SpaceDoc` before
deciding that a remote projection needs no outbox work. If a user edits during
the same reactive turn as a sibling/server update, only that local delta is
diffed against the latest CRDT and queued; the remote-only path remains
queue-free. A regression test covers both branches, and the rebuilt deployed
Helium and WebKit gates pass.

Namespace-liveness checkpoint (2026-09-11): account workspace hydration now
checks both the active principal and the current signed-in account after each
asynchronous IndexedDB boundary and before installing or persisting the
workspace. A late response from an account that was switched away cannot
paint or save the old account's projection. The guard is covered by native and
Wasm warnings-denied checks, the full workspace tests, and fresh public ngrok
Chromium/WebKit smoke runs.

Deletion-convergence checkpoint (2026-09-11): CRDT schema 4 now records
delete and explicit-restore lifecycle intents with their causal state. A
concurrent stale restore cannot clear a delete tombstone, while a restore
created after observing the delete can revive the entity. Legacy scalar
tombstones receive lifecycle anchors during migration, and the invariant is
covered by a repeated randomized-client regression for notes; groups use the
same lifecycle path.

Server-snapshot migration checkpoint (2026-09-11): legacy CRDT snapshots are
now materialized durably during API startup, and the pull/reconcile paths also
persist an upgrade when they encounter one. Row locking makes the backfill
safe during rolling startup, and repeated reads no longer create ephemeral
schema-migration operations. The representation upgrade is delivered through
the normal state-vector response without creating a duplicate user mutation
or replay event.
The rebuilt API then restarted cleanly; its second startup emitted no
additional migration count and the public `/readyz` probe returned ready,
confirming the backfill is idempotent on the deployed database.

Replay-log hygiene checkpoint (2026-09-11): a fresh mutation that repeats
already-known Yrs operations is now claimed for idempotency and mutation-ID
conflict protection, but does not create another durable CRDT replay row or
SSE event. The ignored PostgreSQL acceptance test covers the distinction
between claim growth and replay-log growth.

Transaction-boundary fault checkpoint (2026-09-11): the PostgreSQL acceptance
gate now injects failures after the mutation claim, replay row, durable event,
and pre-commit stages of reconciliation. Each injected failure rolls back all
claims, replay rows, events, and document changes, and the next retry commits
successfully. This closes local transaction fault-injection coverage; live
process termination at every production transaction boundary remains a
deployment-owner chaos gate.

Verification checkpoint (2026-09-11): the guarded API/PostgreSQL restart gate
passed, and the new `make postgres-acceptance` gate ran inside the Compose
network against the restarted database. Durable CRDT replay/retention,
authenticated HTTP reconcile with concurrent account devices, CSRF and
cross-account isolation, and cross-instance PostgreSQL notification delivery
all passed; malformed CRDT input also proved transaction rollback leaves no
claim or update rows behind. Running these tests inside the network is
intentional because the database remains internal-only; a host-side connection
to its container IP is not a valid deployment-topology check. The live API
binary also includes bounded backoff and reconnect handling when the
PostgreSQL notification listener loses its connection or event lookup fails.
The same gate now also passes deterministic aborts after each reconciliation
write stage, with a clean post-failure retry.

Authenticated browser convergence checkpoint (2026-09-11): the authenticated
sync runtime no longer self-cancels when its startup effect records that it
has started. Pending-count reads are generation-fenced, and non-coordinator
tabs refresh the shared durable queue after another tab acknowledges it, so
all open tabs can show server-confirmed `synced` state instead of retaining a
stale pending badge. The acceptance harness now scopes queue and repair checks
to the authenticated principal. The rebuilt acceptance image passed isolated
Chromium profiles, WebKit regular and storage-fallback contexts, and the
390x844 mobile profile in each transport mode. Full Rust tests, clippy,
formatting, database acceptance, migration/capacity rehearsal, observability,
rollout preflight, and restart recovery also pass. Real WorkOS/Dodo provider
flows, physical-phone testing, production alert delivery, and process-kill
chaos remain deployment-owner gates.

Operational checkpoint (2026-09-11): a local disk-pressure incident caused
PostgreSQL recovery to fail closed; targeted Docker build-cache pruning restored
the database without touching its data volume. Database-backed readiness now
increments bounded check, success, and failure metrics, and Prometheus includes
a database-owned readiness-failure page alert. The rebuilt API passed the
public/local readiness checks, database-backed acceptance suite, and guarded
API/PostgreSQL restart-recovery smoke again after recovery.

Final local verification checkpoint (2026-09-11): the release frontend image was
rebuilt and redeployed behind the ngrok origin. The Chromium browser gate passed
in regular and storage-event fallback modes, the WebKit gate passed in both
modes, and the installed Helium-compatible Chromium executable passed in both
modes. These runs include offline edits in two tabs, fresh-tab hydration,
corrupt-snapshot repair state, empty-manifest projection protection, and scoped
CRDT-inbox acknowledgement. The PostgreSQL durable-sync/HTTP-auth boundary
acceptance, notification-listener acceptance, guarded API/database restart
recovery, Prometheus configuration/rules check, public readiness probes, and
ngrok sign-in redirect also passed. The mobile viewport gate now passes at
390x844, the guarded backup was restored into a disposable target and served a
sync acceptance test, and the clean/legacy/sized migration gate passes.
Authenticated isolated-browser, physical phone, real-provider checkout/webhook,
production alert delivery/restore, and production process-termination chaos
remain explicitly open because they require external credentials, devices, or
production-like infrastructure.

Post-deploy verification checkpoint (2026-09-11): after rebuilding the API with
the replay-log fix, the public `/readyz` and `/app` routes returned HTTP 200;
installed Google Chrome and Helium both passed the regular and storage-event
fallback browser gates against `https://adnate-anesthetically-jenice.ngrok-free.dev/app`.

Session-namespace continuity checkpoint (2026-09-11): an expired access
session is now classified separately from a true guest when the browser has a
remembered account-local namespace. Startup reopens that namespace, keeps
local edits and the durable outbox visible, pauses cloud sync with an explicit
"session expired" recovery prompt, and lets the user sign in again without
silently switching to an empty guest board. Explicit sign-out clears the
remembered account marker; a browser with no remembered account still uses the
isolated guest namespace. The account-state regression, full workspace tests,
native/Wasm warnings-denied checks, release bundle, public deployment, and
readiness probe pass. Password sign-in and email verification responses now
also persist the verified account ID before redirecting, closing the startup
window where a transient entitlement failure could otherwise choose the guest
namespace. A manual re-authenticated browser/device convergence run
is still required to prove the post-login resume path. The browser smoke
harness now also seeds a remembered account marker against the unauthenticated
deployment and asserts that an expired session renders the account namespace
and recovery prompt rather than a guest fallback. That regression now passes
against the public ngrok deployment in isolated Linux Chromium regular and
fallback modes, WebKit regular and fallback modes, and the 390x844 mobile
viewport. The server serialization regression also verifies that the login
response exposes only the verified account ID needed for this namespace
bootstrap. The macOS GUI browser run remains unavailable while that host is
locked, and authenticated isolated-browser/device convergence still requires a
real entitled session.

IndexedDB connections now close themselves on `versionchange`, so a
backgrounded current-version tab cannot block a later additive schema upgrade.
This is migration hygiene only; upgrading while an older deployed client is
still holding an uncooperative connection remains a rollout/support case that
must be handled by the staged deployment procedure.

Repair-export checkpoint (2026-09-11): a repair-state export now reads the
active principal's raw IndexedDB records, including the canonical CRDT snapshot
and durable outbox/inbox rows, plus app-scoped local-storage metadata. The
browser regression corrupts a canonical snapshot, opens the repair state, and
verifies the downloaded backup retains that corrupt record for recovery tooling;
ordinary workspace export remains the user-facing export for healthy state.

Backup/restore checkpoint (2026-09-11): the live Compose database was backed up
with the repository script, verified by checksum and `pg_restore --list`, then
restored with the guarded restore script into a separate disposable PostgreSQL
container. Post-restore catalog and application-table queries passed, and the
deployment data volume was not used as a restore target. Production restore
rehearsal, migration replay after restore, and alert delivery remain separate
deployment-owner gates.

The restored disposable target also passed the real ignored PostgreSQL sync
acceptance test, including the store migration startup and durable reconcile,
pull, metadata, compaction, replay, and malformed-update rollback assertions.

Migration acceptance checkpoint (2026-09-11): `make migration-acceptance`
passed against a clean disposable database, a pre-0020 legacy fixture with
representative space/document/update rows, and a synthetic capacity fixture
with 1,000 spaces plus 10,000 durable updates/events. The legacy run verified
the deterministic stable-space-ID and durable mutation-claim backfills, then
ran the PostgreSQL sync acceptance test successfully. A final production-sized
timing run remains a staged-rollout capacity gate.

Capacity timing checkpoint (2026-09-11): the migration fixture now accepts
`TASK_SPACE_MIGRATION_SIZED_SPACES` and
`TASK_SPACE_MIGRATION_SIZED_UPDATES` overrides. A 10,000-space/100,000-update
run through the repeatable `make capacity-acceptance` target completed clean,
legacy, migration, retention, and durable-sync checks in 11 seconds on the
local Compose host; production traffic volume and staged rollout timing remain
deployment-owner capacity gates.

Cursor correctness checkpoint (2026-09-11): reconcile responses now expose an
advisory observed event sequence but cannot advance the browser's applied SSE
cursor, because a document reconcile response does not contain intervening
metadata events. SSE events are processed serially per tab; each document
event waits for its authenticated state-vector pull and durable workspace
projection before its cursor is persisted, while metadata-only events wait for
their durable projection. This prevents a fast document response or an
out-of-order pull from skipping rename, archive, restore, or delete events.

Workspace projection writes now allocate their ordering inside the IndexedDB
readwrite transaction instead of using a per-tab counter. Local board edits no
longer persist a pre-CRDT workspace mirror; the atomic CRDT outbox transaction
owns the durable edit and only writes a workspace projection when its shared
generation is current. A stale-generation sibling therefore cannot replace a
newer durable snapshot or its reload projection.

All workspace projection writers now merge the existing principal-scoped
projection by stable space ID, preserving sibling-created spaces and choosing
the newest per-space metadata/board projection during late physical writes.
Server space timestamps use epoch milliseconds, matching the browser model and
making timestamp-based projection ordering and tombstone comparisons
consistent across reloads and devices.

Remote metadata projections also reject an event whose metadata version is
older than the version already rendered, so cross-instance event reordering
cannot roll a rename/archive/delete state backward.

Local metadata broadcasts now carry their operation identity as well. Each
tab keeps a principal-and-space watermark and applies only the greatest
operation identity it has seen, matching the server's deterministic
same-version metadata winner. This prevents reordered BroadcastChannel or
storage-fallback messages from making two tabs display different rename,
archive, restore, or delete results before the next server reconciliation.

The local tab transport is now principal-scoped and dual-path: it uses a
principal-specific `BroadcastChannel` plus a principal-specific
`localStorage` `storage`-event fallback. Channel-construction, storage-access,
EventSource, and Web Locks failures are caught and downgraded to the next
available mechanism instead of breaking the board in restricted or private
browser profiles. Account switching retargets the local channel before new
messages are accepted, so an old principal's payload cannot be delivered to
the new principal's tab through the shared origin.

Empty-space creation is also broadcast as a first-class local workspace event,
including its stable identity. A sibling tab therefore sees a newly created
space immediately even before that space has its first CRDT edit or the next
manifest refresh; authenticated registration remains idempotent and is still
performed through the server path.

SSE cursor reset recovery now closes and recreates the `EventSource` after a
`sync-reset` event, because browsers retain an internal `Last-Event-ID` across
reconnects. The client clears the persisted cursor, performs the manifest/full
reconciliation, and then reconnects from a clean cursor instead of repeatedly
replaying an expired one.

The local-save indicator is now completed from the same atomic CRDT/outbox
commit that persists an edit. Hydration, space switching, and remote
projection-only updates also settle the indicator instead of leaving it in a
permanent saving state; IndexedDB failures still win and remain visible as a
local-storage error.

The Rust SSE response now carries `Cache-Control: no-cache, no-store,
must-revalidate` and `X-Accel-Buffering: no` itself, in addition to the nginx
configuration, so direct API/ngrok access cannot silently reintroduce proxy
buffering or stale stream caching.

The API now exposes public `/healthz` and database-backed `/readyz` probes
with `no-store` responses. Deployment observability assets now include a
private Prometheus scrape configuration, bounded alert rules, designated
operational roles, and guarded PostgreSQL backup, verification, and restore
scripts. Installing and exercising those assets in the real deployment is
still an acceptance gate; the repository no longer leaves the operational
contract implicit. The backup and checksum path has now been exercised against
the running PostgreSQL 17 instance with the matching Alpine client, including
catalog verification. The guarded restore path was then rehearsed into an
explicit temporary database, its CRDT/event/claim tables were verified, and
the temporary target was removed; it also correctly refuses to proceed without
its explicit confirmation variable. Production restore rehearsal and alert
delivery remain deployment-owner acceptance gates. A temporary API instance
also verified that anonymous metrics scraping is denied and a Bearer-token
scrape returns Prometheus text with `no-store` headers.

The checked-in Prometheus configuration and seven alert rules now pass the
pinned `promtool` gate through `make observability-check`; the gate stages an
empty credentials file, runs semantic fixtures for readiness failures, API
5xx responses, and cursor resets, and never uses or exposes the real metrics
secret. The checked-in Grafana dashboard references the same bounded metric
contract, including Prometheus-compatible latency bucket series, and its JSON
is validated by the same repository gate.

Latest verification evidence: a fresh local Trunk build was exercised in two
same-profile browser tabs with no authenticated API after the generation-guard
and workspace-ordering changes. Both tabs loaded the IndexedDB workspace; two
independent edits made in different tabs converged into both live projections,
and a newly opened third tab rehydrated all three notes from IndexedDB. This
proves the offline same-browser durability and concurrent-edit smoke path only;
it does not prove authenticated cross-browser or cross-device reconciliation.
The same path was rerun in headless Chrome after the principal-scoped
transport and UI hit-testing fixes: the first note was created, edited in tab
one, a second note was created in tab two, tab one received it without a
refresh, and a fresh third tab rehydrated both notes. This remains a local
IndexedDB/BroadcastChannel smoke test, not a substitute for authenticated
server reconciliation.
The rebuilt frontend image is now the one served by the local Nginx/ngrok
path, including its current hashed JavaScript, Wasm, and snippet assets. The
same tunnel reaches the rebuilt API: unauthenticated `/auth/session` and
`/sync/events` return `401` with `Cache-Control: no-store`, a request ID, and
the expected `SESSION_REQUIRED` boundary; `/auth/sign-in` returns the WorkOS
redirect with the ngrok callback URL and a secure state cookie. The API image
booted against the healthy PostgreSQL instance and is attached under the
proxy's `task-space-api` network alias. This proves deployment wiring and
fail-closed auth routing, not an authenticated browser/device convergence run.
After the local-space, cursor-reset, storage-status, direct-SSE-header, local
metadata-watermark, outbox-wire-compatibility, and bounded-outbox hardening
changes, the Rust
workspace, 39 core
tests, 53 server-library tests (51 passed, 2 ignored), 2 server-binary tests, 16 web tests, native and Wasm
web checks, JavaScript syntax extraction, full workspace and Wasm-target
warnings-denied Clippy, and release Trunk build are green. A freshly rebuilt
current API image was started on an
isolated port against the running healthy PostgreSQL instance; it applied
migrations through 0022, backfilled stable space UUIDs, and exposed the
expected CORS/auth boundary, including `Cache-Control: no-store` on the
unauthenticated session response and the configured ngrok origin. The API was
also rebuilt and restarted from the current worktree; provider-backed checkout
and webhook acceptance remain open until the configured Dodo environment and
credentials are verified in a disposable test account.

The ignored PostgreSQL smoke test was run inside the Compose network against
the migrated database. It passed real space registration, stable-ID mismatch
rejection, CRDT reconcile and idempotent retry, durable pull, event replay,
and metadata rename. It also verifies that a fresh edit remains outside the
compaction candidate set even when an older `compacted_at` marker exists; the
compactor rechecks activity after acquiring the document row lock. The browser
outbox now bounds normal pending mutations
to 128 entries or 8 MiB of encoded deltas; when that threshold is reached it
atomically replaces superseded rows with one fresh full-snapshot recovery
mutation, preserving local CRDT state without retrying an already-claimed
mutation ID. Legacy unscoped outbox keys are removed during that coalescing
path so migration fallback cannot replay superseded deltas beside the recovery
snapshot. The same smoke also rejects changed-payload reuse of a mutation ID,
merges a second offline device's CRDT update, and verifies that history
retention forces an explicit cursor reset once replay rows are pruned. The
durable mutation-claim ledger also rejects a changed payload after replay
history pruning, so idempotency does not depend on the event-log retention
window. The ignored HTTP acceptance test now runs two independently
authenticated account devices concurrently through the real Axum/PostgreSQL
reconcile route, pulls the merged document, and checks both device edits before
exercising the cross-account denial and CSRF boundaries.
If a local snapshot is larger than the server's single-update limit, the
browser does not coalesce it into an unsendable recovery row; it preserves the
original durable outbox and exposes a safe local-sync error for repair/export
handling.

After the compaction race, legacy-key hardening, and durable mutation-claim
ledger, the full workspace test,
workspace Clippy, Wasm-target Clippy, native/Wasm checks, JavaScript syntax
check, and the ignored PostgreSQL smoke test were rerun successfully. A fresh
release Trunk bundle was rebuilt and served through the live ngrok path; `/`
returned `200` with no-store headers and `/auth/session` returned the expected
unauthenticated `401` boundary.

The outbox parser now explicitly maps the IndexedDB `mutationId` field to the
Rust `mutation_id` field. Before this correction, the browser could retain
durable CRDT rows while the drain and pending-count reader rejected the queue
shape, leaving paused-account diagnostics misleading and preventing retries.
The fix is covered by the same Wasm/build gates above; an authenticated
browser acceptance run is still required once the deployment API can boot.

Full workspace and `task-web` Clippy with warnings denied now pass for both
the native workspace and the Wasm target. The sync functions keep their
explicit state parameters; the remaining structural allowance is limited to
those intentionally stateful UI helpers.

The compose API previously crash-looped because the configured
`DODO_PAYMENTS_API_KEY` is an opaque 65-character value and the old startup
check only recognized the legacy `dodo_test_` prefix. Startup validation now
accepts documented `dp_test_` keys, legacy `dodo_test_` keys, and
provider-issued opaque keys, while still rejecting a known live-mode prefix.
Provider checkout acceptance still requires a real test credential and valid
test product IDs.

The current local acceptance API is running with a format-valid placeholder
test key so auth, manifests, metadata, CRDT reconciliation, and SSE can be
exercised through the tunnel. Payment-provider calls are intentionally not
considered passing until a real Dodo test key and product IDs are configured;
the placeholder must not be used for checkout acceptance.

The acceptance frontend and API containers were rebuilt from this worktree
after the operational hardening changes. The live ngrok origin now serves the
current HTML, JavaScript, Wasm, and snippet assets with successful `200`
responses; unauthenticated session and SSE requests still fail closed with
`401 SESSION_REQUIRED`, while `/healthz` returns `{"status":"ok"}` and
`/readyz` returns `{"status":"ready"}` after a real PostgreSQL probe. This
confirms the deployed local wiring, but it is not a substitute for a real
authenticated browser/device run.

The repeatable browser gate is now green against the rebuilt frontend:
`TASK_SPACE_BROWSER_EXECUTABLE='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
TASK_SPACE_BROWSER_URL='https://adnate-anesthetically-jenice.ngrok-free.dev/app' \
python3 scripts/browser-sync-smoke.py`
reports `{"status":"ok",
"contexts":["regular","fallback"]}`. It covers three same-profile tabs,
offline edits in both directions, IndexedDB outbox durability, a fresh-tab
reload, normal `BroadcastChannel` delivery, and the `localStorage`/`storage`
event fallback with BroadcastChannel and Web Locks disabled. This gate also
exercised the deterministic per-principal initial-space identity, which fixes
the startup race where simultaneous fresh tabs could create different local
guest spaces and then correctly filter each other's valid CRDT updates. It
also corrupts the canonical IndexedDB snapshot after the successful reload and
verifies that the client fails closed with an explicit repair/export state
instead of claiming the workspace is saved. It does not prove authenticated
Chrome/Helium/phone convergence, provider checkout, long-running chaos, or
backup/restore acceptance.
The same regression now passes in the installed WebKit engine as well
(`python3 scripts/browser-sync-smoke.py --engine webkit`), covering the local
transport, durability, coordinator takeover, and corrupt-record behavior
across two browser engines.
The Chromium regression also passes through the installed Helium executable in
both regular and fallback modes. These are still unauthenticated local-first
browser gates; they do not prove same-account server convergence across
isolated browser profiles.
The browser gate now accepts `TASK_SPACE_SESSION_COOKIE` for the authenticated
acceptance path, requires an `account:*` local principal, and waits for the
reconnected outbox to drain and for the visible `synced` status. That path is
executable with a disposable entitled provider session, but no real provider
session is available in this environment, so its end-to-end pass remains
open.
The same harness now supports `--mobile` at a 390x844 viewport; the local
mobile-layout/durability/fallback/repair gate passes, while physical-phone
authenticated convergence remains an external device gate.
The same harness now has an opt-in `--isolated-profiles` mode that starts two
separate Chromium/Helium profiles, performs online/offline edits against the
same account, waits for both outboxes to drain and for visible `synced` status,
and reloads both stores. It also applies the ngrok warning bypass and stale
auth-lease takeover setup to each isolated profile, so the advertised
cross-browser path reaches the application rather than the tunnel
interstitial. It is
ready for the provider-backed acceptance run but remains unverified here
because no disposable entitled session is available.
It also supports `--cross-browser`, which launches separate Chrome and Helium
profiles for that same-account scenario; this remains an external authenticated
gate for the same reason.
The guarded `make restart-chaos` gate has now restarted the acceptance API and
PostgreSQL containers, waited for database-backed readiness after each restart,
and verified the protected `401`/`no-store` boundary; the ngrok `/readyz` probe
returned `200` afterward. This covers local process/database restart recovery,
not production process-termination chaos or provider-backed convergence.
The PostgreSQL notification listener now has a deterministic readiness signal,
and its ignored integration gate passes against the live database: one store
connection commits a CRDT mutation while a second store connection forwards
the durable event through the process-local delivery channel. This verifies
the multi-instance SSE wake-up path; replay/reconcile remain the correctness
fallback if a notification is delayed or missed.

The namespace lifecycle now clears the previous account projection before an
account switch, suppresses persistence during the transition, preserves the
explicit guest-adoption snapshot, and reloads into the guest principal after
confirmed account expiry. Late account responses are rejected by both
principal and account-state checks. SSE cursors are now persisted only after a
successful authenticated pull commits the merged snapshot and state vector;
transient online `EventSource` failures leave browser reconnect enabled and do
not masquerade as authentication expiry. Same-browser CRDT deltas also enter
a principal/space-scoped IndexedDB inbox before live application. Hydration
replays that inbox, while a successful sibling-snapshot commit removes only
the exact origin-device/generation row covered by that snapshot; pull and
reconcile commits never clear unrelated inbox rows. The IndexedDB schema is
now version 7.
These changes are covered by the rebuilt Chromium and WebKit local-first smoke
gates; authenticated isolated-browser, phone, provider, production alert, and
production process-termination chaos acceptance remain external gates.

Completion-audit rerun (2026-09-11): the current worktree passed `make
browser-sync`, `make browser-mobile`, `make browser-webkit`, `make
observability-check`, `make migration-acceptance`, `make restart-chaos`, and
`make postgres-acceptance`. The public ngrok `/readyz` probe returned
`{"status":"ready"}` after the restart gate. The default installed Chrome
launch had one transient pre-CDP exit and passed on retry in the earlier
audit; the current rerun passed in the pinned containerized Playwright runtime.
The host GUI is currently locked and its direct Chromium launch exited before
CDP, so that host result is not counted as a product failure. These results
verify the reproducible local/deployment gates, while authenticated isolated-browser,
physical-phone, real-provider checkout/webhook, production alert/restore, and
production process-termination chaos acceptance remain external gates.

The browser harness now accepts `TASK_SPACE_BROWSER_URL` in addition to its
local default. With the current ngrok origin, both the regular/fallback and
390x844 mobile gates passed through the public deployment path, and the
installed Helium-compatible executable passed the same public Chromium
regression; the isolated
Chrome/Helium path also applies the ngrok warning bypass before opening each
profile. This verifies that the tested assets and local-first behavior are
reachable through the same origin used for manual auth testing, but it still
does not create authenticated convergence evidence without a real session.

Final deployment audit rerun (2026-09-11): after rebuilding the frontend from
the current worktree and recreating the Nginx container, the public
`/readyz` probe returned `{"status":"ready"}`. The public Chromium,
Helium-compatible Chromium, WebKit, and 390x844 mobile gates all passed in
regular and fallback modes. The final worktree also passes formatting,
warnings-denied Clippy, workspace tests, and whitespace validation. No
`TASK_SPACE_SESSION_COOKIE` is configured in this environment, so the
authenticated isolated-profile/cross-browser path remains intentionally
unclaimed rather than being represented by a guest test.

Deletion-schema audit rerun (2026-09-11): the repeated stale-restore and
tombstone-compaction regressions pass across randomized Yrs client IDs, the
full workspace now reports 39 core tests, 53 server-library tests (51 passed, 2 ignored), 2 server-binary tests, and 16 web tests with no failures, and native plus
Wasm warnings-denied Clippy pass. The release bundle rebuilt with CRDT schema
4 and its public Chromium/Helium/WebKit smoke gates pass. Clean, legacy, and
capacity migration acceptance plus the PostgreSQL durable-sync, authenticated
HTTP, CSRF/account-isolation, and notification-listener gates also pass after
the schema change.

Acceptance rerun checkpoint (2026-09-11): after the latest API restart and
documentation correction, `make observability-check`, `make
migration-acceptance`, `make capacity-acceptance`, `make postgres-acceptance`,
and `make restart-chaos` all passed. The public `/readyz` probe returned HTTP
200 with `{"status":"ready"}`. The deployed ngrok app then passed the browser
smoke in an isolated Linux Playwright runtime for Chromium regular/fallback,
WebKit regular/fallback, and the 390x844 mobile viewport. These are reproducible
local/deployment gates; authenticated isolated profiles, physical-phone
convergence, real Dodo checkout/webhooks, production alert delivery and
restore, and production process-termination chaos remain deployment-owner
gates because they require external credentials, devices, or production-like
infrastructure.

Security hardening checkpoint (2026-09-11): the authenticated HTTP surface now
enforces separate bounded account and client-IP budgets, with normalized proxy
address handling and regression coverage for shared-IP, forwarded-IP, and
malformed-header cases. The unauthenticated auth surface also applies bounded
client-IP budgets to login, verification, reset, refresh, callback, and logout
routes. The API and Nginx images were rebuilt from this
worktree; Nginx syntax validation, live API startup, public `/readyz`, and the
public Chromium regular/fallback browser gate all passed after deployment.

Rollout-control checkpoint (2026-09-11): `scripts/rollout-preflight.sh` and
the `make rollout-preflight` target now provide a repeatable deployment gate
for readiness, HTML delivery, the expected unauthenticated session boundary,
and an optional private metrics scrape. The runbook now defines internal,
canary, percentage-cohort, stop-condition, bounded-rollback, and legacy-path
retirement checkpoints. Actual cohort exposure, alert delivery, and
authenticated device/provider acceptance remain deployment-owner actions.

Authenticated browser acceptance checkpoint (2026-09-11): the repository now
includes `browser_acceptance`, a disposable server binary that reuses the
production protected sync router and real compiled frontend while substituting
only a deterministic in-process session verifier. `make browser-authenticated`
seeds a disposable entitled account, runs two isolated Chromium profiles plus
WebKit and the 390x844 mobile flow, and removes the generated account on exit.
This closes automated same-account browser reconciliation coverage without
introducing a test credential path into the production binary; real WorkOS,
physical-phone, and provider checkout acceptance remain separate external
gates.

Provider test-mode checkpoint (2026-09-11): `make provider-acceptance` now
provides a repeatable external Dodo test-mode gate. Against the configured
test product and public webhook origin it verified product lookup, checkout
creation, a correctly signed subscription webhook, duplicate webhook replay,
and disposable entitlement cleanup. Authenticated checkout through the app's
protected route still requires a real WorkOS session, and physical-phone,
production alert/restore, and process-termination chaos evidence remain
deployment-owner gates.

Real WorkOS deployment checkpoint (2026-09-11): `make workos-authenticated`
now creates and removes a disposable verified WorkOS user, authenticates it
through the real password session exchange, activates a disposable entitlement
through the signed public Dodo webhook path, verifies the protected session and
entitlement endpoints, and runs two isolated browser profiles against the
public frontend and API. The first run exposed two deployment hazards: stale
frontend/API images and a disposable acceptance container accidentally sharing
the production `task-space-api` DNS alias. The latter randomly split requests
between server builds, causing intermittent `426` responses and stale
documents. The stale alias/container was removed, the chaos container now uses
its own name, and the public gate passes with one production API target. A
physical phone and production provider account remain separate
device/deployment-owner gates.

Proxy-fallback checkpoint (2026-09-11): the browser now performs a
coordinator-only server reconciliation every 15 seconds in addition to SSE.
SSE remains the low-latency wake-up path, but a proxy that buffers or silently
drops an EventSource stream cannot prevent eventual convergence. If an SSE
delta arrives without its dependencies, the client also performs a full
state-vector pull before advancing the event cursor.

Abrupt-recovery checkpoint (2026-09-11): `make process-kill-chaos` now sends
`SIGKILL` to the named disposable acceptance API and PostgreSQL containers,
restarts them, and verifies readiness plus the fail-closed auth boundary after
each kill. This strengthens local crash-recovery evidence; production
transaction-boundary process-kill, alert delivery, and restore acceptance still
require production-like infrastructure and an operator-controlled window.

Alert-delivery checkpoint (2026-09-11): `make alert-delivery` now runs a
disposable Prometheus-to-Alertmanager-to-webhook path with a synthetic
readiness-failure metric. It verified that `TaskSpaceReadinessFailures` fires
and reaches the receiver with the expected `database` owner and `page`
severity labels. This closes the repository-level notification wiring check;
staging/production receiver delivery and paging acknowledgement remain
deployment-owner evidence.

Backup/restore rehearsal checkpoint (2026-09-11):
`make backup-restore-acceptance` now creates and checksum-verifies a custom
format dump of the live local Compose database, restores it into a disposable
PostgreSQL container, and runs the durable PostgreSQL sync acceptance test
against the restored target. The live data volume is never used as a restore
target; production maintenance-target restore and migration replay remain
deployment-owner evidence.

## 1. Purpose

Build a local-first synchronization system with these user-visible properties:

- Editing never depends on the network being available.
- An accepted edit survives refreshes, crashes, sign-out, and temporary loss of sync access.
- Tabs in the same browser converge immediately through local communication.
- Browsers and devices signed into the same account converge through durable server reconciliation.
- Concurrent offline edits merge according to documented, deterministic rules.
- SSE improves latency but is never required for correctness.
- Authentication, billing, proxy, or server failures never silently present an empty workspace or discard local changes.
- Every sync state and failure is diagnosable by users, developers, and operators.

This plan aims for reliability comparable to mature local-first products. It does not attempt to copy another product's internal implementation.

## 2. Product guarantees

The completed system must guarantee the following.

1. **Local durability:** every accepted edit is stored in IndexedDB before it is considered locally saved.
2. **Single client source of truth:** the live per-space CRDT document is authoritative. UI projections are derived from it.
3. **No last-snapshot overwrite:** synchronization exchanges CRDT updates and state vectors; it never resolves ordinary concurrency by replacing an entire board with whichever snapshot arrived last.
4. **Deterministic convergence:** replicas that receive the same set of operations eventually produce the same document regardless of delivery order, duplication, or retries.
5. **Durable outbox:** unacknowledged local work remains recoverable until the server confirms it was committed.
6. **Idempotent server effects:** retrying the same mutation cannot apply it twice.
7. **Account isolation:** guest data and every authenticated account have separate local namespaces and separate server ownership.
8. **Transport independence:** synchronization eventually succeeds without SSE through startup, reconnect, focus, online-event, and periodic reconciliation.
9. **Deletion safety:** stale devices cannot accidentally resurrect deleted content.
10. **Non-destructive access changes:** authentication expiry or billing suspension pauses cloud synchronization but does not delete local or server data.
11. **Recoverability:** a user can always see pending work, retry synchronization, and export local data.
12. **Truthful status:** “synced” means every local generation has been acknowledged by the server, not merely written to local storage.

## 3. Terminology

- **Replica:** one running tab containing an in-memory CRDT document.
- **Installation:** one browser profile on one device. Browsers do not share IndexedDB with each other.
- **Principal:** either `guest:<installation_id>` or `account:<account_id>`.
- **Device ID:** stable random identifier for an installation within a principal namespace.
- **Tab ID:** random identifier created for each page lifetime.
- **Local generation:** monotonically increasing number assigned after a local transaction is persisted.
- **Server sequence:** monotonically increasing account-scoped event sequence assigned by the server.
- **State vector:** compact CRDT description of the operations already known by a replica.
- **Outbox:** durable local records that have not been acknowledged by the server.
- **Reconciliation:** bidirectional exchange that uploads pending work and returns everything missing locally.
- **Coordinator:** the tab currently responsible for network synchronization for one principal.
- **SSE:** a best-effort wake-up channel telling the coordinator to reconcile.

## 4. Capability boundaries

Different synchronization scopes require different mechanisms.

| Scope | Shared mechanism | Works offline? | Authentication required? |
|---|---|---:|---:|
| Same tab | In-memory CRDT | Yes | No |
| Multiple tabs in one browser profile | `BroadcastChannel` plus IndexedDB | Yes | No |
| Multiple browsers on one device | Server reconciliation | No, until connectivity returns | Yes |
| Multiple physical devices | Server reconciliation | No, until connectivity returns | Yes |

Chrome, Helium, Safari, Firefox, and phone browsers have isolated local storage. They cannot synchronize directly through IndexedDB or `BroadcastChannel`; each must authenticate and reconcile through the server.

## 5. Target architecture

```text
User action
    |
    v
CRDT transaction in active tab
    |
    +--> Atomic IndexedDB commit: snapshot/update + generation + outbox
    |
    +--> Broadcast actual CRDT update to sibling tabs
    |
    v
Network coordinator tab
    |
    +--> POST reconciliation request with state vector and pending updates
    |
    v
Transactional server merge in PostgreSQL
    |
    +--> Mutation acknowledgement
    +--> Missing CRDT update for this client
    +--> Durable account event
    |
    v
SSE wake-up to other online installations
    |
    v
Other browsers/devices reconcile and apply the same CRDT operations
```

### 5.1 Correctness hierarchy

The system depends on components in this order:

1. The CRDT defines merge and convergence behavior.
2. IndexedDB provides local durability and crash recovery.
3. The reconciliation protocol provides cross-browser and cross-device correctness.
4. The durable server event log provides replayable change notification.
5. SSE reduces latency.
6. Periodic reconciliation repairs missed notifications and unknown failures.

No lower item in this list may be required to preserve correctness promised by a higher item.

## 6. Canonical client data model

### 6.1 One authoritative document per space

Each space has one live CRDT document. It contains:

- schema version;
- space-level board configuration where appropriate;
- notes keyed by globally unique note IDs;
- groups keyed by globally unique group IDs;
- stable ordering data;
- independent fields such as text, color, position, size, and group membership;
- deletion tombstones;
- explicit restoration operations when supported.

The UI's `notes`, `groups`, and related signals are projections of this document. UI code must not mutate those projections independently and later attempt to reconstruct the CRDT.

### 6.2 Local persistent record

Use a principal-scoped IndexedDB database with an atomic record per space:

```text
principal
  account metadata cache
  device state
  space:<space_id>
    schema_version
    encoded_crdt_snapshot
    current_state_vector
    last_server_state_vector
    local_generation
    acknowledged_generation
    pending_updates[]
    in_flight_request
    last_server_sequence
    last_local_commit_at
    last_server_ack_at
    last_error
```

The atomic transaction for a local edit must update the CRDT record, increment `local_generation`, and append the pending update together. A crash must never leave the UI-visible edit without either a recoverable snapshot or update.

### 6.3 Identifiers

- Space, note, and group IDs must be UUIDs or ULIDs generated locally.
- Mutation IDs must be cryptographically random UUIDs.
- IDs must never depend on device clocks or a shared numeric counter.
- Device IDs are random and principal-scoped.
- Tab IDs are ephemeral and never persisted as data ownership.
- Server sequences are generated by the server and are not CRDT conflict clocks.

Compatibility gate: the current deployed domain model still exposes existing
space, note, and group identifiers as JavaScript-safe numeric values. New
values are generated from cryptographically random UUID material and are
checked against the active namespace, so they do not rely on clocks or a
shared counter. The implementation now carries canonical `stable_id` UUIDs
for notes and groups in CRDT schema v4, deterministically re-keys legacy
numeric CRDT maps while preserving their numeric aliases and tombstones, and
adds a durable `spaces.stable_id` UUID with migration `0021_space_stable_ids`.
New sync requests and responses carry the canonical space UUID beside the
legacy route alias, and PostgreSQL verifies that pair. Retiring the numeric
route/database aliases remains a staged-rollout gate; it must preserve the
mapping and tombstones rather than silently renumbering existing data.

### 6.4 Time

Browser wall-clock time must not decide merge winners. Timestamps can support display and diagnostics, but CRDT logical ordering and stable actor identifiers determine convergence.

## 7. Local edit pipeline

Every local edit follows one code path:

1. Validate the requested action locally.
2. Open a CRDT transaction.
3. Apply the operation to the authoritative document.
4. Produce the binary CRDT update generated by that transaction.
5. Atomically persist the updated document, update, and next local generation in IndexedDB.
6. Update the UI projection.
7. Broadcast the actual CRDT update to sibling tabs.
8. Wake the network coordinator when cloud sync is eligible.

If IndexedDB persistence fails, the UI must show a local-save error and must not claim the edit is safely saved. It should keep the in-memory document available for retry or export.

Operations covered by this pipeline include:

- create, edit, move, resize, recolor, and delete note;
- create, rename, move, resize, and delete group;
- add or remove group membership;
- ordering and board-layout changes;
- explicit restoration;
- bulk operations;
- future document fields introduced through schema migration.

## 8. Multi-tab synchronization

### 8.1 Local update channel

A `BroadcastChannel` starts as soon as local storage is initialized, including for guests. It is not tied to server entitlement or SSE.

Messages include:

```text
protocol_version
principal
space_id
origin_device_id
origin_tab_id
local_generation
encoded_crdt_update
```

A receiving tab:

1. rejects messages for another principal or unsupported protocol;
2. ignores messages originating from itself;
3. applies the CRDT update idempotently;
4. persists the merged document in IndexedDB using a write-order guard;
5. refreshes its UI projection if the affected space is visible;
6. wakes the coordinator if server acknowledgement is still pending.

The message contains the actual update. A message containing only “sync now” is insufficient because it forces a tab to depend on server synchronization and does not guarantee local convergence while offline.

The browser keeps UI arrays as a projection only. Before a local edit is
queued, the canonical `SpaceDoc` merges the projection delta and its resulting
board is rendered back into the UI; a sibling-tab edit therefore cannot be
silently overwritten by a stale signal snapshot.

### 8.2 Coordinator election

Use the Web Locks API for one coordinator per principal when available. Use a renewable IndexedDB/localStorage lease with fencing tokens as the fallback.

- Only the coordinator performs routine server reconciliation. SSE remains a
  wake-up-only channel; connected tabs may keep a read-only SSE subscription
  so a coordinator handoff does not create a notification blind spot. It
  never acknowledges data or becomes a second write path.
- Any tab can commit local edits.
- Non-coordinator tabs notify the coordinator after durable local commit.
- If the coordinator closes, crashes, sleeps too long, or loses its lease, another tab takes over.
- The fallback lease stores an owner, renewable expiry, and monotonically increasing fencing epoch. Every coordinator response path re-checks the lease owner/epoch before applying or acknowledging data; a stale coordinator may finish an HTTP request, but it cannot commit its result after takeover.
- Web Locks ownership is treated as the stronger same-browser fence. The localStorage epoch is a best-effort browser fallback and must fail closed for persisted response application whenever storage can be read; storage-disabled environments remain protected by request idempotency and generation guards but should surface reduced coordination diagnostics.
- A future server-side coordinator epoch may strengthen cross-device fencing, but it is not a substitute for idempotent reconcile requests: network responses can always be delayed or duplicated.
- Duplicate network requests remain safe because mutation handling is idempotent.

### 8.3 Broadcast fallback

If `BroadcastChannel` is unavailable or a tab was suspended:

- the tab reloads newer IndexedDB generations on focus and `visibilitychange`;
- the coordinator periodically checks for dirty spaces;
- storage generation comparisons prevent stale in-memory state from overwriting newer persisted state.

## 9. Server reconciliation protocol

Introduce a versioned endpoint rather than changing existing behavior silently:

```text
POST /sync/v2/spaces/:space_id/reconcile
```

### 9.1 Request

The request includes:

- protocol and document schema versions;
- device ID;
- request/mutation ID;
- last known server sequence;
- client state vector captured when the request starts;
- ordered pending CRDT updates or one reconstructed aggregate update;
- local generation captured when the payload is frozen;
- expected space metadata version where relevant.

The exact in-flight payload is persisted before transmission. If the response is lost, the client retries the same bytes with the same request ID.

### 9.2 Server transaction

Within one database transaction, the server:

1. verifies the session and derives the account from it;
2. verifies sync entitlement and resource ownership;
3. validates protocol, schema, IDs, and payload limits;
4. locks the space's synchronization row;
5. checks whether the request ID has already been committed;
6. loads or reconstructs the canonical CRDT document;
7. applies client updates idempotently;
8. computes the update missing from the submitted client state vector;
9. stores the merged durable snapshot and state vector;
10. records request ID, request hash, device ID, and acknowledgement data;
11. appends a durable account event;
12. commits before publishing any live notification.

If the same request ID and hash are retried, the server returns a replayable successful result. Reusing an ID with a different payload is a permanent conflict and must never be accepted.

### 9.3 Response

The response includes:

- acknowledged request and mutation IDs;
- acknowledged local generation;
- current server state vector;
- CRDT update missing from the client;
- current server sequence;
- current metadata version;
- entitlement version;
- `has_more` or reset information when the response is bounded.

### 9.4 Client commit

The client:

1. validates principal, space, protocol, and request identity;
2. applies the returned update to its current CRDT, not an older captured snapshot;
3. atomically persists the merged document and server state vector;
4. removes only explicitly acknowledged outbox records;
5. advances `acknowledged_generation` only to the generation captured by that request;
6. preserves edits created while the request was in flight;
7. reconciles again immediately if a newer local generation exists;
8. broadcasts the returned update to sibling tabs. Pull and SSE-applied
   remote deltas use the same actual-update broadcast path, so one tab can
   repair sibling tabs even when they missed the original event.

## 10. Durable events and SSE

Every committed document or metadata change creates an account-scoped event in PostgreSQL in the same transaction as the change.

SSE events contain only enough information to wake reconciliation:

- server event ID/sequence;
- event kind;
- space or resource ID;
- metadata and entitlement versions where applicable.

Rules:

- Persist the latest processed cursor locally.
- Replay retained events on reconnect.
- Reconcile before marking an event processed.
- If the cursor is too old, return `reset_required` and reconcile the complete space manifest.
- Disable proxy buffering and compression behavior that delays event delivery.
- Use heartbeats to detect dead connections.
- Recreate SSE after auth refresh.
- Run safety reconciliation even while SSE appears healthy.
- Never transmit full document state solely through SSE.

SSE failure may delay visible remote changes, but it must never prevent eventual convergence.

## 11. Bootstrap and namespace lifecycle

### 11.1 Guest startup

1. Resolve or generate the installation ID.
2. Open the guest principal namespace.
3. Load local CRDT documents from IndexedDB.
4. Start local multi-tab communication.
5. Allow immediate offline editing.
6. Keep cloud sync disabled until an authenticated and entitled account is verified.

### 11.2 Authenticated startup

1. Call the server session endpoint with credentials.
2. Treat the session response as the only authoritative account identity.
3. Open the exact account principal namespace.
4. Start local tab synchronization for that namespace.
5. Fetch the server space manifest and metadata tombstones.
6. Reconcile the active space first.
7. Reconcile all remaining spaces with bounded concurrency.
8. Select a real remote/default space rather than a blank local placeholder.
9. Start coordinator ownership and SSE.
10. Display “synced” only after local and remote generations agree.

### 11.3 First sign-in with guest data

Present an explicit one-time choice:

- sync this local guest workspace into the account; or
- keep it separate and open the existing account workspace.

The adoption operation must be idempotent, preserve all IDs/tombstones, and record completion so refreshes cannot import the guest workspace repeatedly.

### 11.4 Sign-out and account switching

- Stop server synchronization and SSE before switching namespaces.
- Flush already-durable local transactions, but do not wait indefinitely for network acknowledgement.
- Never copy one account's local cache into another account automatically.
- Clear session data without deleting account-scoped IndexedDB records.
- Return to the installation's guest namespace after sign-out.
- Ignore late responses from the previous principal.

## 12. Online and offline behavior

### 12.1 Starting online

- Load local data immediately.
- Verify the server session.
- Reconcile local pending work and remote missing work.
- Start SSE after the initial active-space reconciliation.
- Continue editing throughout; bootstrap must not replace newer local work.

### 12.2 Starting offline

- Load all available local data.
- Show “offline—changes saved locally.”
- Permit normal editing.
- Persist and broadcast changes to tabs in the same browser.
- Leave cross-browser/device work pending until connectivity returns.

### 12.3 Going offline while editing

- Allow an in-flight request to fail without deleting its outbox payload.
- Keep accepting and persisting edits.
- Stop aggressive retries and transition to `offline_waiting`.
- Keep local tabs synchronized.

### 12.4 Returning online with remote changes

1. Freeze a reconciliation payload containing all unacknowledged local work.
2. Send the local state vector and pending updates.
3. Receive all server operations missing locally.
4. Merge both sets through the CRDT.
5. Persist the merged document atomically.
6. Remove only server-acknowledged outbox records.
7. Reconcile again if work arrived during the request.
8. Broadcast the merged update to sibling tabs.

The server snapshot must never blindly replace the offline client's document. Different edits are preserved; conflicts on the same field follow Section 14.

### 12.5 Extended offline periods

- If the event cursor remains within retention, replay events and reconcile affected spaces.
- If it falls outside retention, fetch the current manifest and reconcile every space from CRDT state vectors.
- Event-log retention expiry must not make old offline edits unusable.
- A compacted server snapshot must still generate the update missing from an old client.

## 13. Required scenario catalog

### 13.1 Single browser, single tab

- Open online and load the latest server state.
- Open offline and load the latest IndexedDB state.
- Edit online and receive a server acknowledgement.
- Edit offline and preserve changes across reload.
- Close immediately after local commit but before upload.
- Crash during an IndexedDB transaction.
- Lose the response after the server committed successfully.
- Switch spaces while reconciliation is in flight.
- Suspend and resume the computer.
- Recover after IndexedDB quota or corruption errors without claiming success.

### 13.2 Multiple tabs in one browser

- Open a new tab and immediately hydrate the latest local generation.
- Edit one tab and update all sibling tabs while offline.
- Edit different notes concurrently.
- Edit different fields of the same note concurrently.
- Edit the same text concurrently.
- Change the same scalar field concurrently.
- Delete in one tab while editing in another.
- Create, rename, archive, restore, or delete spaces from different tabs.
- Close the coordinator and elect another tab.
- Suspend the coordinator and fence it out after lease expiry.
- Deliver duplicate or reordered broadcast messages.
- Recover a tab that missed messages while backgrounded.
- Operate when `BroadcastChannel` or Web Locks is unavailable.
- Prevent duplicate server effects when multiple tabs briefly believe they are coordinator.

### 13.3 Multiple browsers on one device

- Sign into the same account independently in Chrome, Helium, Safari, or Firefox.
- Bootstrap a browser with no local database from complete server state.
- Edit one browser and observe another without manual refresh.
- Keep one browser offline while another continues syncing.
- Reconnect the offline browser and merge both sides.
- Concurrently edit different and identical fields.
- Expire the session in only one browser.
- Sign out one browser without changing another.
- Clear one browser's local data and rebuild from the server.
- Run with different browser backgrounding and SSE policies.

### 13.4 Multiple physical devices

- Bootstrap phone, tablet, and desktop from one account.
- Edit desktop and receive the change on phone.
- Edit phone and receive the change on desktop.
- Edit multiple devices concurrently while online.
- Edit multiple devices independently while offline, then reconnect in any order.
- Reconnect after hours, days, or longer than event retention.
- Reconnect a device with a badly skewed clock.
- Delete content and ensure an old device cannot resurrect it.
- Reinstall the application/browser and rebuild from server state.
- Lose a device without blocking remaining devices.

### 13.5 Network, proxy, and server failures

- Flaky Wi-Fi and rapid online/offline transitions.
- DNS failure, TLS failure, timeout, and connection reset.
- ngrok tunnel restart or temporary endpoint outage.
- API restart during reconciliation.
- PostgreSQL restart or transaction rollback.
- 429 rate limiting with `Retry-After`.
- 502, 503, and 504 retries with jitter.
- SSE buffering, disconnect, missed events, duplicate events, and cursor reset.
- Responses delivered late or out of order.
- Server commit followed by lost response.
- Multiple API instances processing requests and notifications.

### 13.6 Authentication and account boundaries

- Valid sign-in on a new browser.
- Missing session cookie despite local sign-in markers.
- Access-token expiry during reconcile.
- Refresh-token expiry.
- Concurrent refresh attempts from multiple requests/tabs.
- User A signs out and User B signs in on the same browser.
- Organization/account switching.
- Late User A response arrives after switching to User B.
- Guest-to-account adoption and rejection of repeated adoption.
- No cross-account reads, writes, events, or local-cache display.

### 13.7 Billing and entitlement

- Active entitlement permits normal synchronization.
- Checkout is pending while local editing continues.
- Payment failure enters a configured grace period.
- Grace expiry pauses cloud synchronization.
- Cancellation at period end continues until the entitlement deadline.
- Immediate cancellation pauses cloud sync without deleting data.
- Reactivation flushes all queued local work.
- Refund and dispute state changes follow server-owned policy.
- Browser tampering cannot bypass server entitlement checks.
- Payment return URLs never grant access without verified server state.

## 14. Conflict and convergence policy

“Deterministic convergence” means all replicas that receive the same operations reach byte-equivalent logical state, even if operations arrived in different orders. It does not mean every conflicting intention can remain visible simultaneously.

| Concurrent operation | Required result |
|---|---|
| Edit different notes | Preserve both |
| Edit different fields on one note | Preserve both |
| Insert text at different positions | Preserve both using CRDT text |
| Insert text at the same position | Preserve both in deterministic CRDT order |
| Delete text while another device edits nearby | Apply CRDT text semantics consistently |
| Set the same scalar field differently | One deterministic value wins; do not use wall-clock time |
| Move the same note differently | One deterministic position wins |
| Create items with the same visible name | Preserve both because IDs differ |
| Edit versus delete | Deletion tombstone wins by default |
| Delete versus restore | Only an explicit causally newer restore may revive the item |
| Rename versus space delete | Delete wins |
| Archive versus rename | Preserve rename metadata while archived |
| Two space renames | Lexicographically greater operation ID wins; losing operation is acknowledged as superseded and recorded diagnostically |

Recommended field representation:

- Use CRDT text types for note bodies and titles where concurrent character-level editing matters.
- Use independent CRDT map entries for color, geometry, membership, and other scalar fields.
- Use stable fractional/index CRDT ordering for collections where order is user-visible.
- Keep deletion tombstones inside synchronized state.
- Never infer deletion merely from absence in an old snapshot.

## 15. Metadata synchronization

Space metadata uses explicit idempotent operations:

- create;
- rename;
- archive;
- unarchive;
- delete;
- restore.

Every operation has a mutation ID and expected metadata version. Registration must not implicitly rename or restore an existing space.

Metadata rules:

- Delete is durable and wins over ordinary rename/archive retries.
- Restore is explicit and creates a newer version.
- Newly created offline spaces retain their globally unique IDs when uploaded.
- Newly created remote spaces appear locally after manifest refresh.
- The authenticated space manifest carries metadata only; document content is
  fetched through CRDT reconciliation so large documents cannot block space
  discovery.
- Tombstones remain available for the supported offline and recovery windows.
- Metadata updates create durable account events.

For concurrent metadata operations that target the same version, the server
uses the stable operation ID as the tie-breaker. The greater operation ID
wins regardless of which API instance receives it first. A losing operation is
not retried forever: the client applies the winning remote metadata, removes
the superseded outbox entry, and records the conflict for diagnostics.

## 16. Coordinator state machine

Required states:

```text
loading_local
guest_local_only
checking_session
preparing_account_namespace
disabled_not_entitled
starting
online_idle
syncing
offline_waiting
auth_refreshing
paused_auth
paused_billing
degraded
permanent_local_error
stopped
```

Required triggers:

- successful local commit;
- cross-tab update or wake-up;
- browser `online` event;
- visibility/focus regained;
- coordinator lease acquired or lost;
- SSE event, open, reconnect, or cursor reset;
- periodic safety timer;
- session refresh success/failure;
- entitlement version change;
- explicit user retry or “sync now.”

Only one reconciliation may run per space at a time. Multiple spaces may reconcile with bounded concurrency, prioritizing the visible space.

## 17. Retry and failure classification

| Failure class | Examples | Required behavior |
|---|---|---|
| Offline/transient | Network error, timeout, 502/503/504 | Preserve exact in-flight payload; exponential retry with jitter |
| Rate limited | 429 | Preserve payload and honor `Retry-After` |
| Authentication | 401 | One single-flight refresh; retry once; then pause auth |
| Entitlement | Documented 403 codes | Preserve dirty state and pause network sync |
| Metadata conflict | Version conflict | Fetch metadata and apply documented conflict rule |
| Missing space | 404 | Refresh manifest; create only with proof of pending local creation |
| Payload too large | 413 | Preserve/export; compact or chunk; never discard |
| Invalid update/schema | 422 | Quarantine request metadata, show actionable error, do not loop |
| Mutation ID reused | 409 | Permanent diagnostic conflict; never mutate payload under that ID |
| Local storage failure | Quota, transaction failure, corruption | Do not claim local save; retry/export/recovery flow |

Older responses must be rejected if their principal, coordinator fencing token, run generation, space identity, or request identity is no longer current.

## 18. Authentication and stable test origin

Cross-browser synchronization requires a real authenticated server session in every browser.

- Serve frontend and API through one stable HTTPS origin so cookies are first-party.
- Keep a stable test hostname for auth callback allowlists and payment return URLs.
- Use `Secure`, `HttpOnly`, host-only cookies with `Path=/` and an intentional `SameSite` policy.
- Use credentials on fetch requests and SSE.
- Verify `/auth/session` before entering an account namespace.
- Distinguish `SESSION_REQUIRED` from offline and server-unavailable states.
- Never show a blank guest placeholder as if it were the authenticated server workspace.
- Centralize refresh using a single-flight lock.
- Restart reconciliation and SSE after successful refresh.
- Preserve local pending work after terminal session expiry.
- Protect cookie-authenticated mutations using strict origin/CSRF policy.

The session diagnostic endpoint should reveal safe facts such as authenticated status, account ID, cookie presence category, and entitlement version without exposing tokens.

## 19. Billing integration

Billing controls permission to use cloud synchronization, not permission to edit locally.

- The server enforces entitlement on every sync endpoint.
- Verified webhook/provider state is authoritative.
- Checkout return query parameters never grant access.
- Pending, grace, paused, canceled, expired, refunded, and disputed states have explicit sync policies.
- When cloud sync pauses, local edits continue entering the durable outbox.
- When access resumes, reconciliation sends accumulated work normally.
- Server data is retained for the documented retention window and is never deleted synchronously from a billing webhook.
- Entitlement changes increment a version delivered through account status and event hints.

The detailed lifecycle and operations policy remains in `SYNC_AUTH_BILLING_ROBUSTNESS_PLAN.md` and `SYNC_AUTH_BILLING_RUNBOOK.md`.

## 20. Data retention and compaction

- Compact CRDT history only after writing a durable replacement snapshot.
- Browser/server `SpaceDoc` instances preserve Yrs deleted blocks by default;
  compaction is an explicit retention-gated operation, never an incidental
  edit-time side effect.
- The server compaction worker locks each idle document, removes only
  application tombstones older than `SYNC_TOMBSTONE_RETENTION_SECONDS`, forces
  Yrs GC, records `compacted_at`, and commits the replacement snapshot before
  replay rows are pruned.
- Each compaction run and replaced snapshot increments bounded server metrics;
  `compacted_at` is the per-document audit checkpoint.
- Keep enough event history for the normal reconnect window.
- Treat event expiry as a manifest/full-reconcile trigger, not data loss.
- Preserve document tombstones long enough for the maximum supported offline period, backup restoration window, and account retention policy.
- Test reconciliation from state vectors older than the latest compaction.
- Bound document, update, outbox, and response sizes.
- Never turn an oversized snapshot into an unsendable recovery mutation;
  preserve the original outbox and surface the repair/export requirement.
- Provide a safe export before any unrecoverable local migration or repair.
- When the canonical IndexedDB snapshot is corrupt or a local write is
  blocked, the repair-state export must preserve the active principal's raw
  namespaced records, durable outbox/inbox rows, and app-scoped local-storage
  metadata; the normal workspace export is not sufficient for this path.
- Run cleanup asynchronously with metrics and audit records.

The product must explicitly decide and document:

- maximum supported offline duration before special recovery is required;
- sync event retention;
- tombstone retention;
- canceled-account server-data retention;
- maximum document/update sizes;
- compaction thresholds.

Recommended initial server retention for canceled paid access is 90 days, subject to product and legal review.

## 21. Schema evolution and migration

### 21.1 Document schema

- Store an explicit schema version in every CRDT document.
- Maintain an ordered migration registry.
- Migrations are deterministic and idempotent.
- Unknown future schemas stop with `CLIENT_UPGRADE_REQUIRED` rather than being interpreted incorrectly.
- Migration itself produces a normal CRDT update and is synchronized.

### 21.2 Existing local data

1. Read legacy workspace and CRDT records without modifying them.
2. Create a recoverable export/backup marker.
3. Assign globally unique IDs where legacy numeric IDs exist.
4. Build the canonical document and preserve UUID-backed tombstones and
   relationships, retaining numeric aliases only for compatibility.
5. Write the new principal-scoped record atomically.
6. Validate the new snapshot before marking migration complete.
7. Retain legacy records through a rollback/support window.
8. Ensure interruption at every step can be retried safely.

### 21.3 Existing server data

- Apply additive database migrations before deploying dependent code.
- Migration `0021_space_stable_ids.sql` adds a durable UUID identity to each
  existing space using a deterministic backfill, while keeping the legacy
  numeric key as a compatibility alias during rollout.
- New space registration accepts the client UUID so offline-created spaces do
  not receive a second identity when they first reach the server.
- Sync requests/responses echo and verify the canonical UUID beside the
  numeric route alias; a mismatch is denied before document merge.
- Maintain temporary legacy-ID mappings if server records already use numeric IDs.
- Backfill canonical snapshots and state vectors.
- Do not indefinitely support mixed identifier formats.
- Validate migration against empty, representative legacy, and production-sized fixtures.

The repository's `make migration-acceptance` gate covers the empty,
representative legacy, and synthetic capacity-sized fixtures in disposable
PostgreSQL containers. The capacity fixture contains 1,000 spaces and 10,000
durable updates/events; a final production-sized run remains a deployment
capacity exercise with production-like data volume and timing.

## 22. Security and privacy

- Derive account ownership only from verified server sessions.
- Enforce authorization on manifest, reconcile, metadata, event, and diagnostic routes.
- Validate request sizes before expensive decoding.
- Rate-limit by account and IP with bounded registries.
- Validate CRDT updates in a bounded worker/transaction path.
- Use strict CORS and origin/CSRF protections.
- Never log access tokens, refresh tokens, cookies, raw document content, or full sensitive webhook payloads.
- Encrypt transport and server backups.
- Use least-privilege database credentials and audited deletion/export workflows.
- Fuzz malformed updates, base64, JSON, IDs, cookies, and protocol versions.

End-to-end encryption is a separate architectural decision. If required, it must be designed before finalizing server-side CRDT merging and compaction because an opaque server cannot inspect encrypted document updates. Obsidian-like reliability does not by itself require duplicating Obsidian's encryption design.

## 23. User-visible status and recovery

Display local durability and cloud acknowledgement separately.

Required states:

- loading local data;
- saved locally;
- local only;
- checking account;
- syncing, with pending count;
- synced at a specific time;
- offline—changes saved locally;
- sign-in expired—changes saved locally;
- payment attention—changes saved locally;
- sync degraded—automatic retry scheduled;
- sync error—retry or export diagnostics;
- local storage error—data may exist only in this open tab.

Provide:

- “sync now”;
- pending change and space counts;
- last successful acknowledgement time;
- safe local export;
- retry after auth/billing recovery;
- explicit metadata conflict UI where required;
- explicit guest-workspace adoption choice.

## 24. Diagnostics and observability

### 24.1 Development diagnostics panel

Expose safe, copyable diagnostics:

- principal category and account ID;
- device and tab IDs;
- coordinator ownership and fencing token generation;
- browser online/visibility state;
- IndexedDB schema and document schema versions;
- active space ID;
- local and acknowledged generations;
- outbox count and oldest pending age;
- last known server sequence;
- last reconcile start/result/error;
- SSE state, cursor, and last event time;
- session and entitlement status;
- storage errors and migration state.

Do not expose secrets or document contents.

### 24.2 Server metrics

- reconciliation attempts, outcomes, and latency by failure class;
- update and snapshot sizes;
- duplicate requests and mutation conflicts;
- dirty-client age reports without content;
- SSE connections, reconnects, replay counts, and cursor resets;
- event-log lag and retention cleanup;
- PostgreSQL lock and transaction latency;
- authentication rejection and refresh outcomes;
- entitlement denials and transitions;
- metadata conflicts;
- migration and compaction outcomes.

Use correlation IDs across client diagnostics, API requests, database transactions, events, and entitlement decisions.

The current client emits a bounded `x-request-id` on API calls and exposes a
safe in-app diagnostics panel with principal, device/tab, active space,
online state, coordinator/fencing mode, cursor, pending count, status, and
last acknowledgement/request correlation ID. The authenticated `/auth/diagnostics` route returns
only account/session-presence, entitlement-version/access-mode, and server
clock facts. The server echoes the request ID and includes it in
authentication rejection logs; document contents, cookies, and credentials
remain excluded.

The authenticated API limiter now applies separate bounded account and client-IP
budgets for sync, metadata, entitlement, diagnostics, and billing routes. The
reverse proxy overwrites `X-Real-IP` and `X-Forwarded-For` with the connecting
address, while malformed or absent client-address headers collapse into one
bounded `unknown` bucket. This prevents a single hot account or a rotating
untrusted header value from bypassing the intended protection.

## 25. Test strategy

### 25.1 CRDT unit and property tests

- first full update into an empty replica;
- incremental update from a known state vector;
- duplicate and reordered updates;
- randomized two-, three-, and many-replica delivery orders;
- concurrent edits to different notes and fields;
- concurrent CRDT text editing;
- same scalar field conflict;
- edit/delete and delete/restore;
- ordering and grouping conflicts;
- snapshot encode/decode and compaction;
- unknown schema handling;
- globally unique ID generation under simulated devices.

Every convergence test must compare final logical documents across all replicas, not merely HTTP success.

### 25.2 IndexedDB and tab tests

- atomic edit/snapshot/outbox transaction;
- sibling inbox rows are acknowledged individually, and a late snapshot
  cannot clear unrelated durable inbox updates;
- crash before and after transaction commit;
- write ordering under rapid edits;
- multiple tabs reading and writing the same space;
- actual update delivery through `BroadcastChannel`;
- missed message recovery from generations;
- coordinator election, handoff, and fencing;
- two coordinators temporarily active;
- background/suspended tabs;
- no `BroadcastChannel` or Web Locks support;
- quota exhaustion and corrupt records;
- an empty authenticated manifest projection never erases a populated local
  board projection;
- principal switching while writes are in flight.

### 25.3 API and PostgreSQL integration tests

- atomic merge, acknowledgement, and event commit;
- duplicate request with identical bytes;
- duplicate request ID with changed bytes;
- lost-response retry;
- concurrent reconcile against one space;
- multi-instance processing;
- cross-account denial for every route;
- entitlement enforcement;
- payload and rate limits;
- metadata version conflicts and tombstones;
- event replay, retention expiry, and reset;
- server and PostgreSQL restart around transaction boundaries;
- migration from legacy fixtures.

### 25.4 Browser end-to-end matrix

Run the scenario catalog in:

- two tabs in Chrome;
- Chrome and Helium on the same computer;
- at least one WebKit browser;
- desktop plus phone;
- online, offline, and throttled/flaky network modes;
- authenticated, expired-session, and billing-paused states.

Automated assertions must verify document equality, outbox acknowledgement, server snapshot correctness, and truthful status—not only visual rendering.

### 25.5 Chaos and longevity tests

- randomized dropped, duplicated, delayed, and reordered messages;
- browser termination after every local transaction boundary;
- server termination after every reconciliation transaction boundary;
- long offline periods exceeding event retention;
- high-frequency editing and many spaces;
- snapshot compaction while old clients remain offline;
- device and server clock skew;
- backup restoration followed by client reconciliation;
- repeated auth expiry and tunnel interruption.

## 26. Implementation plan

### Phase 0 — Freeze behavior and reproduce failures

- Document protocol and conflict decisions.
- Add failing tests for current multi-tab, fresh-browser bootstrap, missing-cookie, missed-SSE, and offline convergence bugs.
- Add feature flags for the new sync engine.
- Preserve export and rollback paths.

Exit criteria: every known critical failure is reproducible automatically or through a precise manual fixture.

### Phase 1 — Canonical local CRDT and storage

- Refactor all editor mutations through CRDT transactions.
- Replace competing UI/workspace sources of truth with CRDT-derived projections.
- Add principal-scoped atomic IndexedDB records and durable generations.
- Introduce globally unique IDs and local migration.
- Add local-storage failure UX and export.

Exit criteria: single-tab offline edits survive reload/crash and no projection can overwrite a newer CRDT document.

### Phase 2 — Offline multi-tab replication

- Broadcast actual CRDT updates from startup for guest and account namespaces.
- Apply, persist, and render received updates.
- Add Web Locks coordinator election with fenced lease fallback.
- Add focus/generation fallback for missed messages.

Exit criteria: two tabs converge while the API is unavailable, and coordinator handoff loses no changes.

### Phase 3 — Reconciliation protocol and server transaction

- Add `/sync/v2` contracts.
- Implement frozen in-flight payloads and idempotent replay.
- Implement transactional PostgreSQL CRDT merge, acknowledgements, and state-vector diff.
- Reconcile active space first and all spaces afterward.

Exit criteria: offline edits on two installations converge after reconnect in any order, including after a lost response.

### Phase 4 — Metadata and durable event delivery

- Add explicit idempotent metadata operations and tombstones.
- Add durable account event sequence and replay.
- Convert SSE to wake-up-only behavior.
- Add reset/full-reconcile and periodic safety reconciliation.
- Verify proxy configuration for unbuffered streaming.

Exit criteria: metadata and document changes converge after missed or unavailable SSE without refresh.

### Phase 5 — Authentication and namespace correctness

- Centralize verified session identity and refresh.
- Enforce same-origin credential behavior on the stable HTTPS test domain.
- Implement guest adoption and strict account switching.
- Make missing/expired sessions visible instead of falling back silently to an empty workspace.

Exit criteria: Chrome, Helium, and phone bootstrap the same account state, while different accounts never see or upload each other's local data.

### Phase 6 — Billing, retention, and recovery

- Enforce server-owned entitlement on all sync routes.
- Preserve dirty local work through pause and reactivation.
- Implement retention, retention-gated CRDT compaction, quota, and
  oversized-document recovery. Compaction must run asynchronously and expose
  an operational result rather than blocking foreground reconciliation.
- Complete webhook-driven entitlement transitions and recovery wake-ups.

Exit criteria: every billing transition has the documented sync behavior and no transition deletes pending work.

### Phase 7 — Observability and production hardening

- Add the diagnostics panel, metrics, correlation IDs, dashboards, and alerts.
  The panel/request-ID baseline is implemented; deployment dashboards and
  alert rules are checked in under `deploy/observability`, including
  `grafana-task-space.json`, while installation,
  alert delivery, and a full last-failure export remain operational acceptance
  work.
- Complete load, chaos, migration, backup/restore, and security tests.
- Establish browser/device compatibility and performance budgets.

Exit criteria: failures are detectable, attributable, recoverable, and covered by an operational runbook.

### Phase 8 — Staged rollout

- Apply additive migrations before dependent application code.
- Enable the new engine for internal accounts.
- Compare client/server convergence diagnostics during a shadow period where safe.
- Roll out by staged account percentages.
- Keep the old path available only for bounded rollback.
- Stop rollout on unexplained divergence, cross-account risk, or unacknowledged-data loss.
- Retire old queues, snapshots, and protocol only after migration evidence and the support window.

Exit criteria: the full definition of done is met in production-like environments and no unresolved divergence remains.

## 27. Suggested implementation slices

1. Sync invariants, protocol types, and failing regression tests.
2. Global IDs and schema migration registry.
3. Principal-scoped IndexedDB repository.
4. Canonical CRDT editor transaction pipeline.
5. Multi-tab update messages and UI projection refresh.
6. Coordinator election, fencing, and fallback recovery.
7. Reconciliation request/response and durable in-flight payload.
8. PostgreSQL transactional merge and idempotent replay.
9. Complete-space bootstrap and background reconciliation.
10. Metadata operations, versions, and tombstones.
11. Durable event log, SSE hints, replay, and reset.
12. Session verification, refresh coordination, and namespace switching.
13. Entitlement pause/resume and retention behavior.
14. Compaction, limits, diagnostics, and observability.
15. Cross-browser/device end-to-end and chaos suite.
16. Migration tooling, staged rollout, rollback, and cleanup.

Every slice must include tests, failure behavior, migration implications, user-visible status, metrics, and rollback notes.

## 28. Product decisions to lock before implementation

Recommended defaults are included so implementation can proceed without ambiguity.

| Decision | Recommended default |
|---|---|
| Note text conflict model | Character-level CRDT text |
| Scalar conflicts | Native deterministic CRDT map ordering |
| Edit versus delete | Delete wins; explicit restore required |
| Rename conflicts | Deterministic winner plus conflict diagnostic |
| Same-browser network ownership | One coordinator per principal |
| SSE role | Wake-up only |
| Safety reconciliation | On startup, online, focus, SSE reconnect, and periodic timer |
| Guest adoption | Explicit one-time user choice |
| Billing pause | Keep editing and queue indefinitely within local storage limits |
| Canceled-account server retention | 90 days initially |
| E2EE | Separate follow-up decision before server merge is finalized |

## 29. Definition of done

Sync is complete only when all of the following are true:

- One canonical CRDT document drives each space's UI and persistence.
- Every accepted edit is durably local before being reported as saved.
- Two tabs synchronize while completely offline.
- Coordinator loss and temporary dual coordinators do not lose or duplicate effects.
- A fresh authenticated browser or phone receives every server space and its content.
- Multiple offline devices can reconnect in any order and converge without snapshot overwrite.
- Concurrent text, scalar, ordering, deletion, restoration, and metadata behavior matches documented tests.
- Lost, duplicated, reordered, and delayed requests or events are harmless.
- Missing SSE for an arbitrary period still self-heals.
- A stale event cursor and compacted server history still permit full reconciliation.
- Auth expiry, logout, account switching, and organization switching cannot leak or erase data.
- Billing pause and reactivation preserve and later flush local changes.
- Deleted content cannot be resurrected by stale replicas.
- Local storage, migration, oversized-data, and corrupt-data failures provide a recovery/export path.
- All protected routes enforce server-derived account ownership and entitlement.
- The browser status accurately distinguishes local save from server acknowledgement.
- Diagnostics can identify principal, coordinator, generations, outbox, server cursor, SSE, auth, entitlement, and last failure without exposing content or secrets.
- Unit, property, IndexedDB, PostgreSQL, multi-tab, cross-browser, mobile, chaos, migration, and backup/restore gates pass.
- Production dashboards, alerts, retention jobs, incident procedures, and rollback have designated owners.
- The legacy sync path is removed only after staged rollout proves there is no unresolved divergence or data loss.
