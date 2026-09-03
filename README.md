# Task Space

A paper-and-sticky-notes style canvas task manager. Local-first, your tasks live in the browser and stay yours. Optional Pro (sync across devices) is paid


## Stack

- Client: Leptos (CSR, Rust→wasm) + trunk + Tailwind
- CRDT: `yrs` / `y-sync` (client & server)
- Server: axum + SQLx (PostgreSQL), WorkOS (User Management, private mode), Dodo Payments
- Hosting: Hetzner VPS, Docker Compose, Caddy + Postgres (backups → Hetzner Storage Box)

## Development

```sh
make dev          # app under apps/web (trunk, :8080); runs `make css` first (Tailwind)
```


```sh
# https://task-space-waitlist.mrsheerluck003.workers.dev  (POST /waitlist, GET /waitlist/count)
cd api/waitlist && npm run dev        # optional: local miniflare on :8787
# deploy after changes: npx wrangler d1 migrations apply task-space-waitlist --remote && npx wrangler deploy
```


## Status
Work in progress

## Data boundary before auth and sync

The browser is currently the source of truth. Workspace documents are stored in
IndexedDB; the old `localStorage` workspace key is retained only as a migration
and recovery fallback. The shared document schema lives in `crates/core` so the
eventual account and sync layers can consume the same shape.

The Yrs migration layer is now present in `crates/core` and the browser stores a
per-space Yrs snapshot beside the JSON workspace record. JSON remains the
readable export/migration projection while editor mutations are moved to
incremental Yrs updates. The server crate contains the transport-neutral merge
engine, account-scoped SSE/HTTP routes, state-vector pulls, and idempotent
mutation handling. The browser has an IndexedDB-backed Yrs update queue,
authenticated HTTP push/retry plumbing, SSE event application, and reconnect
recovery. Sync is opt-in until an authenticated session enables it.
`crates/server/migrations/0001_sync.sql` defines the durable PostgreSQL
snapshot and update log that will replace the in-memory store before production
auth is enabled. Billing preparation is also provider-neutral: server-owned
entitlements, normalized webhook events, provider-event idempotency, stale
event protection, and the authenticated `/account/entitlement` read boundary
are in `crates/core/src/billing.rs`, `crates/server/src/billing.rs`, and
`crates/server/migrations/0002_billing.sql`. A future Dodo adapter only needs to
verify its signature, map its customer to an account, normalize the event, and
apply it to the billing store.

- A **space** owns its name, archive state, notes, groups, note positions, due
  dates, statuses, and deletion tombstones.
- **Workspace** metadata owns the space list, the active space, schema version,
  and the stable local device identifier used during account migration.
- **Device-only UI state** owns pan, zoom, open menus, selection, editing state,
  undo/redo history, and the currently open date picker. It is intentionally not
  part of a workspace export or future sync document.

Workspace exports include every space and can be restored with confirmation.
Older board-only exports remain importable into the current space. Deletions are
kept as tombstones at the persistence boundary so a future sync layer can merge
them instead of silently resurrecting removed content.
