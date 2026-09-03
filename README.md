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
`crates/server/migrations/0001_sync.sql` defines the durable PostgreSQL snapshot
and update log used by the runtime repository. Billing is provider-neutral at
the application boundary: server-owned entitlements, normalized webhook
events, provider-event idempotency, stale event protection, and the
authenticated `/account/entitlement` read boundary are in
`crates/core/src/billing.rs`, `crates/server/src/postgres.rs`, and
`crates/server/migrations/0002_billing.sql`. The Dodo adapter verifies its
signature, maps its customer metadata to an account, normalizes subscription
events, and applies them transactionally to PostgreSQL.

## Auth and billing integration

The server now exposes the provider integration points:

- `/auth/sign-in` and `/auth/sign-up` accept the custom paper UI's email and
  password forms and authenticate through WorkOS's headless AuthKit API.
- `/auth/verify-email` completes the six-digit verification flow for new
  accounts.
- `/auth/password-reset` requests a reset email and
  `/auth/password-reset/confirm` accepts the token from the custom reset page.
- `/auth/callback` exchanges the authorization code and sets rotated HttpOnly
  access/refresh cookies; `/auth/refresh` rotates them again.
- `/webhooks/dodo` verifies Standard Webhooks signatures before accepting Dodo
  subscription events.
- `/billing/checkout` creates a hosted monthly or yearly Dodo checkout for the
  authenticated account and puts the internal account id in provider metadata.
- `/sync/spaces/{space_id}` registers the local space before any update can be
  pushed, preventing arbitrary client ids from creating server documents.
- `DODO_PRO_MONTHLY_PRODUCT_ID` and `DODO_PRO_YEARLY_PRODUCT_ID` map the two
  Pro products to the same sync entitlement.

Copy `.env.example` to `.env` for deployment values. The compose setup keeps
the paper UI static and proxies auth, sync, entitlement, checkout, and webhook
paths to the Rust API container. The API waits for PostgreSQL, runs the checked
in migrations, and does not fall back to browser storage or an in-memory store.

For a local end-to-end run, create `.env` from `.env.example`, fill in WorkOS
and Dodo test-mode values, and add both `http://localhost:3000/auth/callback`
and the production callback URL to the WorkOS client. Start the API and
database, then start Trunk in a second terminal:

```sh
docker compose -f deploy/docker-compose.yml up --build postgres task-space-api
make dev                         # opens the paper UI at http://localhost:8080
```

While running on Trunk, the browser sends auth, checkout, sync, and SSE
requests to `http://localhost:3000`. Production stays same-origin and secure
behind Caddy. The API relaxes the cookie `Secure` flag only for the local HTTP
callback.

For the current localhost setup, configure the WorkOS application redirects as:

- App homepage URL: `http://localhost:8080/`
- Redirect URI: `http://localhost:3000/auth/callback`
- Initiate login URI: leave unset for the normal custom password flow (only set
  this to a dedicated OAuth/impersonation entry route when that is enabled)
- Sign-up URL: `http://localhost:8080/signup`
- Sign-out URI: `http://localhost:8080/`
- Password reset URL: `http://localhost:8080/reset-password`

The paper-branded sign-in, sign-up, forgot-password, reset-password, and email
verification pages are `/signin`, `/signup`, `/forgot-password`,
`/reset-password`, and `/verify-email`. Their forms call the secure API routes
above; WorkOS credentials and session tokens stay server-side. Add the matching
production URLs when deploying. Dodo should send the subscription lifecycle
events listed in its webhook dashboard; the browser should only enable sync
after the verified webhook has updated the server entitlement.

For launch pricing, the recommended starting point is **$2/month or
$20/year**. The yearly plan is roughly a 17% discount while $1/month leaves too
little room after payment fees and support. A $1/month founder offer can still
work as a time-limited early-access price, but it should not be the permanent
Pro price.

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
