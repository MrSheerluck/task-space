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
