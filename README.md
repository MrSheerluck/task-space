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
