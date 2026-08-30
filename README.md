# task-space

A paper-and-sticky-notes style canvas task manager. Local-first, your tasks live in the browser and stay yours. Optional Pro (sync across devices) is paid

## Stack

- Client: Leptos (CSR, Rust→wasm) + trunk + Tailwind
- CRDT: `yrs` / `y-sync` (client & server)
- Server: axum + SQLx (PostgreSQL), WorkOS (User Management, private mode), Dodo Payments
- Hosting: Hetzner VPS, Docker Compose, Caddy + Postgres (backups → Hetzner Storage Box)

## Development

```sh
trunk serve    # app under apps/web (after workspace is scaffolded, M0)
```

## Status
Work in progress
