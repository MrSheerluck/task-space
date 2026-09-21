# Task Space

Task Space is a paper-and-sticky-notes task manager built around an infinite
canvas. It is local-first by default: the free board works offline, needs no
account, and stores work in the browser. Pro is optional and adds account-based
cloud sync across devices.

## Product

- Create spaces and arrange tasks on a tactile paper canvas.
- Keep working offline with local browser storage.
- Export and restore workspace data as JSON.
- Add an account only when you want cloud sync.
- Use Pro sync to keep spaces converged across browsers and devices.

The browser remains the local source of truth. Authenticated sync is backed by
PostgreSQL and is enabled only for accounts with an active Pro entitlement.
Local work is not deleted when an account expires or the network is unavailable.

## Stack

- Frontend: Leptos CSR, Rust/WASM, Trunk, and Tailwind CSS
- Local data and sync model: IndexedDB, Yrs, and Y-Sync
- API: Axum and SQLx
- Database: PostgreSQL
- Authentication: WorkOS
- Billing: Dodo Payments
- Deployment: Docker Compose, Nginx, and Caddy

## Self-hosting

### Requirements

- A Linux host with Docker and Docker Compose
- A domain pointing to the host for production HTTPS
- WorkOS credentials for accounts and sessions
- Dodo Payments credentials and Pro product IDs for paid cloud sync

### Configure

Copy the example configuration and replace every placeholder with deployment
values:

```sh
cp .env.example .env
```

At minimum, configure the database values, `TASK_SPACE_ALLOWED_ORIGINS`, and
the WorkOS variables. To enable Pro, also configure:

- `DODO_PAYMENTS_API_KEY`
- `DODO_PAYMENTS_WEBHOOK_KEY`
- `DODO_PAYMENTS_ENVIRONMENT`
- `DODO_PAYMENTS_RETURN_URL`
- `DODO_PRO_MONTHLY_PRODUCT_ID`
- `DODO_PRO_YEARLY_PRODUCT_ID`

Use the public HTTPS origin consistently in `WORKOS_REDIRECT_URI`,
`WORKOS_POST_LOGIN_REDIRECT_URI`, `DODO_PAYMENTS_RETURN_URL`, and
`TASK_SPACE_ALLOWED_ORIGINS`. Register the matching sign-in, sign-up, sign-out,
password-reset, and callback URLs in WorkOS. Configure Dodo to send all
subscription lifecycle events to `/webhooks/dodo`.

### Build and run

Build the frontend, then start PostgreSQL, the API, the static frontend, and
Caddy:

```sh
make css
cd apps/web && trunk build --release
cd ../..
docker compose --env-file .env -f deploy/docker-compose.yml up --build -d \
  postgres task-space-api task-space caddy
```

Update `deploy/caddy/Caddyfile` with your domain before starting Caddy. It
terminates HTTPS and proxies the frontend service. PostgreSQL and the API are
not exposed publicly by the compose file; the API runs on the internal Compose
network and the local frontend port is bound to loopback.

Check the deployment with:

```sh
curl -fsS https://your-domain.example/healthz
curl -fsS https://your-domain.example/readyz
```

The API runs checked-in migrations on startup. Keep the `postgres_data` volume,
and create regular encrypted PostgreSQL backups. The repository includes
`scripts/backup-postgres.sh`, `scripts/verify-postgres-backup.sh`, and
`scripts/restore-postgres.sh` for the backup workflow.

### Local development

For a local UI and API run:

```sh
docker compose -f deploy/docker-compose.yml up --build postgres task-space-api
make dev
```

The UI runs at `http://localhost:8080` and the API at `http://localhost:3000`.
For local auth and checkout testing, use the localhost URLs from `.env.example`
and Dodo test-mode credentials. A payment provider cannot deliver webhooks to a
private localhost address, so use an HTTPS tunnel when testing the complete
checkout-to-entitlement flow.

## Useful commands

```sh
make check                  # check the Rust workspace
make browser-sync           # exercise local-first browser behavior
make browser-authenticated  # exercise authenticated sync behavior
make backup-restore-acceptance
```
