#!/usr/bin/env bash
set -euo pipefail

# Exercise the migration path without touching the Compose PostgreSQL volume.
# The empty database proves a clean install; the legacy database deliberately
# has the pre-0020 schema and representative rows so the additive backfills
# are exercised before the normal durable-sync acceptance test runs.
repo_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
acceptance_network="${TASK_SPACE_MIGRATION_NETWORK:-task-space-migration-${run_id}}"
database_container="${TASK_SPACE_MIGRATION_CONTAINER:-task-space-migration-postgres-${run_id}}"
database_volume="${TASK_SPACE_MIGRATION_VOLUME:-task-space-migration-postgres-data-${run_id}}"
database_user="restore"
database_password="restore-local-only"
database_name="taskspace"
database_url="postgres://${database_user}:${database_password}@${database_container}:5432/${database_name}"
sized_space_count="${TASK_SPACE_MIGRATION_SIZED_SPACES:-1000}"
sized_update_count="${TASK_SPACE_MIGRATION_SIZED_UPDATES:-10000}"
migration_started_at=$SECONDS

[[ "$sized_space_count" =~ ^[1-9][0-9]*$ ]]
[[ "$sized_update_count" =~ ^[1-9][0-9]*$ ]]

cleanup() {
  docker rm -f "$database_container" >/dev/null 2>&1 || true
  docker network rm "$acceptance_network" >/dev/null 2>&1 || true
  docker volume rm "$database_volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker network create "$acceptance_network" >/dev/null
docker volume create "$database_volume" >/dev/null
docker run -d \
  --name "$database_container" \
  --network "$acceptance_network" \
  --volume "$database_volume:/var/lib/postgresql/data" \
  -e POSTGRES_DB="$database_name" \
  -e POSTGRES_USER="$database_user" \
  -e POSTGRES_PASSWORD="$database_password" \
  postgres:17-alpine >/dev/null

for attempt in $(seq 1 30); do
  if docker exec "$database_container" pg_isready -U "$database_user" -d "$database_name" >/dev/null 2>&1; then
    break
  fi
  test "$attempt" -lt 30
  sleep 1
done

run_postgres_test() {
  local target_url="$1"
  docker run --rm \
    --network "$acceptance_network" \
    --volume "$repo_dir:/src" \
    --volume task-space-rust-target:/src/target \
    --volume task-space-rust-cargo:/cargo-cache \
    --volume task-space-rustup:/rustup-cache \
    --workdir /src \
    --env CARGO_HOME=/cargo-cache \
    --env RUSTUP_HOME=/rustup-cache \
    --env RUSTUP_TOOLCHAIN=1.96.0 \
    --env TASK_SPACE_TEST_DATABASE_URL="$target_url" \
    rust:1.96-bookworm \
    sh -ceu 'cargo test -p task-server --test postgres_sync -- --ignored --nocapture'
}

# This starts with an entirely empty database, so the test's store.migrate()
# applies every migration from 0001 through the current version.
run_postgres_test "$database_url"

legacy_database="legacy_taskspace"
docker exec "$database_container" psql -U "$database_user" -d postgres -v ON_ERROR_STOP=1 \
  -c "CREATE DATABASE ${legacy_database}"
legacy_url="postgres://${database_user}:${database_password}@${database_container}:5432/${legacy_database}"

# Apply only the pre-0020 SQL files without creating SQLx's migration ledger.
# The application migrator must therefore see and upgrade this as a legacy
# database rather than treating it as an already-current installation.
docker run --rm \
  --network "$acceptance_network" \
  --volume "$repo_dir:/src:ro" \
  --workdir /src \
  --env DATABASE_URL="$legacy_url" \
  --env SIZED_SPACE_COUNT="$sized_space_count" \
  --env SIZED_UPDATE_COUNT="$sized_update_count" \
  postgres:17-alpine \
  sh -ceu '
    for migration_file in /src/crates/server/migrations/*.sql; do
      migration_name="$(basename "$migration_file")"
      case "$migration_name" in
        000*.sql|001*.sql) : ;;
        *) break ;;
      esac
      psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$migration_file"
    done
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
      -v sized_space_count="$SIZED_SPACE_COUNT" \
      -v sized_update_count="$SIZED_UPDATE_COUNT" <<\SQL
INSERT INTO spaces (account_id, space_id, name, created_at, updated_at)
VALUES ($$legacy-fixture$$, 42, $$legacy fixture$$, NOW() - INTERVAL $$2 days$$, NOW() - INTERVAL $$1 day$$);
INSERT INTO crdt_documents (account_id, space_id, snapshot)
VALUES ($$legacy-fixture$$, 42, decode($$00$$, $$hex$$));
INSERT INTO crdt_updates (account_id, space_id, mutation_id, update, metadata, event_kind)
VALUES ($$legacy-fixture$$, 42, $$legacy-mutation$$, decode($$00$$, $$hex$$), $${"fixture":true}$$, $$document$$);
INSERT INTO spaces (account_id, space_id, name, created_at, updated_at)
SELECT $$sized-fixture$$, space_number, $$sized space $$ || space_number,
       NOW() - INTERVAL $$2 days$$, NOW() - INTERVAL $$1 day$$
FROM generate_series(1, :sized_space_count) AS space_number;
INSERT INTO crdt_documents (account_id, space_id, snapshot)
SELECT $$sized-fixture$$, space_number, decode($$00$$, $$hex$$)
FROM generate_series(1, :sized_space_count) AS space_number;
INSERT INTO crdt_updates (account_id, space_id, mutation_id, update, event_kind)
SELECT $$sized-fixture$$,
       ((update_number - 1) % :sized_space_count) + 1,
       $$sized-mutation-$$ || update_number,
       decode($$00$$, $$hex$$),
       $$document$$
FROM generate_series(1, :sized_update_count) AS update_number;
    INSERT INTO sync_events (event_id, account_id, space_id, event_kind, update)
SELECT event_id, account_id, space_id, event_kind, update
FROM crdt_updates
WHERE account_id = $$sized-fixture$$;
SQL
  '

large_event_count_before="$(docker run --rm --network "$acceptance_network" postgres:17-alpine \
  psql "$legacy_url" -Atqc "SELECT count(*) FROM sync_events WHERE account_id='sized-fixture'")"
test "$large_event_count_before" = "$sized_update_count"
run_postgres_test "$legacy_url"

stable_id="$(docker run --rm --network "$acceptance_network" postgres:17-alpine \
  psql "$legacy_url" -Atqc "SELECT stable_id::text FROM spaces WHERE account_id='legacy-fixture' AND space_id=42")"
claim_count="$(docker run --rm --network "$acceptance_network" postgres:17-alpine \
  psql "$legacy_url" -Atqc "SELECT count(*) FROM sync_mutation_claims WHERE account_id='legacy-fixture' AND space_id=42 AND mutation_id='legacy-mutation'")"
large_space_count="$(docker run --rm --network "$acceptance_network" postgres:17-alpine \
  psql "$legacy_url" -Atqc "SELECT count(*) FROM spaces WHERE account_id='sized-fixture' AND stable_id IS NOT NULL")"
large_claim_count="$(docker run --rm --network "$acceptance_network" postgres:17-alpine \
  psql "$legacy_url" -Atqc "SELECT count(*) FROM sync_mutation_claims WHERE account_id='sized-fixture'")"
large_event_count="$(docker run --rm --network "$acceptance_network" postgres:17-alpine \
  psql "$legacy_url" -Atqc "SELECT count(*) FROM sync_events WHERE account_id='sized-fixture'")"
printf 'Migration fixture counts: legacy stable_id=%s claims=%s, sized spaces=%s claims=%s events=%s\n' \
  "$stable_id" "$claim_count" "$large_space_count" "$large_claim_count" "$large_event_count_before"
test -n "$stable_id"
test "$claim_count" = 1
test "$large_space_count" = "$sized_space_count"
test "$large_claim_count" = "$sized_update_count"
test "$large_event_count" = 0

migration_elapsed_seconds=$((SECONDS - migration_started_at))
printf 'Migration acceptance elapsed seconds: %s (sized spaces=%s, updates=%s)\n' \
  "$migration_elapsed_seconds" "$sized_space_count" "$sized_update_count"

printf 'Migration acceptance passed for empty, legacy, and sized fixtures (legacy stable_id=%s, claims=%s, sized spaces=%s, claims=%s, events before retention=%s, after retention=%s)\n' \
  "$stable_id" "$claim_count" "$large_space_count" "$large_claim_count" "$large_event_count_before" "$large_event_count"
