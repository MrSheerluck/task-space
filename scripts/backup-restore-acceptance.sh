#!/usr/bin/env bash
set -euo pipefail

# Back up the live local Compose database, verify the artifact, restore it into
# a disposable PostgreSQL container, and run the durable sync acceptance suite
# against the restored target. The live data volume is never a restore target.
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${TASK_SPACE_BACKUP_ENV_FILE:-$repo_dir/.env}"
compose_network="${TASK_SPACE_BACKUP_NETWORK:-deploy_default}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
restore_network="task-space-backup-restore-${run_id}"
restore_container="task-space-backup-postgres-${run_id}"
backup_dir="$(mktemp -d "${TMPDIR:-/tmp}/task-space-backup.XXXXXX")"
database_password="${POSTGRES_PASSWORD:-change-me-local-only}"

cleanup() {
  docker rm -f "$restore_container" >/dev/null 2>&1 || true
  docker network rm "$restore_network" >/dev/null 2>&1 || true
  rm -rf -- "$backup_dir"
}
trap cleanup EXIT

if [[ -f "$env_file" ]]; then
  original_path="$PATH"
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
  PATH="$original_path"
  export PATH
  database_password="${POSTGRES_PASSWORD:-$database_password}"
fi

docker run --rm --network "$compose_network" \
  --volume "$repo_dir:/src:ro" \
  --volume "$backup_dir:/backups" \
  --env DATABASE_URL="postgres://taskspace:${database_password}@postgres:5432/taskspace" \
  --env TASK_SPACE_BACKUP_DIR=/backups \
  postgres:17-alpine \
  sh -ceu '/src/scripts/backup-postgres.sh'

backup_path="$(find "$backup_dir" -type f -name '*.dump' -print -quit)"
if [[ -z "$backup_path" ]]; then
  echo 'backup acceptance did not produce a dump file' >&2
  exit 1
fi

docker run --rm --network "$compose_network" \
  --volume "$repo_dir:/src:ro" \
  --volume "$backup_dir:/backups:ro" \
  postgres:17-alpine \
  sh -ceu "/src/scripts/verify-postgres-backup.sh /backups/$(basename "$backup_path")"

docker network create "$restore_network" >/dev/null
docker run --detach --name "$restore_container" \
  --network "$restore_network" \
  --env POSTGRES_DB=taskspace \
  --env POSTGRES_USER=restore \
  --env POSTGRES_PASSWORD=restore-local-only \
  postgres:17-alpine >/dev/null

for _ in $(seq 1 45); do
  if docker exec "$restore_container" pg_isready -U restore -d taskspace >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$restore_container" pg_isready -U restore -d taskspace >/dev/null

docker run --rm --network "$restore_network" \
  --volume "$repo_dir:/src:ro" \
  --volume "$backup_dir:/backups:ro" \
  --env TASK_SPACE_RESTORE_DATABASE_URL="postgres://restore:restore-local-only@${restore_container}:5432/taskspace" \
  --env TASK_SPACE_RESTORE_CONFIRM=I_UNDERSTAND_RESTORE_IS_DESTRUCTIVE \
  postgres:17-alpine \
  sh -ceu "/src/scripts/restore-postgres.sh /backups/$(basename "$backup_path")"

docker run --rm --network "$restore_network" \
  --volume "$repo_dir:/src" \
  --volume task-space-rust-target:/src/target \
  --volume task-space-rust-cargo:/cargo-cache \
  --volume task-space-rustup:/rustup-cache \
  --workdir /src \
  --env CARGO_HOME=/cargo-cache \
  --env RUSTUP_HOME=/rustup-cache \
  --env RUSTUP_TOOLCHAIN=1.96.0 \
  --env TASK_SPACE_TEST_DATABASE_URL="postgres://restore:restore-local-only@${restore_container}:5432/taskspace" \
  rust:1.96-bookworm \
  sh -ceu 'cargo test -p task-server --test postgres_sync -- --ignored --nocapture'

printf 'Backup/restore acceptance passed without restoring into the live Compose volume\n'
