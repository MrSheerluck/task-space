#!/usr/bin/env bash
set -euo pipefail

umask 077

database_url="${DATABASE_URL:?DATABASE_URL must point at the database to back up}"
backup_dir="${TASK_SPACE_BACKUP_DIR:-./backups}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup_path="${backup_dir}/task-space-${timestamp}.dump"
checksum_path="${backup_path}.sha256"

mkdir -p -- "$backup_dir"
pg_dump --format=custom --no-owner --no-privileges --file="$backup_path" "$database_url"
sha256sum "$backup_path" > "$checksum_path"

printf 'Created %s\n' "$backup_path"
printf 'Created %s\n' "$checksum_path"
