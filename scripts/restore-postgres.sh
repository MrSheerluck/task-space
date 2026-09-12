#!/usr/bin/env bash
set -euo pipefail

backup_path="${1:?usage: restore-postgres.sh BACKUP.dump}"
database_url="${TASK_SPACE_RESTORE_DATABASE_URL:?TASK_SPACE_RESTORE_DATABASE_URL must name the restore target}"
confirmation="${TASK_SPACE_RESTORE_CONFIRM:-}"

if [[ "$confirmation" != "I_UNDERSTAND_RESTORE_IS_DESTRUCTIVE" ]]; then
  printf '%s\n' 'Refusing to restore without TASK_SPACE_RESTORE_CONFIRM=I_UNDERSTAND_RESTORE_IS_DESTRUCTIVE.' >&2
  exit 2
fi

test -f "$backup_path"
if [[ -f "${backup_path}.sha256" ]]; then
  sha256sum -c "${backup_path}.sha256"
fi

pg_restore \
  --clean \
  --if-exists \
  --exit-on-error \
  --no-owner \
  --no-privileges \
  --dbname="$database_url" \
  "$backup_path"

printf 'Restored %s into the explicitly supplied target. Run migrations and the PostgreSQL sync smoke test before serving traffic.\n' "$backup_path"
