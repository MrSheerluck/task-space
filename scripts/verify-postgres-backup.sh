#!/usr/bin/env bash
set -euo pipefail

backup_path="${1:?usage: verify-postgres-backup.sh BACKUP.dump}"
checksum_path="${backup_path}.sha256"

test -f "$backup_path"
test -f "$checksum_path"
sha256sum -c "$checksum_path"
pg_restore --list "$backup_path" >/dev/null
printf 'Backup is readable and its checksum matches: %s\n' "$backup_path"
