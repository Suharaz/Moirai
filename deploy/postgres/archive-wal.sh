#!/usr/bin/env bash
# archive_command of the production Postgres (docker-compose.yml): `bash archive-wal.sh %p %f`.
# Copies one completed WAL segment (or history / backup label file) into the pg_wal_archive volume, where
# the backup service encrypts and ships it. The copy is fsynced before the atomic rename and the
# directory after it, so a host crash never leaves a truncated segment under its final name: Postgres
# recycles its own copy as soon as this command succeeds.
# A file that is already archived (the server crashed after the rename, before it recorded the success)
# counts as archived only when it is byte-identical; anything else fails and Postgres retries.
set -euo pipefail

[[ $# -eq 2 ]] || { echo "usage: archive-wal.sh %p %f" >&2; exit 2; }
src="$1"
name="$2"
dir="${HDT_WAL_ARCHIVE_DIR:-/var/lib/postgresql/wal_archive}"
dest="${dir}/${name}"

if [[ -f "$dest" ]]; then
  cmp -s "$src" "$dest" || { echo "archive-wal: ${dest} exists with different content" >&2; exit 1; }
  exit 0
fi
cp "$src" "${dest}.partial"
sync "${dest}.partial"
mv "${dest}.partial" "$dest"
sync "$dir"
