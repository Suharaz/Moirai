#!/usr/bin/env bash
# egress-proxy entrypoint: renders the backup destination allowlist from HDT_BACKUP_S3_ENDPOINT (the
# object storage host is deployment specific), checks the configuration, then runs Squid in the
# foreground. /run/hdt-squid is a tmpfs; the root filesystem stays read-only.
set -euo pipefail

conf=/etc/hdt-squid/squid.conf
backup_list=/run/hdt-squid/backup.txt
mkdir -p "$(dirname "$backup_list")"

endpoint="${HDT_BACKUP_S3_ENDPOINT:-}"
if [[ -n "$endpoint" ]]; then
  # squid.conf denies IP literals (to_ip_literal) and any port but 443 for the backup client, so such an
  # endpoint would only show up hours later as backup_stale: refuse it here, at start.
  host=""
  if [[ "$endpoint" =~ ^https://([A-Za-z0-9.-]+)(:443)?$ ]]; then host="${BASH_REMATCH[1]}"; fi
  if [[ -z "$host" || "$host" =~ ^[0-9.]+$ || "$host" =~ ^0[xX][0-9a-fA-F.xX]*$ ]]; then
    echo "HDT_BACKUP_S3_ENDPOINT must be https://<DNS name>[:443] (no IP address, no other port); got '${endpoint}'" >&2
    exit 1
  fi
  printf '%s\n' "$host" >"$backup_list"
else
  echo "# HDT_BACKUP_S3_ENDPOINT is not set: backups cannot leave the host" >"$backup_list"
fi

squid -k parse -f "$conf"
exec squid -N -d 1 -f "$conf"
