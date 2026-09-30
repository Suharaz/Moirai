#!/bin/sh
# public-store-init (one-shot, docker-compose.yml): prepares the `public-store` MinIO for the public
# data path. Idempotent; runs on every `docker compose up` before public-publisher and dashboard start.
#
# - bucket `hdt-public`, never anonymous, created with object lock, which also turns versioning on: an
#   overwrite (latest.json) or a delete (the publisher's pruning) only adds a version or a delete marker,
#   and every version is kept under a GOVERNANCE lock for HDT_PUBLIC_LOCK_DAYS (default 1 day, the
#   publisher's own retention being 24 h), then expired by lifecycle.json one day after it stopped being
#   current or when its lock ends, whichever is later (delete markers left alone are removed too).
#   Object lock can only be chosen when a bucket is created: an existing bucket without it stops
#   this script (docs/runbook.md section 8 recreates it; the publisher rewrites everything in a minute);
# - user of public-publisher: put / get / delete objects and list the bucket (writer-policy.json), never a
#   version-level delete, a retention change or a governance bypass, so it cannot destroy a published
#   version;
# - user of the dashboard: get objects only (reader-policy.json), so a compromised dashboard can
#   neither write nor delete a snapshot, nor enumerate the bucket.
# Credentials come from docker secrets (files); the root credentials never leave this container and
# public-store itself.
set -eu

secret() {
  value="$(cat "/run/secrets/$1")"
  [ -n "$value" ] || { echo "secret $1 is empty" >&2; exit 1; }
  printf '%s' "$value"
}

BUCKET=hdt-public
LOCK_DAYS="${HDT_PUBLIC_LOCK_DAYS:-1}"
case "$LOCK_DAYS" in
  '' | *[!0-9]* | 0*) echo "HDT_PUBLIC_LOCK_DAYS must be a positive whole number of days" >&2; exit 1 ;;
esac
MC_CONFIG_DIR="$(mktemp -d)"
export MC_CONFIG_DIR
trap 'rm -rf "$MC_CONFIG_DIR"' EXIT

mc --quiet alias set store http://public-store:9000 \
  "$(secret public_store_root_user)" "$(secret public_store_root_password)" >/dev/null
mc --quiet ready store
mc --quiet mb --ignore-existing --with-lock "store/${BUCKET}"
lock="$(mc --quiet --json retention info --default "store/${BUCKET}" 2>&1)" || true
case "$lock" in
  *'"enabled":"Enabled"'*) ;;
  *)
    echo "bucket ${BUCKET} has no object lock (created before it was required); recreate it (runbook section 8)" >&2
    exit 1
    ;;
esac
mc --quiet retention set --default governance "${LOCK_DAYS}d" "store/${BUCKET}" >/dev/null
mc --quiet ilm import "store/${BUCKET}" </etc/hdt-public-store/lifecycle.json >/dev/null
mc --quiet anonymous set none "store/${BUCKET}"

ensure_user() {  # ensure_user POLICY ACCESS_KEY SECRET_KEY POLICY_FILE
  mc --quiet admin policy create store "$1" "$4" >/dev/null
  mc --quiet admin user add store "$2" "$3" >/dev/null
  info="$(mc --quiet admin user info store "$2")"
  case "$info" in
    *"PolicyName: $1"*) ;;
    *) mc --quiet admin policy attach store "$1" --user "$2" >/dev/null ;;
  esac
}

ensure_user hdt-public-writer "$(secret public_store_writer_access_key)" \
  "$(secret public_store_writer_secret_key)" /etc/hdt-public-store/writer-policy.json
ensure_user hdt-public-reader "$(secret public_store_reader_access_key)" \
  "$(secret public_store_reader_secret_key)" /etc/hdt-public-store/reader-policy.json

echo "public-store ready: bucket ${BUCKET} (versioned, ${LOCK_DAYS}-day governance lock), writer and read-only reader users"
