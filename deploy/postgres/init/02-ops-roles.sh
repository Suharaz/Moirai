#!/usr/bin/env bash
# Operations roles of the production database (docker-compose.yml), created or brought up to date.
# Runs at cluster init right after 01-roles.sh, and by hand on an existing cluster (docs/runbook.md
# section 13): every statement is idempotent and converges the role to the attributes below, password
# included, so re-running it is how an existing cluster picks up a changed attribute.
#
# - hdt_backup (HDT_PG_PASSWORD_BACKUP or HDT_PG_PASSWORD_BACKUP_FILE): REPLICATION for pg_basebackup;
#   pg_read_all_data and BYPASSRLS for pg_dump, which runs with row_security=off and refuses to dump a
#   table with row level security (secrets, store) unless the role bypasses it. Read-only by default.
# - hdt_monitor (HDT_PG_PASSWORD_MONITOR or HDT_PG_PASSWORD_MONITOR_FILE): pg_monitor for
#   postgres-exporter. Read-only by default.
# A role whose password is not configured is skipped (the dev stack configures neither); a configured but
# empty or unreadable password file is an error.
# Replication connections of hdt_backup match only a `replication` line of pg_hba.conf, so the line is
# added to the local server's file (PGDATA) when this runs inside the Postgres container; run against a
# remote server (PGHOST), only the roles are managed.
#
# The image entrypoint sources this file when it is not executable: no `exit` on the success path and
# every helper carries the hdt_ops_ prefix.
set -euo pipefail

hdt_ops_configured() {  # hdt_ops_configured VAR: VAR or VAR_FILE is set
  local file_var="${1}_FILE"
  [[ -n "${!file_var:-}" || -n "${!1:-}" ]]
}

hdt_ops_password() {  # hdt_ops_password VAR -> the password from VAR_FILE (docker secret) or VAR
  local file_var="${1}_FILE" value
  if [[ -n "${!file_var:-}" ]]; then
    [[ -r "${!file_var}" ]] || { echo "02-ops-roles: ${file_var} is not readable" >&2; return 1; }
    value="$(<"${!file_var}")"
    value="${value%$'\n'}"
  else
    value="${!1:-}"
  fi
  [[ -n "$value" ]] || { echo "02-ops-roles: ${1} is empty" >&2; return 1; }
  printf '%s' "$value"
}

hdt_ops_psql() {
  psql -v ON_ERROR_STOP=1 -X -q --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" "$@"
}

hdt_ops_role() {  # hdt_ops_role ROLE PASSWORD ATTRIBUTES PREDEFINED_ROLE
  hdt_ops_psql -v role="$1" -v password="$2" -v attributes="$3" -v predefined="$4" <<'SQL'
BEGIN;
-- Concurrent runs (the backup test and an operator) serialize here: CREATE ROLE is not race-free.
SELECT pg_advisory_xact_lock(hashtext('hdt_ops_roles')) AS locked \gset
SELECT format('CREATE ROLE %I', :'role')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role') \gexec
SELECT format('ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE %s PASSWORD %L',
              :'role', :'attributes', :'password') \gexec
SELECT format('GRANT %I TO %I', :'predefined', :'role') \gexec
SELECT format('ALTER ROLE %I SET default_transaction_read_only = on', :'role') \gexec
COMMIT;
SQL
}

hdt_ops_backup_hba() {
  local line="host replication hdt_backup all scram-sha-256" hba="${PGDATA:-}/pg_hba.conf"
  if [[ -z "${PGDATA:-}" || ! -f "$hba" ]]; then
    echo "02-ops-roles: no local pg_hba.conf; the server needs the line: ${line}" >&2
    return 0
  fi
  if ! grep -qxF "$line" "$hba"; then
    printf '%s\n' "$line" >>"$hba"
    hdt_ops_psql -c 'SELECT pg_reload_conf()' >/dev/null
    echo "02-ops-roles: added the hdt_backup replication line to ${hba}" >&2
  fi
}

hdt_ops_roles() {
  local password
  if hdt_ops_configured HDT_PG_PASSWORD_BACKUP; then
    password="$(hdt_ops_password HDT_PG_PASSWORD_BACKUP)"
    hdt_ops_role hdt_backup "$password" "REPLICATION BYPASSRLS" pg_read_all_data
    hdt_ops_backup_hba
    echo "02-ops-roles: hdt_backup is up to date" >&2
  fi
  if hdt_ops_configured HDT_PG_PASSWORD_MONITOR; then
    password="$(hdt_ops_password HDT_PG_PASSWORD_MONITOR)"
    hdt_ops_role hdt_monitor "$password" "NOREPLICATION NOBYPASSRLS" pg_monitor
    echo "02-ops-roles: hdt_monitor is up to date" >&2
  fi
}

hdt_ops_roles
