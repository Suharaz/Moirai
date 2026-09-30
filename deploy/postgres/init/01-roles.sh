#!/usr/bin/env bash
# Creates one Postgres login role per service. Passwords come from HDT_PG_PASSWORD_<ROLE> environment
# variables or, in production, from the docker secret file named by HDT_PG_PASSWORD_<ROLE>_FILE.
# hdt_migrator owns the schema; every other role receives only the grants written in the Alembic
# migration of the phase that owns each table.
#
# The operations roles of production (hdt_backup, hdt_monitor) are managed by 02-ops-roles.sh.
set -euo pipefail

password_of() {
  local var="$1" file_var="${1}_FILE"
  if [[ -n "${!file_var:-}" ]]; then
    local value
    value="$(<"${!file_var}")"
    printf '%s' "${value%$'\n'}"
  elif [[ -n "${!var:-}" ]]; then
    printf '%s' "${!var}"
  else
    return 1
  fi
}

create_role() {
  local role="$1" var="$2"
  local password
  password="$(password_of "$var")" || { echo "missing $var (or ${var}_FILE)" >&2; exit 1; }
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v role="$role" -v password="$password" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'role', :'password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role') \gexec
SQL
}

create_role hdt_migrator HDT_PG_PASSWORD_MIGRATOR
create_role hdt_recorder HDT_PG_PASSWORD_RECORDER
create_role hdt_council HDT_PG_PASSWORD_COUNCIL
create_role hdt_scorer HDT_PG_PASSWORD_SCORER
create_role hdt_risk HDT_PG_PASSWORD_RISK
create_role hdt_execution HDT_PG_PASSWORD_EXECUTION
create_role hdt_news HDT_PG_PASSWORD_NEWS
create_role hdt_veto_scan HDT_PG_PASSWORD_VETO_SCAN
create_role hdt_configapi HDT_PG_PASSWORD_CONFIGAPI
create_role hdt_console_ro HDT_PG_PASSWORD_CONSOLE_RO
create_role hdt_publisher_ro HDT_PG_PASSWORD_PUBLISHER_RO
create_role hdt_telegram HDT_PG_PASSWORD_TELEGRAM

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
ALTER DATABASE hdt OWNER TO hdt_migrator;
ALTER SCHEMA public OWNER TO hdt_migrator;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO hdt_recorder, hdt_council, hdt_scorer, hdt_risk, hdt_execution,
  hdt_news, hdt_veto_scan, hdt_configapi, hdt_console_ro, hdt_publisher_ro, hdt_telegram;
-- The console and the public publisher never write.
ALTER ROLE hdt_console_ro SET default_transaction_read_only = on;
ALTER ROLE hdt_publisher_ro SET default_transaction_read_only = on;
SQL
