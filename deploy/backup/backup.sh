#!/usr/bin/env bash
# hdt-backup: client-side encrypted backups of the trading database and the lake (phase 10).
#
#   hdt-backup wal       ship archived WAL segments (archive_command copies them into HDT_BACKUP_WAL_DIR),
#                        then delete the local copies             -> wal/<segment>.zst.age
#                        (one run at a time: STATE_DIR/wal.lock)
#   hdt-backup base      physical base backup (pg_basebackup)     -> base/<id>/base.tar.zst.age, settings.conf
#                        (server parameters a recovery must match), SHA256SUMS last
#   hdt-backup logical   logical dump (pg_dump -Fc)               -> logical/<id>/<db>.dump.age, SHA256SUMS last
#   hdt-backup lake      new closed lake files, content-addressed -> lake/objects/<sha256>.age
#                        and the file list of the whole lake     -> lake/manifests/<id>.tsv.age
#   hdt-backup daily     base + logical + lake now
#   hdt-backup restore-tested BASE_ID LOGICAL_ID LAKE_ID
#                        record a passed restore drill (docs/runbook.md section 14); exported as
#                        kind="restore_test" for the overdue-drill alert. Each id must be a set this
#                        host uploaded (STATE_DIR/shipped.tsv, appended by base, logical and lake).
#   hdt-backup daemon    WAL shipping every HDT_BACKUP_WAL_INTERVAL_S seconds in its own process, and the
#                        daily steps at or after HDT_BACKUP_HOUR_UTC: each of base, logical and lake runs
#                        until it succeeds once per UTC day; a failed step is retried with exponential
#                        backoff (HDT_BACKUP_RETRY_BASE_S doubling up to HDT_BACKUP_RETRY_MAX_S) without
#                        repeating the steps that already succeeded that day. HDT_BACKUP_HOUR_UTC (0-23)
#                        and the interval / backoff settings are whole decimal numbers, checked at start.
#
# Everything is encrypted with age to HDT_BACKUP_AGE_RECIPIENTS (space separated age1... keys) before it
# leaves the host; the identity is held offline by the operator (hdt-restore). Uploads use write-only
# credentials and never replace an existing object (lib.sh put_object), so this container can neither
# read back, overwrite nor delete a backup; retention and object lock are bucket policies. SHA256SUMS
# hold the hash of the plaintext stream, checked by hdt-restore.
# Postgres: PGHOST/PGPORT/PGUSER (hdt_backup)/PGDATABASE, password in HDT_PG_PASSWORD_BACKUP(_FILE).
# Status for Prometheus (node-exporter textfile collector): HDT_BACKUP_METRICS_DIR/hdt_backup.prom, with
# the last success and last failure time of every step (deploy/prometheus/alerts.yml, group hdt-backup).
# Each step works in its own directory under HDT_BACKUP_STATE_DIR/work, removed when the step ends,
# whatever the outcome.

LIB_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=deploy/backup/lib.sh
source "${LIB_DIR}/lib.sh"

WAL_DIR="${HDT_BACKUP_WAL_DIR:-/var/lib/postgresql/wal_archive}"
LAKE_DIR="${HDT_BACKUP_LAKE_DIR:-/data/lake}"
STATE_DIR="${HDT_BACKUP_STATE_DIR:-/var/lib/hdt-backup}"
METRICS_DIR="${HDT_BACKUP_METRICS_DIR:-}"
WAL_INTERVAL_S="${HDT_BACKUP_WAL_INTERVAL_S:-60}"
BACKUP_HOUR_UTC="${HDT_BACKUP_HOUR_UTC:-3}"
RETRY_BASE_S="${HDT_BACKUP_RETRY_BASE_S:-300}"
RETRY_MAX_S="${HDT_BACKUP_RETRY_MAX_S:-7200}"
WORK_DIR="${STATE_DIR}/work"
DAILY_KINDS=(base logical lake)
ID_PATTERN='^[0-9]{8}T[0-9]{6}Z$'
STEP_WORK=""
WAL_LOCK_WAIT_S=900
HASH_FD=""
HASH_PID=""

pg_env() {
  local password
  password="$(env_value HDT_PG_PASSWORD_BACKUP)" || exit 1
  if [[ -n "$password" ]]; then export PGPASSWORD="$password"; fi
  [[ -n "${PGHOST:-}" && -n "${PGUSER:-}" && -n "${PGDATABASE:-}" ]] || die "PGHOST, PGUSER and PGDATABASE must be set"
}

work_file() {  # work_file NAME -> a new file in the directory of the running step
  [[ -n "$STEP_WORK" && -d "$STEP_WORK" ]] || die "work_file outside a step"
  mktemp "${STEP_WORK}/$1.XXXXXX" || die "cannot create a work file"
}

is_daily_kind() {
  local kind
  for kind in "${DAILY_KINDS[@]}"; do [[ "$kind" != "$1" ]] || return 0; done
  return 1
}

# decimal_setting NAME VALUE MIN MAX -> VALUE as a base-10 number. Bash arithmetic reads a leading 0 as
# octal, so HDT_BACKUP_HOUR_UTC=08 would be an error in every comparison: normalise once, at start.
decimal_setting() {
  if [[ ! "$2" =~ ^[0-9]{1,9}$ ]] || ((10#$2 < $3 || 10#$2 > $4)); then
    die "$1 must be a whole number from $3 to $4 (got '$2')"
  fi
  printf '%d' "$((10#$2))"
}

check_settings() {
  BACKUP_HOUR_UTC="$(decimal_setting HDT_BACKUP_HOUR_UTC "$BACKUP_HOUR_UTC" 0 23)" || exit 1
  WAL_INTERVAL_S="$(decimal_setting HDT_BACKUP_WAL_INTERVAL_S "$WAL_INTERVAL_S" 1 3600)" || exit 1
  RETRY_BASE_S="$(decimal_setting HDT_BACKUP_RETRY_BASE_S "$RETRY_BASE_S" 1 86400)" || exit 1
  RETRY_MAX_S="$(decimal_setting HDT_BACKUP_RETRY_MAX_S "$RETRY_MAX_S" "$RETRY_BASE_S" 86400)" || exit 1
}

# Hash a stream while it is written elsewhere: `hash_start SUMS LABEL` opens HASH_FD, a pipe into
# sha256sum that writes "<sha256>  LABEL" to SUMS; the stream is copied into it with
# `tee "/dev/fd/${HASH_FD}"`; `hash_finish` closes the pipe and waits for the hasher. (A bare `wait` does
# not wait for a `>(...)` expanded inside a pipeline, so SUMS could still be empty when read.)
hash_start() {
  exec {HASH_FD}> >(sha256sum | awk -v label="$2" '{print $1 "  " label}' >"$1")
  HASH_PID=$!
}

hash_finish() {
  exec {HASH_FD}>&-
  wait "$HASH_PID"
}

# write_state NAME VALUE: replaces STATE_DIR/NAME durably (write, fsync, rename, fsync the directory).
write_state() {
  printf '%s\n' "$2" >"${STATE_DIR}/$1.tmp" \
    && sync "${STATE_DIR}/$1.tmp" \
    && mv -f "${STATE_DIR}/$1.tmp" "${STATE_DIR}/$1" \
    && sync "$STATE_DIR"
}

# record_shipped KIND ID: the complete set KIND/ID was uploaded from this host (restore-tested checks it).
record_shipped() {
  mkdir -p "$STATE_DIR" && printf '%s\t%s\n' "$1" "$2" >>"${STATE_DIR}/shipped.tsv" \
    || die "cannot record $1/$2 in ${STATE_DIR}/shipped.tsv"
}

# ------------------------------------------------------------------------------------ state and metrics

# with_state_lock COMMAND...: the WAL loop and the daily steps run in separate processes and share the
# state files; every read-modify-write of them holds this lock.
with_state_lock() {
  mkdir -p "$STATE_DIR" || return 1
  (
    flock 9 || exit 1
    "$@"
  ) 9>"${STATE_DIR}/state.lock"
}

write_metrics() {
  [[ -n "$METRICS_DIR" ]] || return 0
  local file="${STATE_DIR}/metrics.tsv" out pending=0
  mkdir -p "$METRICS_DIR" || return 1
  out="$(mktemp "${METRICS_DIR}/.hdt_backup.XXXXXX")" || return 1
  if [[ -d "$WAL_DIR" ]]; then pending="$(find "$WAL_DIR" -maxdepth 1 -type f ! -name '.*' ! -name '*.partial' | wc -l)"; fi
  {
    echo "# HELP hdt_backup_last_success_timestamp_seconds Unix time of the last successful backup step"
    echo "# TYPE hdt_backup_last_success_timestamp_seconds gauge"
    if [[ -f "$file" ]]; then
      awk -F'\t' '$2 == "success" {printf "hdt_backup_last_success_timestamp_seconds{kind=\"%s\"} %s\n", $1, $3}' "$file"
    fi
    echo "# HELP hdt_backup_last_failure_timestamp_seconds Unix time of the last failed backup step"
    echo "# TYPE hdt_backup_last_failure_timestamp_seconds gauge"
    if [[ -f "$file" ]]; then
      awk -F'\t' '$2 == "failure" {printf "hdt_backup_last_failure_timestamp_seconds{kind=\"%s\"} %s\n", $1, $3}' "$file"
    fi
    echo "# HELP hdt_backup_wal_pending_files WAL segments archived locally and not yet shipped"
    echo "# TYPE hdt_backup_wal_pending_files gauge"
    echo "hdt_backup_wal_pending_files ${pending}"
  } >"$out"
  chmod 0644 "$out"
  mv -f "$out" "${METRICS_DIR}/hdt_backup.prom"
}

record_outcome_locked() {  # record_outcome_locked KIND success|failure
  local file="${STATE_DIR}/metrics.tsv" tmp
  touch "$file" || return 1
  tmp="$(mktemp "${STATE_DIR}/metrics.XXXXXX")" || return 1
  awk -F'\t' -v k="$1" -v o="$2" '!($1 == k && $2 == o)' "$file" >"$tmp"
  printf '%s\t%s\t%s\n' "$1" "$2" "$(date -u +%s)" >>"$tmp"
  mv -f "$tmp" "$file"
  write_metrics || log "cannot write ${METRICS_DIR}/hdt_backup.prom"
}

record_outcome() { with_state_lock record_outcome_locked "$@"; }

# Daily bookkeeping: STATE_DIR/done.<kind> holds the UTC date of the last success; STATE_DIR/retry.<kind>
# holds "<date> <failed attempts> <unix time of the next attempt>" while that day's step keeps failing.
# DAY is the UTC date at which the step started: a step that ends after midnight still counts for the day
# it ran for, so the next day's run is not skipped.
mark_daily() {  # mark_daily KIND success|failure DAY
  local kind="$1" today="$3" now attempts=0 day="" count="" delay i
  now="$(date -u +%s)"
  if [[ "$2" == success ]]; then
    printf '%s\n' "$today" >"${STATE_DIR}/done.${kind}.tmp" || return 1
    mv -f "${STATE_DIR}/done.${kind}.tmp" "${STATE_DIR}/done.${kind}" || return 1
    rm -f "${STATE_DIR}/retry.${kind}"
    return
  fi
  if [[ -f "${STATE_DIR}/retry.${kind}" ]]; then read -r day count _ <"${STATE_DIR}/retry.${kind}" || true; fi
  if [[ "$day" == "$today" && "$count" =~ ^[0-9]+$ ]]; then attempts="$count"; fi
  attempts=$((attempts + 1))
  delay="$RETRY_BASE_S"
  for ((i = 1; i < attempts && delay < RETRY_MAX_S; i++)); do delay=$((delay * 2)); done
  ((delay <= RETRY_MAX_S)) || delay="$RETRY_MAX_S"
  printf '%s %s %s\n' "$today" "$attempts" "$((now + delay))" >"${STATE_DIR}/retry.${kind}" || return 1
  log "${kind}: attempt ${attempts} of ${today} failed; next attempt in ${delay}s"
}

daily_due() {  # daily_due KIND: not yet successful today and not inside a retry backoff
  local kind="$1" today now day="" count="" next=""
  today="$(date -u +%F)"
  now="$(date -u +%s)"
  [[ "$(cat "${STATE_DIR}/done.${kind}" 2>/dev/null || true)" != "$today" ]] || return 1
  if [[ -f "${STATE_DIR}/retry.${kind}" ]]; then read -r day count next <"${STATE_DIR}/retry.${kind}" || true; fi
  if [[ "$day" == "$today" && "$next" =~ ^[0-9]+$ ]] && ((now < next)); then return 1; fi
  return 0
}

# run_step KIND FUNCTION: runs the step in a subshell (a `die` ends only the step) inside its own work
# directory, removes that directory, records the outcome (and, for a daily kind, the day's bookkeeping).
run_step() {
  local kind="$1" status=0 day
  day="$(date -u +%F)"
  mkdir -p "$WORK_DIR" || die "cannot create ${WORK_DIR}"
  STEP_WORK="$(mktemp -d "${WORK_DIR}/${kind}.XXXXXX")" || die "cannot create a work directory"
  ("$2") || status=$?
  rm -rf "$STEP_WORK"
  STEP_WORK=""
  if [[ $status -eq 0 ]]; then
    record_outcome "$kind" success || true
  else
    record_outcome "$kind" failure || true
    log "${kind}: failed (exit ${status})"
  fi
  if is_daily_kind "$kind"; then
    with_state_lock mark_daily "$kind" "$([[ $status -eq 0 ]] && echo success || echo failure)" "$day" \
      || log "${kind}: cannot update the daily bookkeeping in ${STATE_DIR}"
  fi
  return "$status"
}

# ----------------------------------------------------------------------------------------------- steps

# WAL: segments are shipped oldest first, one at a time, by one run at a time (STATE_DIR/wal.lock: the
# daemon's loop and a manual `hdt-backup wal`). STATE_DIR/wal_in_flight names the segment whose upload
# starts, written before the upload; STATE_DIR/wal_last_shipped the segment uploaded last. A later run
# finds:
# - the segment of wal_last_shipped still present (stopped between upload and deletion): only deletes it;
# - the segment of wal_in_flight refused as existing (HTTP 412): an upload of ours stored it but its
#   answer was lost, or the step was killed during it; it counts as shipped;
# - any other existing object: not ours; the segment stays local and the step fails (runbook section 13).
do_wal() {
  [[ -d "$WAL_DIR" ]] || die "WAL archive directory ${WAL_DIR} does not exist"
  mkdir -p "$STATE_DIR" || die "cannot create ${STATE_DIR}"
  local lock segment name tmp shipped=0 list last in_flight status
  exec {lock}>"${STATE_DIR}/wal.lock" || die "cannot open ${STATE_DIR}/wal.lock"
  flock -w "$WAL_LOCK_WAIT_S" "$lock" || die "another WAL run has held ${STATE_DIR}/wal.lock for ${WAL_LOCK_WAIT_S}s"
  last="$(cat "${STATE_DIR}/wal_last_shipped" 2>/dev/null || true)"
  in_flight="$(cat "${STATE_DIR}/wal_in_flight" 2>/dev/null || true)"
  list="$(find "$WAL_DIR" -maxdepth 1 -type f ! -name '.*' ! -name '*.partial' | LC_ALL=C sort)" \
    || die "cannot list ${WAL_DIR}"
  while IFS= read -r segment; do
    [[ -n "$segment" ]] || continue
    name="$(basename "$segment")"
    if [[ "$name" != "$last" ]]; then
      tmp="$(work_file wal)" || exit 1
      zstd -q -c "$segment" | encrypt_to "$tmp" || die "cannot encrypt ${name}"
      # in_flight is the segment an earlier run started to upload (read before this upload is recorded).
      if [[ "$name" != "$in_flight" ]]; then
        write_state wal_in_flight "$name" || die "cannot record the segment in flight ${name}"
      fi
      status=0
      put_object "wal/${name}.zst.age" "$tmp" || status=$?
      if [[ $status -eq 3 && "$name" != "$in_flight" ]]; then
        die "wal/${name}.zst.age already exists but was not shipped from here; keeping ${segment} (runbook: backups)"
      elif [[ $status -eq 3 ]]; then
        log "wal/${name}.zst.age: stored by an interrupted earlier upload from here; counted as shipped"
      fi
      rm -f "$tmp"
      write_state wal_last_shipped "$name" || die "cannot record the shipped segment ${name}"
      last="$name"
    fi
    rm -f "$segment" || die "cannot remove shipped segment ${name}"
    shipped=$((shipped + 1))
  done <<<"$list"
  if [[ $shipped -gt 0 ]]; then log "wal: shipped ${shipped} segment(s)"; fi
}

# Parameters a hot-standby recovery checks against the primary (a restored server refuses to replay WAL
# when one is lower); the server sets them on its command line, so they are not in the base backup.
RECOVERY_PARAMETERS="'max_connections', 'max_worker_processes', 'max_wal_senders', 'max_prepared_transactions', 'max_locks_per_transaction'"

do_base() {
  pg_env
  local id tmp sums settings
  id="$(stamp)"
  tmp="$(work_file base)" || exit 1
  sums="$(work_file sums)" || exit 1
  settings="$(work_file settings)" || exit 1
  psql --no-password -X -A -t -v ON_ERROR_STOP=1 \
    -c "SELECT name || ' = ''' || setting || '''' FROM pg_settings WHERE name IN (${RECOVERY_PARAMETERS}) ORDER BY name" \
    >"$settings" || die "cannot read the server parameters"
  [[ "$(wc -l <"$settings")" -eq 5 ]] || die "unexpected server parameter list"
  # -X none: the WAL that makes the backup consistent is archived by Postgres and shipped by `wal`.
  hash_start "$sums" base.tar
  pg_basebackup -D - -F tar -X none --checkpoint=fast --label "hdt-${id}" --no-password \
    | tee "/dev/fd/${HASH_FD}" \
    | zstd -q -T0 -c | encrypt_to "$tmp" || die "pg_basebackup failed"
  hash_finish || die "cannot hash base.tar"
  grep -q ' base.tar$' "$sums" || die "missing checksum of base.tar"
  printf '%s  settings.conf\n' "$(sha256_file "$settings")" >>"$sums" || die "cannot hash settings.conf"
  # SHA256SUMS goes last: hdt-restore treats an id without it as incomplete.
  put_new "base/${id}/base.tar.zst.age" "$tmp"
  put_new "base/${id}/settings.conf" "$settings"
  put_new "base/${id}/SHA256SUMS" "$sums"
  record_shipped base "$id"
  log "base: uploaded base/${id}"
}

do_logical() {
  pg_env
  local id tmp sums name="${PGDATABASE}.dump"
  id="$(stamp)"
  tmp="$(work_file dump)" || exit 1
  sums="$(work_file sums)" || exit 1
  hash_start "$sums" "$name"
  pg_dump --format=custom --no-password --dbname "$PGDATABASE" \
    | tee "/dev/fd/${HASH_FD}" \
    | encrypt_to "$tmp" || die "pg_dump failed"
  hash_finish || die "cannot hash ${name}"
  grep -q " ${name}\$" "$sums" || die "missing checksum of ${name}"
  put_new "logical/${id}/${name}.age" "$tmp"
  put_new "logical/${id}/SHA256SUMS" "$sums"
  record_shipped logical "$id"
  log "logical: uploaded logical/${id}"
}

# Lake: closed hourly Parquet files are immutable, so every file is uploaded once, keyed by its sha256.
# `lake_index.tsv` remembers path, size, mtime and sha256 so unchanged files are not read again; a new
# file is read once, hashed while it is encrypted. Writers' temporary files (*.tmp, dotfiles) are skipped,
# and so is a file that disappears or changes while it is read (the next run takes it).
do_lake() {
  [[ -d "$LAKE_DIR" ]] || die "lake directory ${LAKE_DIR} does not exist"
  mkdir -p "$STATE_DIR" || die "cannot create ${STATE_DIR}"
  local index="${STATE_DIR}/lake_index.tsv" uploaded="${STATE_DIR}/lake_uploaded.txt"
  touch "$index" "$uploaded" || die "cannot write ${STATE_DIR}"
  declare -A known_sha=() sent=()
  local path size mtime sha
  while IFS=$'\t' read -r path size mtime sha; do
    if [[ -n "$path" ]]; then known_sha["${path}|${size}|${mtime}"]="$sha"; fi
  done <"$index"
  while IFS= read -r sha; do
    if [[ -n "$sha" ]]; then sent["$sha"]=1; fi
  done <"$uploaded"

  local id manifest new_index tmp sums listing after status count=0 files=0 skipped=0
  id="$(stamp)"
  manifest="$(work_file manifest)" || exit 1
  new_index="$(work_file index)" || exit 1
  listing="$(cd "$LAKE_DIR" && find . -type f ! -name '.*' ! -name '*.tmp' -printf '%P\t%s\t%T@\n' | LC_ALL=C sort)" \
    || die "cannot list ${LAKE_DIR}"
  while IFS=$'\t' read -r path size mtime; do
    [[ -n "$path" ]] || continue
    sha="${known_sha["${path}|${size}|${mtime}"]:-}"
    tmp=""
    if [[ -z "$sha" || -z "${sent[$sha]:-}" ]]; then
      tmp="$(work_file lakeobj)" || exit 1
      sums="$(work_file lakesum)" || exit 1
      status=0
      hash_start "$sums" object
      tee "/dev/fd/${HASH_FD}" <"${LAKE_DIR}/${path}" | encrypt_to "$tmp" || status=$?
      hash_finish || die "cannot hash ${path}"
      after="$(find "${LAKE_DIR}/${path}" -maxdepth 0 -printf '%s\t%T@' 2>/dev/null || true)"
      if [[ "$after" != "${size}"$'\t'"${mtime}" ]]; then
        log "lake: ${path} changed or disappeared while it was read; skipped until the next run"
        rm -f "$tmp" "$sums"
        skipped=$((skipped + 1))
        continue
      fi
      [[ $status -eq 0 ]] || die "cannot encrypt ${path}"
      sha="$(cut -d' ' -f1 <"$sums")"
      [[ "$sha" =~ ^[0-9a-f]{64}$ ]] || die "cannot hash ${path}"
      rm -f "$sums"
    fi
    if [[ -z "${sent[$sha]:-}" ]]; then
      status=0
      put_object "lake/objects/${sha}.age" "$tmp" || status=$?
      # 3: the object exists (an upload whose bookkeeping a restart interrupted); content-addressed,
      # and hdt-restore checks every restored file against its sha256.
      printf '%s\n' "$sha" >>"$uploaded"
      sent["$sha"]=1
      if [[ $status -eq 0 ]]; then count=$((count + 1)); fi
    fi
    if [[ -n "$tmp" ]]; then rm -f "$tmp"; fi
    printf '%s\t%s\t%s\n' "$path" "$sha" "$size" >>"$manifest"
    printf '%s\t%s\t%s\t%s\n' "$path" "$size" "$mtime" "$sha" >>"$new_index"
    files=$((files + 1))
  done <<<"$listing"
  mv -f "$new_index" "$index" || die "cannot update ${index}"
  tmp="$(work_file lakemanifest)" || exit 1
  encrypt_to "$tmp" <"$manifest" || die "cannot encrypt the lake manifest"
  put_new "lake/manifests/${id}.tsv.age" "$tmp"
  record_shipped lake "$id"
  log "lake: ${files} file(s) in lake/manifests/${id}.tsv.age, ${count} new object(s) uploaded, ${skipped} skipped"
}

do_daily() {
  local status=0
  run_step base do_base || status=1
  run_step logical do_logical || status=1
  run_step lake do_lake || status=1
  return "$status"
}

# restore_tested BASE_ID LOGICAL_ID LAKE_ID: only sets uploaded from this host (STATE_DIR/shipped.tsv)
# count, so the overdue-drill alert cannot be cleared with made-up ids.
restore_tested() {
  [[ $# -eq 3 ]] || die "usage: hdt-backup restore-tested BASE_ID LOGICAL_ID LAKE_ID"
  local shipped="${STATE_DIR}/shipped.tsv" kind id i
  local -a kinds=(base logical lake) ids=("$@")
  for i in 0 1 2; do
    kind="${kinds[i]}"
    id="${ids[i]}"
    [[ "$id" =~ $ID_PATTERN ]] || die "invalid backup id: ${id}"
    grep -qxF "${kind}"$'\t'"${id}" "$shipped" 2>/dev/null \
      || die "${kind}/${id} was not uploaded from this host (${shipped}); give the ids the drill restored"
  done
  printf '%s\tbase=%s\tlogical=%s\tlake=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$@" >>"${STATE_DIR}/restore_tests.tsv" \
    || die "cannot write ${STATE_DIR}/restore_tests.tsv"
  record_outcome restore_test success || die "cannot record the restore test"
  log "restore test recorded: base=$1 logical=$2 lake=$3"
}

# ---------------------------------------------------------------------------------------------- daemon

WAL_PID=""

wal_loop() {
  trap 'exit 0' TERM INT
  while :; do
    run_step wal do_wal || true
    sleep "$WAL_INTERVAL_S" &
    wait $! || true
  done
}

start_wal_loop() {
  wal_loop &
  WAL_PID=$!
}

stop_daemon() {
  log "stopping"
  if [[ -n "$WAL_PID" ]]; then
    kill "$WAL_PID" 2>/dev/null || true
    wait "$WAL_PID" 2>/dev/null || true
  fi
  exit 0
}

daemon() {
  mkdir -p "$STATE_DIR" || die "cannot create ${STATE_DIR}"
  # Work directories of steps interrupted by a restart are incomplete; nothing references them.
  rm -rf "$WORK_DIR"
  trap stop_daemon TERM INT
  log "daemon started: wal every ${WAL_INTERVAL_S}s, daily steps at or after ${BACKUP_HOUR_UTC}:00 UTC"
  start_wal_loop
  local kind hour
  while :; do
    if ! kill -0 "$WAL_PID" 2>/dev/null; then
      log "wal loop exited; restarting it"
      start_wal_loop
    fi
    hour="$(date -u +%H)"
    if ((10#$hour >= 10#$BACKUP_HOUR_UTC)); then
      for kind in "${DAILY_KINDS[@]}"; do
        if daily_due "$kind"; then run_step "$kind" "do_${kind}" || true; fi
      done
    fi
    with_state_lock write_metrics || log "cannot write ${METRICS_DIR}/hdt_backup.prom"
    sleep "$WAL_INTERVAL_S" &
    wait $! || true
  done
}

main() {
  [[ $# -ge 1 ]] || die "usage: hdt-backup wal|base|logical|lake|daily|restore-tested|daemon"
  target_kind >/dev/null
  check_settings
  local command="$1"
  shift
  case "$command" in
    wal) run_step wal do_wal ;;
    base) run_step base do_base ;;
    logical) run_step logical do_logical ;;
    lake) run_step lake do_lake ;;
    daily) do_daily ;;
    restore-tested) restore_tested "$@" ;;
    daemon) daemon ;;
    *) die "unknown command: ${command}" ;;
  esac
}

main "$@"
