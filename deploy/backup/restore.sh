#!/usr/bin/env bash
# hdt-restore: restore side of hdt-backup (phase 10). Runs with READ credentials
# (HDT_RESTORE_S3_ACCESS_KEY / HDT_RESTORE_S3_SECRET_KEY, or a file:/// target) and the offline age
# identity (HDT_RESTORE_AGE_IDENTITY_FILE); neither is ever deployed next to the backup writer. The read
# key needs s3:ListBucket as well as s3:GetObject: without it a missing object answers 403, not 404.
#
#   hdt-restore list [PREFIX]                      keys under PREFIX (base/, logical/, wal/, lake/)
#   hdt-restore latest base|logical|lake           newest complete backup id (lake: newest manifest id)
#   hdt-restore logical ID|latest DSN [ARGS...]    pg_restore the dump into DSN (extra pg_restore ARGS)
#   hdt-restore base ID|latest PGDATA [--target-time TS]
#                                                  unpack the base backup into an empty PGDATA, verify it
#                                                  against its backup manifest and configure archive
#                                                  recovery (restore_command = hdt-restore wal %f %p, up to
#                                                  TS or to the end of the shipped WAL), then start Postgres
#                                                  on PGDATA with the same environment
#   hdt-restore wal NAME DEST                      restore_command: one WAL segment or history file
#   hdt-restore lake ID|latest DIR                 rebuild the lake file tree listed by the manifest
#
# `latest` skips ids that are malformed or lie in the future (more than HDT_RESTORE_MAX_CLOCK_SKEW_S,
# default 300, ahead of this clock: an object planted with the write-only key cannot pose as the newest
# backup) and, for base and logical, sets that are incomplete: SHA256SUMS (uploaded last) must exist,
# list exactly the files of that kind, and every listed file must be present. An explicit ID must be
# complete too. Every downloaded artifact is decrypted and checked against the plaintext sha256 recorded
# at backup time before it is used.
#
# `wal` exit status, as PostgreSQL reads it: 0 restored; 1 the segment was never shipped (the normal end
# of archive recovery); 255 anything else (network, HTTP, decryption, disk), which makes recovery stop
# with an error instead of promoting a server that misses later transactions. Restart Postgres to retry.

HDT_LOG_NAME=hdt-restore
LIB_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
# shellcheck source=deploy/backup/lib.sh
source "${LIB_DIR}/lib.sh"

ID_PATTERN='^[0-9]{8}T[0-9]{6}Z$'
MAX_CLOCK_SKEW_S="${HDT_RESTORE_MAX_CLOCK_SKEW_S:-300}"

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/hdt-restore.XXXXXX")" || { log "cannot create a work directory"; exit 255; }
trap 'rm -rf "$WORK_DIR"' EXIT

# newest_allowed_id -> the id of "now + clock skew"; ids above it are in the future
newest_allowed_id() {
  date -u -d "@$(($(date -u +%s) + MAX_CLOCK_SKEW_S))" +%Y%m%dT%H%M%SZ
}

# candidate_ids KEYS_FILE KIND -> well-formed, not-future ids under KIND/, newest first
candidate_ids() {
  local keys="$1" kind="$2" limit id
  limit="$(newest_allowed_id)" || die "cannot read the clock"
  while IFS= read -r id; do
    [[ -n "$id" ]] || continue
    if ! [[ "$id" =~ $ID_PATTERN ]]; then
      log "${kind}: ignoring malformed id ${id}"
    elif [[ "$id" > "$limit" ]]; then
      log "${kind}: ignoring future id ${id}"
    else
      printf '%s\n' "$id"
    fi
  done < <(case "$kind" in
    lake) sed -n 's|^lake/manifests/\([^/]*\)\.tsv\.age$|\1|p' "$keys" ;;
    *) awk -F/ -v kind="$kind" '$1 == kind && NF >= 3 {print $2}' "$keys" ;;
  esac | LC_ALL=C sort -u -r)
}

# object_of KIND NAME -> key suffix of the uploaded object for a SHA256SUMS entry
object_of() {
  case "$1:$2" in
    base:base.tar) printf 'base.tar.zst.age' ;;
    base:settings.conf) printf 'settings.conf' ;;
    logical:*.dump) printf '%s.age' "$2" ;;
    *) return 1 ;;
  esac
}

sums_file() { printf '%s/%s-%s.SHA256SUMS' "$WORK_DIR" "$1" "$2"; }

# complete_set KIND ID KEYS_FILE: SHA256SUMS of the set is downloaded to `sums_file KIND ID`, is
# well-formed, lists exactly the files of KIND, and every listed file exists in KEYS_FILE.
complete_set() {
  local kind="$1" id="$2" keys="$3" sums names name object status=0
  sums="$(sums_file "$kind" "$id")"
  grep -qxF "${kind}/${id}/SHA256SUMS" "$keys" || { log "${kind}/${id}: no SHA256SUMS (incomplete)"; return 1; }
  get_object "${kind}/${id}/SHA256SUMS" "$sums" || status=$?
  [[ $status -eq 0 ]] || { log "${kind}/${id}/SHA256SUMS: not readable"; return 1; }
  if grep -Evq '^[0-9a-f]{64}  [A-Za-z0-9._-]+$' "$sums"; then
    log "${kind}/${id}: malformed SHA256SUMS"
    return 1
  fi
  names="$(awk '{print $2}' "$sums" | LC_ALL=C sort)"
  case "$kind" in
    base) [[ "$names" == $'base.tar\nsettings.conf' ]] ;;
    logical) [[ "$names" =~ ^[A-Za-z0-9._-]+\.dump$ ]] ;;
  esac || { log "${kind}/${id}: SHA256SUMS lists unexpected files"; return 1; }
  while IFS= read -r name; do
    object="$(object_of "$kind" "$name")" || return 1
    grep -qxF "${kind}/${id}/${object}" "$keys" || { log "${kind}/${id}: ${object} is missing (incomplete)"; return 1; }
  done <<<"$names"
}

resolve_id() {  # resolve_id KIND ID|latest -> a complete backup id
  local kind="$1" id="$2" keys="${WORK_DIR}/keys.$1" prefix
  case "$kind" in
    lake) prefix=lake/manifests/ ;;
    base | logical) prefix="${kind}/" ;;
    *) die "unknown backup kind: ${kind}" ;;
  esac
  list_keys "$prefix" >"$keys" || die "cannot list ${prefix}"
  if [[ "$id" == latest ]]; then
    local candidate
    id=""
    while IFS= read -r candidate; do
      if [[ "$kind" == lake ]] || complete_set "$kind" "$candidate" "$keys"; then
        id="$candidate"
        break
      fi
    done < <(candidate_ids "$keys" "$kind")
    [[ -n "$id" ]] || die "no complete ${kind} backup found"
  else
    [[ "$id" =~ $ID_PATTERN ]] || die "invalid backup id: ${id}"
    if [[ "$kind" == lake ]]; then
      grep -qxF "lake/manifests/${id}.tsv.age" "$keys" || die "lake manifest ${id} not found"
    else
      complete_set "$kind" "$id" "$keys" || die "${kind}/${id} is not a complete backup"
    fi
  fi
  printf '%s' "$id"
}

# fetch_decrypt KEY OUT : download + decrypt one object
fetch_decrypt() {
  local key="$1" out="$2" enc status=0
  enc="${WORK_DIR}/$(basename "$key")"
  get_object "$key" "$enc" || status=$?
  [[ $status -eq 0 ]] || die "object ${key} not found"
  decrypt_to "$out" <"$enc" || die "cannot decrypt ${key}"
  rm -f "$enc"
}

verify_sum() {  # verify_sum SUMS_FILE NAME FILE
  local expected actual
  expected="$(awk -v n="$2" '$2 == n {print $1}' "$1")"
  [[ -n "$expected" ]] || die "no checksum recorded for $2"
  actual="$(sha256_file "$3")" || exit 1
  [[ "$expected" == "$actual" ]] || die "checksum mismatch for $2"
}

cmd_list() {
  list_keys "${1:-}"
}

cmd_latest() {
  [[ $# -eq 1 ]] || die "usage: hdt-restore latest base|logical|lake"
  resolve_id "$1" latest
  echo
}

cmd_logical() {
  [[ $# -ge 2 ]] || die "usage: hdt-restore logical ID|latest DSN [pg_restore args...]"
  local id dsn sums name dump
  id="$(resolve_id logical "$1")" || exit 1
  dsn="$2"
  shift 2
  sums="$(sums_file logical "$id")"
  name="$(awk '{print $2}' "$sums")"
  dump="${WORK_DIR}/${name}"
  fetch_decrypt "logical/${id}/${name}.age" "$dump"
  verify_sum "$sums" "$name" "$dump"
  log "logical: restoring logical/${id} (${name})"
  pg_restore --exit-on-error --dbname "$dsn" "$@" "$dump" || die "pg_restore failed"
  log "logical: restored logical/${id}"
}

cmd_base() {
  [[ $# -ge 2 ]] || die "usage: hdt-restore base ID|latest PGDATA [--target-time TS]"
  local id pgdata target_time="" sums tar
  id="$(resolve_id base "$1")" || exit 1
  pgdata="$2"
  shift 2
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --target-time)
        [[ $# -ge 2 ]] || die "--target-time needs a timestamp"
        target_time="$2"
        shift 2
        ;;
      *) die "unknown option: $1" ;;
    esac
  done
  if [[ -d "$pgdata" ]] && [[ -n "$(ls -A "$pgdata")" ]]; then die "${pgdata} is not empty"; fi
  mkdir -p "$pgdata" || die "cannot create ${pgdata}"
  chmod 0700 "$pgdata" || die "cannot set the mode of ${pgdata}"
  sums="$(sums_file base "$id")"
  tar="${WORK_DIR}/base.tar"
  get_object "base/${id}/base.tar.zst.age" "${WORK_DIR}/base.tar.zst.age" || die "base/${id} not found"
  decrypt_to /dev/stdout <"${WORK_DIR}/base.tar.zst.age" | zstd -q -d -c >"$tar" || die "cannot decrypt base/${id}"
  rm -f "${WORK_DIR}/base.tar.zst.age"
  verify_sum "$sums" base.tar "$tar"
  tar -xf "$tar" -C "$pgdata" || die "cannot unpack base/${id}"
  rm -f "$tar"
  if [[ -f "${pgdata}/backup_manifest" ]]; then
    pg_verifybackup --no-parse-wal "$pgdata" >/dev/null || die "pg_verifybackup rejected base/${id}"
  fi
  get_object "base/${id}/settings.conf" "${WORK_DIR}/settings.conf" || die "base/${id}/settings.conf not found"
  verify_sum "$sums" settings.conf "${WORK_DIR}/settings.conf"
  if grep -Evq "^(max_connections|max_worker_processes|max_wal_senders|max_prepared_transactions|max_locks_per_transaction) = '[0-9]+'$" \
    "${WORK_DIR}/settings.conf"; then
    die "unexpected content in base/${id}/settings.conf"
  fi
  {
    echo "# hdt-restore ${id}: parameters of the primary (recovery refuses lower values)"
    cat "${WORK_DIR}/settings.conf"
    echo "# hdt-restore ${id}: archive recovery from the shipped WAL"
    echo "restore_command = 'hdt-restore wal %f %p'"
    if [[ -n "$target_time" ]]; then
      echo "recovery_target_time = '${target_time}'"
    fi
    echo "recovery_target_action = 'promote'"
    # The restored server never archives into the production archive.
    echo "archive_mode = 'off'"
  } >>"${pgdata}/postgresql.auto.conf"
  touch "${pgdata}/recovery.signal"
  log "base: base/${id} unpacked into ${pgdata}; start Postgres on it to replay WAL${target_time:+ up to ${target_time}}"
}

# fetch_wal NAME DEST: runs in a subshell; exits 2 when the segment was never shipped, 1 on any error.
fetch_wal() {
  local name="$1" dest="$2" enc status=0
  [[ "$name" =~ ^[0-9A-F]{8,}(\.history|\.partial|\.[0-9A-F]{8}\.backup)?$ ]] || die "invalid WAL name: ${name}"
  enc="${WORK_DIR}/${name}.zst.age"
  get_object "wal/${name}.zst.age" "$enc" || status=$?
  [[ $status -ne 2 ]] || exit 2
  [[ $status -eq 0 ]] || die "cannot download wal/${name}"
  decrypt_to /dev/stdout <"$enc" | zstd -q -d -c >"${dest}.hdt-partial" || die "cannot decrypt wal/${name}"
  mv -f "${dest}.hdt-partial" "$dest" || die "cannot write ${dest}"
}

cmd_wal() {
  [[ $# -eq 2 ]] || { log "usage: hdt-restore wal NAME DEST"; exit 255; }
  local status=0
  (fetch_wal "$1" "$2") || status=$?
  case "$status" in
    0) ;;
    2) exit 1 ;;
    *)
      rm -f "${2}.hdt-partial"
      log "wal $1: not restored (exit ${status}); stopping recovery instead of ending it early"
      exit 255
      ;;
  esac
}

cmd_lake() {
  [[ $# -eq 2 ]] || die "usage: hdt-restore lake ID|latest DIR"
  local id dest manifest path sha size out files=0
  id="$(resolve_id lake "$1")" || exit 1
  dest="$2"
  manifest="${WORK_DIR}/manifest.tsv"
  fetch_decrypt "lake/manifests/${id}.tsv.age" "$manifest"
  mkdir -p "$dest" || die "cannot create ${dest}"
  while IFS=$'\t' read -r path sha size; do
    [[ -n "$path" ]] || continue
    [[ "$path" != /* && "$path" != *..* ]] || die "unsafe path in manifest: ${path}"
    out="${dest}/${path}"
    if [[ -f "$out" ]] && [[ "$(sha256_file "$out")" == "$sha" ]]; then
      files=$((files + 1))
      continue
    fi
    mkdir -p "$(dirname "$out")" || die "cannot create $(dirname "$out")"
    fetch_decrypt "lake/objects/${sha}.age" "${out}.hdt-partial"
    [[ "$(sha256_file "${out}.hdt-partial")" == "$sha" ]] || die "checksum mismatch for ${path}"
    [[ "$(stat -c %s "${out}.hdt-partial")" == "$size" ]] || die "size mismatch for ${path}"
    mv -f "${out}.hdt-partial" "$out" || die "cannot write ${out}"
    files=$((files + 1))
  done <"$manifest"
  log "lake: ${files} file(s) restored from lake/manifests/${id}.tsv.age into ${dest}"
}

main() {
  [[ $# -ge 1 ]] || die "usage: hdt-restore list|latest|logical|base|wal|lake ..."
  local command="$1"
  shift
  if [[ "$command" == wal ]]; then
    # restore_command: a configuration error must stop recovery (255), never read as end of WAL (1).
    (target_kind >/dev/null) || exit 255
    cmd_wal "$@"
    return
  fi
  target_kind >/dev/null
  case "$command" in
    list) cmd_list "$@" ;;
    latest) cmd_latest "$@" ;;
    logical) cmd_logical "$@" ;;
    base) cmd_base "$@" ;;
    lake) cmd_lake "$@" ;;
    *) die "unknown command: ${command}" ;;
  esac
}

main "$@"
