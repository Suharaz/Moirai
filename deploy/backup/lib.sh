#!/usr/bin/env bash
# Shared helpers of hdt-backup (backup.sh) and hdt-restore (restore.sh). Sourced, never executed.
#
# Target (HDT_BACKUP_TARGET):
#   file:///<dir>              local directory (restore drills, tests, a mounted backup disk)
#   s3://<bucket>[/<prefix>]   S3-compatible object storage, path-style, AWS Signature V4 through curl;
#                              endpoint in HDT_BACKUP_S3_ENDPOINT (https://host[:port]), region in
#                              HDT_BACKUP_S3_REGION (default us-east-1).
# Credentials are read from NAME or from the file named by NAME_FILE (docker secrets). The backup side
# holds write-only credentials (PutObject); the restore side holds read credentials and the age identity.
#
# Error handling: steps run in subshells whose failures are checked explicitly (`|| die`), because bash
# ignores `set -e` inside functions called from an `||` list. `die` exits the current (sub)shell.

set -euo pipefail
umask 077

log() { printf '%s %s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${HDT_LOG_NAME:-hdt-backup}" "$*" >&2; }
die() {
  log "error: $*"
  exit 1
}

# env_value NAME -> value of NAME, or content of the file named by NAME_FILE; empty when neither is set.
env_value() {
  local name="$1" file_var="${1}_FILE" value
  if [[ -n "${!file_var:-}" ]]; then
    [[ -r "${!file_var}" ]] || die "${file_var} points to an unreadable file"
    value="$(<"${!file_var}")" || die "cannot read ${file_var}"
    printf '%s' "${value%$'\n'}"
  else
    printf '%s' "${!name:-}"
  fi
}

require_value() {
  local value
  value="$(env_value "$1")" || exit 1
  [[ -n "$value" ]] || die "$1 (or $1_FILE) is not set"
  printf '%s' "$value"
}

stamp() { date -u +%Y%m%dT%H%M%SZ; }

sha256_file() {
  local out
  out="$(sha256sum "$1")" || die "cannot hash $1"
  printf '%s' "${out%% *}"
}

# ---------------------------------------------------------------------------------------------- target

TARGET="${HDT_BACKUP_TARGET:-}"
S3_REGION="${HDT_BACKUP_S3_REGION:-us-east-1}"

target_kind() {
  case "$TARGET" in
    file:///*) echo file ;;
    s3://?*) echo s3 ;;
    *) die "HDT_BACKUP_TARGET must be file:///<dir> or s3://<bucket>[/<prefix>]" ;;
  esac
}

file_root() { printf '%s' "${TARGET#file://}"; }

# s3_bucket_url -> http(s)://endpoint/bucket
s3_bucket_url() {
  local endpoint="${HDT_BACKUP_S3_ENDPOINT:-}" rest
  [[ "$endpoint" =~ ^https?://[^/]+$ ]] || die "HDT_BACKUP_S3_ENDPOINT must be http(s)://host[:port] without a path"
  rest="${TARGET#s3://}"
  printf '%s/%s' "$endpoint" "${rest%%/*}"
}

# s3_prefix -> "prefix/" or ""
s3_prefix() {
  local rest="${TARGET#s3://}" prefix
  if [[ "$rest" == */* && -n "${rest#*/}" ]]; then
    prefix="${rest#*/}"
    printf '%s/' "${prefix%/}"
  fi
}

# put_object KEY FILE: creates KEY, never replaces it (write-only credentials HDT_BACKUP_S3_ACCESS_KEY /
# HDT_BACKUP_S3_SECRET_KEY). S3 uploads carry `If-None-Match: *` (the bucket policy should require it,
# docs/runbook.md section 13), so an existing backup can be neither overwritten by this key nor by a
# retry. Returns 0 when created and 3 when KEY already exists (HTTP 412, or the file is present); any
# other failure ends the (sub)shell through `die`.
put_object() {
  local key="$1" file="$2" kind
  kind="$(target_kind)" || exit 1
  case "$kind" in
    file)
      local dest
      dest="$(file_root)/${key}"
      mkdir -p "$(dirname "$dest")" || die "cannot create $(dirname "$dest")"
      [[ ! -e "$dest" ]] || return 3
      cp "$file" "${dest}.partial" || die "cannot write ${key}"
      # link(2) never replaces an existing name, so a concurrent writer of the same key cannot win twice.
      if ! ln "${dest}.partial" "$dest" 2>/dev/null; then
        rm -f "${dest}.partial"
        [[ ! -e "$dest" ]] || return 3
        die "cannot write ${key}"
      fi
      rm -f "${dest}.partial"
      ;;
    s3)
      local access secret sha url code attempt uncertain=0
      access="$(require_value HDT_BACKUP_S3_ACCESS_KEY)" || exit 1
      secret="$(require_value HDT_BACKUP_S3_SECRET_KEY)" || exit 1
      sha="$(sha256_file "$file")" || exit 1
      url="$(s3_bucket_url)" || exit 1
      for attempt in 1 2 3 4; do
        code="$(curl --silent --show-error --connect-timeout 20 --max-time 3600 \
          --aws-sigv4 "aws:amz:${S3_REGION}:s3" --user "${access}:${secret}" -o /dev/null -w '%{http_code}' \
          -H "If-None-Match: *" -H "x-amz-content-sha256: ${sha}" -H "Content-Type: application/octet-stream" \
          --upload-file "$file" "${url}/$(s3_prefix)${key}")" || code=000
        case "$code" in
          200) return 0 ;;
          # An earlier attempt of this call whose answer was lost may have created the object.
          412) if [[ $uncertain -eq 1 ]]; then return 0; else return 3; fi ;;
          000 | 408 | 429 | 5??) uncertain=1 ;;
          409) ;;  # ConditionalRequestConflict: a concurrent conditional write; retry
          *) die "PUT ${key}: HTTP ${code}" ;;
        esac
        [[ $attempt -lt 4 ]] || break
        log "PUT ${key}: HTTP ${code}, retrying"
        sleep $((attempt * 5))
      done
      die "PUT ${key} failed (last HTTP ${code})"
      ;;
  esac
}

# put_new KEY FILE: put_object for keys that must not exist yet (fresh backup ids).
put_new() {
  local status=0
  put_object "$1" "$2" || status=$?
  [[ $status -ne 3 ]] || die "$1 already exists in the backup target; refusing to overwrite it"
  return "$status"
}

# get_object KEY OUT (read credentials HDT_RESTORE_S3_ACCESS_KEY / HDT_RESTORE_S3_SECRET_KEY);
# returns 2 when the object does not exist.
get_object() {
  local key="$1" out="$2" kind
  kind="$(target_kind)" || exit 1
  case "$kind" in
    file)
      local src
      src="$(file_root)/${key}"
      [[ -f "$src" ]] || return 2
      cp "$src" "$out" || die "cannot read ${key}"
      ;;
    s3)
      local access secret url code
      access="$(require_value HDT_RESTORE_S3_ACCESS_KEY)" || exit 1
      secret="$(require_value HDT_RESTORE_S3_SECRET_KEY)" || exit 1
      url="$(s3_bucket_url)" || exit 1
      code="$(curl --silent --show-error --retry 3 --connect-timeout 20 \
        --aws-sigv4 "aws:amz:${S3_REGION}:s3" --user "${access}:${secret}" \
        -o "$out" -w '%{http_code}' "${url}/$(s3_prefix)${key}")" || die "GET ${key} failed"
      case "$code" in
        200) ;;
        404)
          rm -f "$out"
          return 2
          ;;
        *)
          rm -f "$out"
          die "GET ${key}: HTTP ${code}"
          ;;
      esac
      ;;
  esac
}

# list_keys PREFIX -> every key under PREFIX (relative to the target root), sorted.
list_keys() {
  local prefix="$1" kind
  kind="$(target_kind)" || exit 1
  case "$kind" in
    file)
      local root
      root="$(file_root)"
      [[ -d "${root}/${prefix}" ]] || return 0
      (cd "$root" && find "$prefix" -type f ! -name '*.partial') | LC_ALL=C sort
      ;;
    s3)
      local access secret url base token="" page keys
      access="$(require_value HDT_RESTORE_S3_ACCESS_KEY)" || exit 1
      secret="$(require_value HDT_RESTORE_S3_SECRET_KEY)" || exit 1
      url="$(s3_bucket_url)" || exit 1
      base="$(s3_prefix)"
      {
        while :; do
          local args=(--get --data-urlencode "list-type=2" --data-urlencode "prefix=${base}${prefix}")
          if [[ -n "$token" ]]; then args+=(--data-urlencode "continuation-token=${token}"); fi
          page="$(curl --fail --silent --show-error --retry 3 --connect-timeout 20 \
            --aws-sigv4 "aws:amz:${S3_REGION}:s3" --user "${access}:${secret}" "${args[@]}" "$url")" \
            || die "LIST ${prefix} failed"
          keys="$(grep -o '<Key>[^<]*</Key>' <<<"$page" | sed -e 's/^<Key>//' -e 's/<\/Key>$//')" || keys=""
          # shellcheck disable=SC2001 # strips the prefix of every line; ${var//} cannot anchor per line
          if [[ -n "$keys" ]]; then sed -e "s|^${base}||" <<<"$keys"; fi
          grep -q '<IsTruncated>true</IsTruncated>' <<<"$page" || break
          token="$(grep -o '<NextContinuationToken>[^<]*</NextContinuationToken>' <<<"$page" \
            | sed -e 's/^<NextContinuationToken>//' -e 's/<\/NextContinuationToken>$//')" || token=""
          [[ -n "$token" ]] || break
        done
      } | LC_ALL=C sort
      ;;
  esac
}

# ---------------------------------------------------------------------------------------------- crypto

# encrypt_to OUT < plaintext ; age to every recipient in HDT_BACKUP_AGE_RECIPIENTS (space separated).
encrypt_to() {
  local out="$1" r
  local recipients=()
  for r in ${HDT_BACKUP_AGE_RECIPIENTS:-}; do
    [[ "$r" == age1* ]] || die "HDT_BACKUP_AGE_RECIPIENTS must list age public keys (age1...)"
    recipients+=(-r "$r")
  done
  [[ ${#recipients[@]} -gt 0 ]] || die "HDT_BACKUP_AGE_RECIPIENTS is empty"
  age "${recipients[@]}" -o "$out"
}

# decrypt_to OUT < ciphertext ; identity file in HDT_RESTORE_AGE_IDENTITY_FILE.
decrypt_to() {
  local out="$1" identity="${HDT_RESTORE_AGE_IDENTITY_FILE:-}"
  [[ -r "$identity" ]] || die "HDT_RESTORE_AGE_IDENTITY_FILE must point to the age identity file"
  age -d -i "$identity" -o "$out"
}
