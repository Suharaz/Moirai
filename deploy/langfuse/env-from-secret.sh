#!/bin/sh
# Entrypoint wrapper of langfuse-web and langfuse-worker (docker-compose.yml). Langfuse reads its
# credentials only from environment variables, so they are kept in the docker secret `langfuse.env`
# (KEY=value lines, written by scripts/gen_prod_secrets.py) instead of the compose file, and exported
# here just before the image's own entrypoint runs. Nothing is printed.
set -eu
secret_file=/run/secrets/langfuse.env
[ -r "$secret_file" ] || { echo "missing docker secret langfuse.env" >&2; exit 1; }
while IFS= read -r line || [ -n "$line" ]; do
  # shellcheck disable=SC2163 # `export "$line"` exports the KEY=value pair itself
  case "$line" in
    '' | '#'*) continue ;;
    *=*) export "$line" ;;
    *) echo "langfuse.env: malformed line" >&2; exit 1 ;;
  esac
done <"$secret_file"
exec "$@"
