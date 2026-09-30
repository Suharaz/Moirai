"""Generate the production docker secrets of docker-compose.yml into `secrets/` (phase 10).

Every secret that is a random value, or is derived from one, is written here: Postgres role passwords and
the per-service DSNs built from them, Redis ACL passwords with the rendered ACL file and per-service URLs,
the config-api session secret, the Telegram /resume TOTP seed, public-store users, Grafana and Langfuse
credentials. Existing values are kept; derived files (DSNs, URLs, ACL, langfuse.env) are rewritten from
them, so the script is safe to re-run. To rotate one value, delete its file and run the script again,
then recreate the containers that mount it. A secret file that exists but is empty stops the script
(it is never silently replaced by a new value); every file is written atomically (temporary file in the
same directory, fsync, rename).

Files the operator provides (never generated here) are checked and listed when missing:
- `scope_private_key.<service>.json`: `scripts/gen_scope_keys.py`;
- `order_signing_key`, `testnet_driver_signing_key`, `order_verify_keys.json`:
  `python -m hdt.risk.signer generate` (phase 09);
- `backup_s3_access_key`, `backup_s3_secret_key`: the write-only key of the backup bucket;
- `alertmanager_deadman_url`: the ping URL of the external dead-man check (docs/runbook.md section 9).

Every file in the directory ends up mode 0444 (the directory itself 0700): the files are bind-mounted
into containers that run as unprivileged users. Secret values are never printed. The only output
besides file names is the otpauth URI of a newly
created Telegram TOTP seed, shown once so it can be enrolled in an authenticator app.
Exit code 1 when an operator-provided file is missing.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import secrets
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Final

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from gen_redis_acl import PLACEHOLDER, TEMPLATE, render  # noqa: E402
from hdt.configapi.totp import new_totp_secret, provisioning_uri  # noqa: E402

DEFAULT_DIR: Final[Path] = ROOT / "secrets"

# Postgres login roles (hdt_<name>): the service roles of deploy/postgres/init/01-roles.sh, plus `backup`
# and `monitor` of deploy/postgres/init/02-ops-roles.sh.
PG_ROLES: Final[tuple[str, ...]] = (
    "migrator",
    "recorder",
    "council",
    "scorer",
    "risk",
    "execution",
    "news",
    "veto_scan",
    "configapi",
    "console_ro",
    "publisher_ro",
    "telegram",
    "backup",
    "monitor",
)
# pg_dsn.<file name> -> role it connects as.
PG_DSNS: Final[dict[str, str]] = {
    "migrator": "migrator",
    "recorder": "recorder",
    "council": "council",
    "scorer": "scorer",
    "risk": "risk",
    "execution": "execution",
    "news": "news",
    "veto_scan": "veto_scan",
    "configapi": "configapi",
    "console": "console_ro",
    "publisher": "publisher_ro",
    "telegram": "telegram",
}
# Redis ACL users that get a redis_url.<user> file (the others connect nowhere or only as admin).
REDIS_URL_USERS: Final[tuple[str, ...]] = (
    "recorder",
    "council",
    "news",
    "veto_scan",
    "scorer",
    "risk",
    "execution",
    "testnet_driver",
    "configapi",
    "telegram",
)
SCOPE_KEY_SERVICES: Final[tuple[str, ...]] = (
    "recorder",
    "council",
    "news",
    "veto_scan",
    "scorer",
    "execution",
    "telegram",
)
OPERATOR_FILES: Final[tuple[str, ...]] = (
    *(f"scope_private_key.{service}.json" for service in SCOPE_KEY_SERVICES),
    "order_signing_key",
    "testnet_driver_signing_key",
    "order_verify_keys.json",
    "backup_s3_access_key",
    "backup_s3_secret_key",
    "alertmanager_deadman_url",
)
PG_HOST: Final[str] = "postgres:5432"
PG_DATABASE: Final[str] = "hdt"
REDIS_HOST: Final[str] = "redis:6379"
LANGFUSE_MINIO_USER: Final[str] = "langfuse"


def _token(nbytes: int = 32) -> str:
    """URL-safe random token ([A-Za-z0-9_-]), safe inside DSNs, URLs and the Redis ACL file."""
    return secrets.token_urlsafe(nbytes)


class SecretDir:
    """Reads and writes secret files (mode 0444 inside a 0700 directory: containers run as other users)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.created: list[str] = []
        self.written: list[str] = []

    def prepare(self) -> None:
        self.path.mkdir(parents=True, exist_ok=True)
        if os.name == "posix":
            self.path.chmod(0o700)

    def read(self, name: str) -> str | None:
        """Value of `name`, None when the file does not exist; an existing empty file is an error."""
        file = self.path / name
        if not file.is_file():
            return None
        value = file.read_text(encoding="utf-8").strip()
        if not value:
            raise SystemExit(
                f"{file} exists but is empty; restore its value, or delete the file to generate a new one"
            )
        return value

    def write(self, name: str, value: str) -> None:
        """Atomically replace `name` with `value` (temporary file, fsync, rename); no-op when unchanged."""
        file = self.path / name
        if file.is_file() and file.read_text(encoding="utf-8").strip() == value:
            return
        fd, tmp_name = tempfile.mkstemp(prefix=f".{name}.", suffix=".tmp", dir=self.path)
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(value + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            if os.name == "posix":
                tmp.chmod(0o444)
            elif file.exists():
                file.chmod(0o600)  # Windows refuses to replace a read-only file.
            os.replace(tmp, file)
        except BaseException:
            with contextlib.suppress(OSError):
                tmp.unlink()
            raise
        if os.name == "posix":
            directory = os.open(self.path, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        self.written.append(name)

    def normalize_permissions(self) -> None:
        """Every file readable by the container users (0444), the directory closed to other host users."""
        if os.name != "posix":
            return
        for file in self.path.iterdir():
            if file.is_file():
                file.chmod(0o444)

    def keep_or_create(self, name: str, factory: str) -> str:
        """Existing value of `name`, or `factory` written as its new value."""
        value = self.read(name)
        if value is not None:
            return value
        self.write(name, factory)
        self.created.append(name)
        return factory


def generate_postgres(store: SecretDir) -> None:
    store.keep_or_create("pg_superuser_password", _token())
    passwords = {role: store.keep_or_create(f"pg_password.{role}", _token()) for role in PG_ROLES}
    for name, role in PG_DSNS.items():
        dsn = f"postgresql+psycopg://hdt_{role}:{passwords[role]}@{PG_HOST}/{PG_DATABASE}"
        store.write(f"pg_dsn.{name}", dsn)


def redis_users() -> list[str]:
    """ACL user names of deploy/redis/users.acl.template, lower case (placeholder VETO_SCAN -> veto_scan)."""
    return sorted({match.lower() for match in PLACEHOLDER.findall(TEMPLATE.read_text(encoding="utf-8"))})


def generate_redis(store: SecretDir) -> None:
    passwords = {user: store.keep_or_create(f"redis_password.{user}", _token()) for user in redis_users()}

    def lookup(name: str) -> str | None:
        return passwords.get(name.removeprefix("HDT_REDIS_PASSWORD_").lower())

    store.write("redis_users.acl", render(TEMPLATE.read_text(encoding="utf-8"), lookup).rstrip("\n"))
    for user in REDIS_URL_USERS:
        store.write(f"redis_url.{user}", f"redis://{user}:{passwords[user]}@{REDIS_HOST}/0")
    # redis-exporter reads its password from a JSON map keyed by redis://<user>@<host:port>.
    store.write(
        "redis_password.monitor.json", json.dumps({f"redis://monitor@{REDIS_HOST}": passwords["monitor"]})
    )


def generate_services(store: SecretDir) -> str | None:
    """Session secret, Telegram TOTP seed, public store, Grafana. Returns the otpauth URI of a new seed."""
    store.keep_or_create("session_secret", _token(48))
    uri = None
    if store.read("telegram_totp_secret") is None:
        seed = new_totp_secret()
        store.keep_or_create("telegram_totp_secret", seed)
        uri = provisioning_uri(seed, "telegram-resume")
    # MinIO: access keys 3-20 characters, secret keys 8-40 characters.
    store.keep_or_create("public_store_root_user", f"root{secrets.token_hex(6)}")
    store.keep_or_create("public_store_root_password", _token(30))
    store.keep_or_create("public_store_writer_access_key", f"publisher{secrets.token_hex(5)}")
    store.keep_or_create("public_store_writer_secret_key", _token(30))
    store.keep_or_create("public_store_reader_access_key", f"dashboard{secrets.token_hex(5)}")
    store.keep_or_create("public_store_reader_secret_key", _token(30))
    store.keep_or_create("grafana_admin_password", _token(24))
    return uri


def generate_langfuse(store: SecretDir) -> None:
    pg = store.keep_or_create("langfuse_pg_password", _token())
    redis = store.keep_or_create("langfuse_redis_password", _token())
    clickhouse = store.keep_or_create("langfuse_clickhouse_password", _token())
    minio = store.keep_or_create("langfuse_minio_password", _token(30))
    public_key = store.keep_or_create("langfuse_public_key", f"pk-lf-{uuid.uuid4()}")
    secret_key = store.keep_or_create("langfuse_secret_key", f"sk-lf-{uuid.uuid4()}")
    values = {
        "DATABASE_URL": f"postgresql://langfuse:{pg}@langfuse-postgres:5432/langfuse",
        "SALT": store.keep_or_create("langfuse_salt", _token()),
        "ENCRYPTION_KEY": store.keep_or_create("langfuse_encryption_key", secrets.token_hex(32)),
        "NEXTAUTH_SECRET": store.keep_or_create("langfuse_nextauth_secret", _token()),
        "CLICKHOUSE_PASSWORD": clickhouse,
        "REDIS_AUTH": redis,
        "LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID": LANGFUSE_MINIO_USER,
        "LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY": minio,
        "LANGFUSE_S3_MEDIA_UPLOAD_ACCESS_KEY_ID": LANGFUSE_MINIO_USER,
        "LANGFUSE_S3_MEDIA_UPLOAD_SECRET_ACCESS_KEY": minio,
        "LANGFUSE_INIT_PROJECT_PUBLIC_KEY": public_key,
        "LANGFUSE_INIT_PROJECT_SECRET_KEY": secret_key,
        "LANGFUSE_INIT_USER_PASSWORD": store.keep_or_create("langfuse_init_user_password", _token(24)),
    }
    store.write("langfuse.env", "\n".join(f"{key}={value}" for key, value in values.items()))


def missing_operator_files(store: SecretDir) -> list[str]:
    return [name for name in OPERATOR_FILES if store.read(name) is None]


def generated_names() -> set[str]:
    """Every file name this script writes (docker-compose.yml secrets are checked against it in tests)."""
    names = {"pg_superuser_password", "redis_users.acl", "session_secret", "telegram_totp_secret"}
    names |= {f"pg_password.{role}" for role in PG_ROLES}
    names |= {f"pg_dsn.{name}" for name in PG_DSNS}
    names |= {f"redis_password.{user}" for user in redis_users()}
    names |= {f"redis_url.{user}" for user in REDIS_URL_USERS}
    names.add("redis_password.monitor.json")
    names |= {
        f"public_store_{suffix}"
        for suffix in (
            "root_user",
            "root_password",
            "writer_access_key",
            "writer_secret_key",
            "reader_access_key",
            "reader_secret_key",
        )
    }
    names |= {"grafana_admin_password", "langfuse.env"}
    names |= {
        f"langfuse_{suffix}"
        for suffix in (
            "pg_password",
            "redis_password",
            "clickhouse_password",
            "minio_password",
            "public_key",
            "secret_key",
            "salt",
            "encryption_key",
            "nextauth_secret",
            "init_user_password",
        )
    }
    return names


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dir", type=Path, default=DEFAULT_DIR, help="secrets directory (default: secrets/)")
    args = parser.parse_args(argv)
    store = SecretDir(args.dir)
    store.prepare()
    generate_postgres(store)
    generate_redis(store)
    uri = generate_services(store)
    generate_langfuse(store)
    store.normalize_permissions()
    for name in sorted(store.written):
        state = "created" if name in store.created else "updated"
        print(f"{state}: {name}")
    if uri is not None:
        print("Telegram /resume TOTP (enroll now, shown once):")
        print(uri)
    missing = missing_operator_files(store)
    for name in missing:
        print(f"missing (operator-provided): {name}")
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
