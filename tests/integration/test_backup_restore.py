"""Backups restore into a scratch environment (phase 10, deploy/backup, services/backup).

- Logical + lake (dev Postgres): deploy/postgres/init/02-ops-roles.sh brings the dev cluster's hdt_backup
  role up to date, as an operator does on an existing cluster. A migrated database with rows in the
  row-level-security tables `secrets` and `store` is dumped as hdt_backup, and a lake tree (with recorder
  temp files, which are skipped) is backed up, into a file:/// target; nothing readable is left in the
  target, and `hdt-restore` rebuilds both into a scratch database and directory. `latest` skips
  incomplete and future sets, an explicit incomplete id is refused, a tampered checksum is refused.
- Failure paths: a failed step leaves no work files and records its failure and retry backoff; uploads
  never replace an existing object (file target, and an S3 target served by MinIO with
  `If-None-Match: *`); a WAL upload stored but not recorded counts as shipped on the next run, while an
  object whose upload never started here is refused; hashing waits for a slow hasher and a step that
  ends after UTC midnight counts for the day it started; the daemon reads a zero-padded backup hour as
  decimal and refuses an invalid one; `hdt-restore wal` exits 1 only for a segment that was never
  shipped and 255 for any other failure; deploy/postgres/archive-wal.sh accepts a re-archived identical
  segment and refuses a different one.
- Alert rules (promtool): failing and missing backup steps, overdue restore drills, persistent alert
  delivery failures.
- Physical + WAL (throwaway cluster started with the production command of docker-compose.yml, whose
  archive_command is deploy/postgres/archive-wal.sh): base backup, more commits, WAL shipping, then two
  restores into fresh volumes: to the end of the shipped WAL (every commit present) and to a point in time
  (later commits absent); a restore drill is recorded only with ids of sets uploaded from that host.

All need Docker (the `hdt/backup` image); they are skipped when it is absent.
"""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
import yaml
from alembic import command
from alembic.config import Config

from hdt.core.config import read_env_value

ROOT = Path(__file__).resolve().parents[2]
BACKUP_IMAGE = "hdt/backup:pytest"
POSTGRES_IMAGE = "hdt/postgres:pytest"
INIT_DIR = ROOT / "deploy" / "postgres" / "init"
ARCHIVE_SCRIPT = ROOT / "deploy" / "postgres" / "archive-wal.sh"
PG_ROLES = (
    "MIGRATOR",
    "RECORDER",
    "COUNCIL",
    "SCORER",
    "RISK",
    "EXECUTION",
    "NEWS",
    "VETO_SCAN",
    "CONFIGAPI",
    "CONSOLE_RO",
    "PUBLISHER_RO",
    "TELEGRAM",
    "BACKUP",
    "MONITOR",
)

pytestmark = [pytest.mark.docker, pytest.mark.integration]


def _docker(*args: str, check: bool = True, timeout: float = 300) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(  # noqa: S603 - fixed docker CLI, arguments built by the test
        ["docker", *args],  # noqa: S607 - docker from PATH, as an operator runs it
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=ROOT,
        check=False,
    )
    if check and result.returncode != 0:
        raise AssertionError(f"docker {' '.join(args[:3])} failed ({result.returncode}):\n{result.stderr}")
    return result


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return _docker("info", "--format", "{{.ServerVersion}}", check=False, timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.fixture(scope="module")
def images() -> None:
    if not _docker_available():
        pytest.skip("docker is not available")
    _docker("build", "-q", "-f", "services/backup/Dockerfile", "-t", BACKUP_IMAGE, ".", timeout=1200)
    _docker("build", "-q", "-f", "services/postgres/Dockerfile", "-t", POSTGRES_IMAGE, ".", timeout=1200)


def _env_args(env: dict[str, str]) -> list[str]:
    return [arg for key, value in env.items() for arg in ("-e", f"{key}={value}")]


def _env_password(name: str) -> str:
    value = read_env_value(name)
    if value is None:
        raise RuntimeError(f"{name} is not set: add it to .env (see .env.example)")
    return str(value)


def _public_key(identity: str) -> str:
    match = re.search(r"^# public key: (age1[0-9a-z]+)$", identity, re.MULTILINE)
    assert match is not None, "age-keygen wrote no public key"
    return match.group(1)


def _tree(root: Path) -> dict[str, bytes]:
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def _backup_id(at: datetime) -> str:
    return at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def _compose() -> dict[str, Any]:
    data = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


# ------------------------------------------------------------------------------ logical + lake (dev pg)


def _container_pg(admin_dsn: str) -> tuple[list[str], sa.engine.URL]:
    """docker run network args and the admin URL as seen from a container."""
    url = sa.engine.make_url(admin_dsn)
    if sys.platform.startswith("linux"):
        return ["--network", "host"], url
    host = "host.docker.internal" if url.host in ("127.0.0.1", "localhost", "::1") else url.host
    return [], url.set(host=host)


def _user_args() -> list[str]:
    """Bind-mounted host directories stay usable by the test on Linux (container runs as the host user)."""
    if sys.platform == "win32":
        return []
    return ["--user", f"{os.getuid()}:{os.getgid()}"]


@pytest.fixture(scope="module")
def backup_password(images: None, pg_admin_dsn: str) -> str:
    """Runs 02-ops-roles.sh against the dev cluster (the existing-cluster path of docs/runbook.md section
    13: roles created or altered in place); returns the password of hdt_backup."""
    net_args, url = _container_pg(pg_admin_dsn)
    password = _env_password("HDT_PG_PASSWORD_BACKUP")
    env = {
        "PGHOST": str(url.host),
        "PGPORT": str(url.port or 5432),
        "PGPASSWORD": str(url.password or ""),
        "POSTGRES_USER": str(url.username),
        "POSTGRES_DB": str(url.database or "postgres"),
        "HDT_PG_PASSWORD_BACKUP": password,
        "HDT_PG_PASSWORD_MONITOR": _env_password("HDT_PG_PASSWORD_MONITOR"),
    }
    _docker(
        "run", "--rm", *net_args, "-v", f"{INIT_DIR}:/init:ro", *_env_args(env),
        BACKUP_IMAGE, "bash", "/init/02-ops-roles.sh",
    )  # fmt: skip
    admin = sa.create_engine(pg_admin_dsn)
    try:
        with admin.connect() as conn:
            attributes = conn.execute(
                sa.text(
                    "SELECT rolname, rolcanlogin, rolreplication, rolbypassrls, rolsuper FROM pg_roles "
                    "WHERE rolname IN ('hdt_backup', 'hdt_monitor') ORDER BY rolname"
                )
            ).all()
    finally:
        admin.dispose()
    assert [tuple(row) for row in attributes] == [
        ("hdt_backup", True, True, True, False),
        ("hdt_monitor", True, False, False, False),
    ]
    return password


class LocalRun:
    """`hdt-backup` / `hdt-restore` runs against a file:/// target in host directories."""

    def __init__(self, base: Path, net_args: list[str], pg: sa.engine.URL) -> None:
        self.target = base / "target"
        self.state = base / "state"
        self.keys = base / "keys"
        self.lake = base / "lake"
        for directory in (self.target, self.state, self.keys, self.lake):
            directory.mkdir(parents=True)
        self.net_args = net_args
        self.pg = pg
        self.recipient = ""

    def run(
        self,
        *command: str,
        env: dict[str, str] | None = None,
        check: bool = True,
        volumes: tuple[str, ...] = (),
    ) -> Any:
        mounts = [
            "-v", f"{self.target}:/backup",
            "-v", f"{self.state}:/state",
            "-v", f"{self.keys}:/keys",
            "-v", f"{self.lake}:/data/lake:ro",
            *(arg for volume in volumes for arg in ("-v", volume)),
        ]  # fmt: skip
        base_env = {
            "HDT_BACKUP_TARGET": "file:///backup",
            "HDT_BACKUP_STATE_DIR": "/state",
            "HDT_BACKUP_METRICS_DIR": "/state/metrics",
            "HDT_BACKUP_LAKE_DIR": "/data/lake",
            "HDT_BACKUP_WAL_DIR": "/state/wal",
            "HDT_BACKUP_AGE_RECIPIENTS": self.recipient,
            "HDT_RESTORE_AGE_IDENTITY_FILE": "/keys/identity.txt",
            "TMPDIR": "/tmp",  # noqa: S108 - tmpfs inside the throwaway container
        }
        return _docker(
            "run", "--rm", *self.net_args, *_user_args(), *mounts,
            *_env_args({**base_env, **(env or {})}), BACKUP_IMAGE, *command,
            check=check,
        )  # fmt: skip

    def keygen(self) -> None:
        self.run("age-keygen", "-o", "/keys/identity.txt")
        self.recipient = _public_key((self.keys / "identity.txt").read_text(encoding="utf-8"))

    def backup_env(self, database: str, password: str) -> dict[str, str]:
        """Connection of hdt-backup: the hdt_backup role, as in production."""
        return {
            "PGHOST": str(self.pg.host),
            "PGPORT": str(self.pg.port or 5432),
            "PGUSER": "hdt_backup",
            "PGDATABASE": database,
            "HDT_PG_PASSWORD_BACKUP": password,
        }

    def dsn(self, database: str) -> str:
        return self.pg.set(drivername="postgresql", database=database).render_as_string(hide_password=False)

    def metrics(self) -> str:
        return (self.state / "metrics" / "hdt_backup.prom").read_text(encoding="utf-8")


def _migrated(url: str) -> None:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    command.upgrade(cfg, "head")


@pytest.fixture
def databases(fresh_database: Callable[[], AbstractContextManager[str]]) -> Iterator[tuple[str, str]]:
    """(migrated source database URL, empty restore target URL), admin credentials."""
    with fresh_database() as source, fresh_database() as target:
        _migrated(source)
        yield source, target


SNAPSHOT_QUERIES = (
    "SELECT id, label, payload::text, amount::text, at, encode(blob, 'hex') FROM probe ORDER BY id",
    "SELECT last_value FROM probe_id_seq",
    "SELECT scope, name, version, encode(sealed_blob, 'hex'), status, check_details::text, created_at "
    "FROM secrets ORDER BY scope, name, version",
    "SELECT prefix, key, value::text, created_at FROM store ORDER BY prefix, key",
    "SELECT relname, relrowsecurity FROM pg_class WHERE relname IN ('secrets', 'store') ORDER BY relname",
    "SELECT tablename, policyname FROM pg_policies ORDER BY tablename, policyname",
    "SELECT version_num FROM alembic_version",
)


def _snapshot(url: str) -> list[list[tuple[Any, ...]]]:
    engine = sa.create_engine(url)
    try:
        with engine.connect() as conn:
            return [[tuple(row) for row in conn.execute(sa.text(query))] for query in SNAPSHOT_QUERIES]
    finally:
        engine.dispose()


def _seed(url: str, sentinel: str) -> None:
    engine = sa.create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "CREATE TABLE probe (id bigserial PRIMARY KEY, label text NOT NULL, payload jsonb, "
                    "amount numeric(20, 8), at timestamptz NOT NULL, blob bytea)"
                )
            )
            conn.execute(sa.text("CREATE INDEX probe_label ON probe (label)"))
            conn.execute(
                sa.text(
                    "INSERT INTO probe (label, payload, amount, at, blob) "
                    "SELECT :sentinel || '-' || g, jsonb_build_object('g', g, 'odd', g % 2 = 1), "
                    "g * 1.25, TIMESTAMPTZ '2026-09-01 00:00:00+00' + g * INTERVAL '1 minute', "
                    "sha256(g::text::bytea) FROM generate_series(1, 500) AS g"
                ),
                {"sentinel": sentinel},
            )
            # Row-level security tables: pg_dump refuses them unless the role bypasses RLS (review C1).
            conn.execute(
                sa.text(
                    "INSERT INTO secrets (scope, name, version, sealed_blob, last4, fingerprint, status, "
                    "check_details, created_by, created_at) VALUES ('exec', 'binance', 1, :blob, 'wxyz', "
                    "'fp-1', 'active', '{}'::jsonb, 'pytest', TIMESTAMPTZ '2026-09-02 00:00:00+00')"
                ),
                {"blob": sentinel.encode()},
            )
            conn.execute(
                sa.text(
                    "INSERT INTO store (prefix, key, value) VALUES ('news.episodic', 'evt-1', "
                    "jsonb_build_object('record_type', 'known_event', 'note', CAST(:sentinel AS text)))"
                ),
                {"sentinel": sentinel},
            )
    finally:
        engine.dispose()


@pytest.mark.pg
def test_logical_and_lake_backups_restore_into_scratch(
    backup_password: str, pg_admin_dsn: str, databases: tuple[str, str], tmp_path: Path
) -> None:
    source_url, target_url = databases
    source_db = sa.engine.make_url(source_url).database
    target_db = sa.engine.make_url(target_url).database
    assert source_db is not None
    assert target_db is not None
    sentinel = f"hdt-backup-sentinel-{secrets.token_hex(8)}"
    _seed(source_url, sentinel)

    net_args, container_pg = _container_pg(pg_admin_dsn)
    run = LocalRun(tmp_path, net_args, container_pg)
    run.keygen()

    parquet = os.urandom(200_000)
    files = {
        "binance/depth/date=2026-09-27/hour=01/part-0.parquet": parquet,
        "binance/depth/date=2026-09-27/hour=02/part-0.parquet": os.urandom(50_000),
        "cmc/quotes/date=2026-09-27/part-0.parquet": sentinel.encode() * 100,
        "copies/same-content.parquet": parquet,
    }
    # Recorder temp files (lake/raw_store.py) and dotfiles are never backed up (review I10).
    skipped = {
        "binance/depth/date=2026-09-27/hour=02/part-1.parquet.tmp": os.urandom(1_000),
        "cmc/quotes/date=2026-09-27/staging.tmp": os.urandom(1_000),
        "cmc/.lock": b"",
    }
    for relative, content in {**files, **skipped}.items():
        path = run.lake / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    run.run("hdt-backup", "logical", env=run.backup_env(source_db, backup_password))
    run.run("hdt-backup", "lake")

    # Client-side encryption: no plaintext row or lake content is readable in the target.
    for name, content in _tree(run.target).items():
        assert sentinel.encode() not in content, f"plaintext found in {name}"
    metrics = run.metrics()
    assert 'hdt_backup_last_success_timestamp_seconds{kind="logical"}' in metrics
    assert 'hdt_backup_last_success_timestamp_seconds{kind="lake"}' in metrics
    # Identical lake files are stored once; temp files are not stored at all.
    assert len(list((run.target / "lake" / "objects").iterdir())) == 3
    assert not list((run.state / "work").glob("*")), "steps must remove their work directories"

    (logical_id,) = [p.name for p in (run.target / "logical").iterdir()]
    (manifest,) = list((run.target / "lake" / "manifests").iterdir())
    lake_id = manifest.name.removesuffix(".tsv.age")

    # Sets planted with the write-only key cannot pose as the newest backup (review I9): a newer set
    # without its SHA256SUMS (upload interrupted), and complete-looking sets dated in the future.
    now = datetime.now(UTC)
    dump = next((run.target / "logical" / logical_id).glob("*.dump.age"))
    incomplete, future = _backup_id(now + timedelta(seconds=60)), _backup_id(now + timedelta(days=1))
    (run.target / "logical" / incomplete).mkdir()
    shutil.copy(dump, run.target / "logical" / incomplete / dump.name)
    shutil.copytree(run.target / "logical" / logical_id, run.target / "logical" / future)
    shutil.copy(manifest, manifest.with_name(f"{future}.tsv.age"))
    assert run.run("hdt-restore", "latest", "logical").stdout.strip() == logical_id
    assert run.run("hdt-restore", "latest", "lake").stdout.strip() == lake_id
    refused = run.run("hdt-restore", "logical", incomplete, run.dsn(target_db), check=False)
    assert refused.returncode != 0
    assert "not a complete backup" in refused.stderr

    run.run("hdt-restore", "logical", "latest", run.dsn(target_db))
    restored = _snapshot(target_url)
    assert restored == _snapshot(source_url)
    assert restored[2], "the RLS table secrets must be dumped with its rows"
    assert restored[3], "the RLS table store must be dumped with its rows"

    restored_lake = tmp_path / "state" / "restored-lake"
    run.run("hdt-restore", "lake", lake_id, "/state/restored-lake")
    assert _tree(restored_lake) == files

    # A later lake backup uploads only new content and the newest manifest lists the new file.
    files["binance/depth/date=2026-09-27/hour=03/part-0.parquet"] = os.urandom(10_000)
    new_path = run.lake / "binance/depth/date=2026-09-27/hour=03/part-0.parquet"
    new_path.parent.mkdir(parents=True, exist_ok=True)
    new_path.write_bytes(files["binance/depth/date=2026-09-27/hour=03/part-0.parquet"])
    time.sleep(1.1)  # backup ids have one-second resolution
    run.run("hdt-backup", "lake")
    assert len(list((run.target / "lake" / "objects").iterdir())) == 4
    run.run("hdt-restore", "lake", "latest", "/state/restored-lake-2")
    assert _tree(tmp_path / "state" / "restored-lake-2") == files

    # A dump whose recorded checksum does not match is refused before anything is restored.
    sums = run.target / "logical" / logical_id / "SHA256SUMS"
    name = sums.read_text(encoding="utf-8").split()[1]
    sums.write_text(f"{hashlib.sha256(b'other').hexdigest()}  {name}\n", encoding="utf-8", newline="\n")
    refused = run.run("hdt-restore", "logical", logical_id, run.dsn(target_db), check=False)
    assert refused.returncode != 0
    assert "checksum mismatch" in refused.stderr


# ------------------------------------------------------------------------------------- failure paths


@pytest.mark.pg
def test_a_failed_step_leaves_no_work_files_and_backs_off(
    backup_password: str,
    pg_admin_dsn: str,
    fresh_database: Callable[[], AbstractContextManager[str]],
    tmp_path: Path,
) -> None:
    with fresh_database() as url:
        source_db = sa.engine.make_url(url).database
        assert source_db is not None
        _backs_off_then_succeeds(backup_password, pg_admin_dsn, source_db, tmp_path)


def _backs_off_then_succeeds(backup_password: str, pg_admin_dsn: str, source_db: str, tmp_path: Path) -> None:
    net_args, container_pg = _container_pg(pg_admin_dsn)
    run = LocalRun(tmp_path, net_args, container_pg)
    run.keygen()
    wrong = run.backup_env(source_db, "not-the-password")

    for attempt in (1, 2):
        failed = run.run("hdt-backup", "logical", env=wrong, check=False)
        assert failed.returncode != 0
        assert not list((run.state / "work").glob("*")), "a failed step must remove its work directory"
        day, attempts, next_at = (run.state / "retry.logical").read_text(encoding="utf-8").split()
        assert (day, int(attempts)) == (datetime.now(UTC).date().isoformat(), attempt)
        # HDT_BACKUP_RETRY_BASE_S (300 s) doubles with every failed attempt of the day.
        assert int(next_at) - time.time() == pytest.approx(300 * 2 ** (attempt - 1), abs=60)
    assert 'hdt_backup_last_failure_timestamp_seconds{kind="logical"}' in run.metrics()
    assert 'hdt_backup_last_success_timestamp_seconds{kind="logical"}' not in run.metrics()
    assert not list(run.target.rglob("*.age")), "nothing is uploaded by a failed dump"

    run.run("hdt-backup", "logical", env=run.backup_env(source_db, backup_password))
    assert not (run.state / "retry.logical").exists()
    done = (run.state / "done.logical").read_text(encoding="utf-8").strip()
    assert done == datetime.now(UTC).date().isoformat()
    assert 'hdt_backup_last_success_timestamp_seconds{kind="logical"}' in run.metrics()


def _put_twice_script() -> str:
    return (
        "source /usr/local/lib/hdt-backup/lib.sh; "
        "printf first >/tmp/a; printf second >/tmp/b; "
        "put_object t/object /tmp/a && echo first=0; "
        "put_object t/object /tmp/b || echo second=$?; "
        "get_object t/object /tmp/c && echo content=$(cat /tmp/c); "
        "get_object t/missing /tmp/d || echo missing=$?"
    )


def test_uploads_never_replace_an_object_in_a_file_target(images: None, tmp_path: Path) -> None:
    run = LocalRun(tmp_path, [], sa.engine.make_url("postgresql://unused@localhost/unused"))
    out = run.run("bash", "-c", _put_twice_script()).stdout.split()
    assert out == ["first=0", "second=3", "content=first", "missing=2"]
    assert (run.target / "t" / "object").read_bytes() == b"first"
    assert not list(run.target.rglob("*.partial"))


def test_uploads_never_replace_an_object_in_an_s3_target(images: None) -> None:
    """The production path: SigV4 PUT with `If-None-Match: *` against MinIO (the public-store image)."""
    image = str(_compose()["services"]["public-store"]["image"])
    prefix = f"hdts3{secrets.token_hex(3)}"
    user, password = "hdtroot", secrets.token_urlsafe(24)
    try:
        _docker("network", "create", "--internal", prefix)
        _docker(
            "run", "-d", "--name", prefix, "--network", prefix,
            *_env_args({"MINIO_ROOT_USER": user, "MINIO_ROOT_PASSWORD": password}),
            image, "server", "/data",
        )  # fmt: skip
        deadline = time.monotonic() + 60
        while _docker("exec", prefix, "mc", "ready", "local", check=False).returncode != 0:
            assert time.monotonic() < deadline, "MinIO did not start"
            time.sleep(1)
        _docker("exec", prefix, "mc", "alias", "set", "hdt", "http://127.0.0.1:9000", user, password)
        _docker("exec", prefix, "mc", "mb", "hdt/backups")
        env = {
            "HDT_BACKUP_TARGET": "s3://backups/host-1",
            "HDT_BACKUP_S3_ENDPOINT": f"http://{prefix}:9000",
            "HDT_BACKUP_S3_ACCESS_KEY": user,
            "HDT_BACKUP_S3_SECRET_KEY": password,
            "HDT_RESTORE_S3_ACCESS_KEY": user,
            "HDT_RESTORE_S3_SECRET_KEY": password,
        }
        out = _docker(
            "run", "--rm", "--network", prefix, *_env_args(env), BACKUP_IMAGE,
            "bash", "-c", _put_twice_script(),
        ).stdout.split()  # fmt: skip
        assert out == ["first=0", "second=3", "content=first", "missing=2"]
    finally:
        _docker("rm", "-f", prefix, check=False)
        _docker("network", "rm", prefix, check=False)


def test_restore_command_ends_recovery_only_for_a_segment_never_shipped(images: None, tmp_path: Path) -> None:
    run = LocalRun(tmp_path, [], sa.engine.make_url("postgresql://unused@localhost/unused"))
    run.keygen()
    segment = "/tmp/segment"  # noqa: S108 - path inside the throwaway backup container
    missing = run.run("hdt-restore", "wal", "000000010000000000000099", segment, check=False)
    assert missing.returncode == 1, missing.stderr
    # Anything else (here an object that does not decrypt) must stop recovery, never end it (review I8).
    (run.target / "wal").mkdir()
    (run.target / "wal" / "000000010000000000000098.zst.age").write_bytes(os.urandom(512))
    corrupt = run.run("hdt-restore", "wal", "000000010000000000000098", segment, check=False)
    assert corrupt.returncode == 255, corrupt.stderr
    unconfigured = run.run(
        "hdt-restore", "wal", "000000010000000000000099", segment, env={"HDT_BACKUP_TARGET": ""},
        check=False,
    )  # fmt: skip
    assert unconfigured.returncode == 255, unconfigured.stderr


def test_archive_command_is_idempotent_for_identical_segments_only(images: None) -> None:
    script = (
        "set -u; mkdir -p /tmp/archive; head -c 65536 /dev/urandom >/tmp/segment; "
        "run() { HDT_WAL_ARCHIVE_DIR=/tmp/archive "
        "bash /hdt/archive-wal.sh /tmp/segment 000000010000000000000001; }; "
        "run && echo first=0; run && echo again=0; "
        "head -c 65536 /dev/urandom >/tmp/segment; run 2>/dev/null || echo different=$?; "
        "ls -A /tmp/archive"
    )
    out = _docker(
        "run", "--rm", "-v", f"{ARCHIVE_SCRIPT}:/hdt/archive-wal.sh:ro", BACKUP_IMAGE, "bash", "-c", script
    ).stdout.split()
    assert out == ["first=0", "again=0", "different=1", "000000010000000000000001"]


def test_wal_upload_stored_but_not_recorded_counts_as_shipped(images: None, tmp_path: Path) -> None:
    """Review N6-B: an upload that reached the target but whose bookkeeping did not happen (the answer was
    lost, or the step was killed during the upload) must not stall WAL shipping for good. The first run
    stores segment 1 and then cannot record it; the next run counts the existing object as shipped,
    never replaces it, and ships segment 2. An existing object whose upload never started here is still
    refused and its segment kept."""
    run = LocalRun(tmp_path, [], sa.engine.make_url("postgresql://unused@localhost/unused"))
    run.keygen()
    wal = run.state / "wal"
    wal.mkdir()
    segments = {f"00000001000000000000000{i}": os.urandom(65_536) for i in (1, 2)}
    for name, content in segments.items():
        (wal / name).write_bytes(content)
    first_object = run.target / "wal" / "000000010000000000000001.zst.age"

    blocker = run.state / "wal_last_shipped.tmp"  # the record after the upload cannot be written
    blocker.mkdir()
    interrupted = run.run("hdt-backup", "wal", check=False)
    assert interrupted.returncode != 0
    assert "cannot record the shipped segment 000000010000000000000001" in interrupted.stderr
    stored = first_object.read_bytes()
    assert sorted(p.name for p in wal.iterdir()) == sorted(segments), "nothing is deleted before the record"

    blocker.rmdir()
    resumed = run.run("hdt-backup", "wal")
    assert "counted as shipped" in resumed.stderr
    assert not list(wal.iterdir()), "every shipped segment leaves the local archive"
    assert first_object.read_bytes() == stored, "the stored object is never replaced"
    assert (run.state / "wal_last_shipped").read_text(encoding="utf-8").strip() == "000000010000000000000002"
    for name, content in segments.items():
        run.run("hdt-restore", "wal", name, f"/state/restored-{name}")
        assert (run.state / f"restored-{name}").read_bytes() == content

    foreign = "000000010000000000000003"
    (wal / foreign).write_bytes(os.urandom(65_536))
    (run.target / "wal" / f"{foreign}.zst.age").write_bytes(b"not uploaded from here")
    refused = run.run("hdt-backup", "wal", check=False)
    assert refused.returncode != 0
    assert "already exists but was not shipped from here" in refused.stderr
    assert (wal / foreign).exists(), "a segment whose object is not ours stays local"


DATE_SHIM = """#!/bin/bash
# `date` as seen by hdt-backup: SHIM_HOUR fixes the UTC hour; with SHIM_ROLL_DAY the UTC day is
# 2026-09-27 until /tmp/day-rolled exists and 2026-09-28 afterwards.
if [[ "$*" == "-u +%H" && -n "${SHIM_HOUR:-}" ]]; then echo "$SHIM_HOUR"; exit 0; fi
if [[ "$*" == "-u +%F" && -n "${SHIM_ROLL_DAY:-}" ]]; then
  if [[ -e /tmp/day-rolled ]]; then echo 2026-09-28; else echo 2026-09-27; fi
  exit 0
fi
exec /usr/bin/date "$@"
"""
# A slow hasher: its line appears 2 s after the end of its input. UTC midnight passes while it hashes.
SHA256SUM_SHIM = """#!/bin/bash
touch /tmp/day-rolled
sum="$(/usr/bin/sha256sum "$@")"
sleep 2
printf '%s\\n' "$sum"
"""


def _shims(base: Path) -> str:
    """A /shim directory (put first in PATH) with the `date` and `sha256sum` stand-ins above."""
    shim = base / "shim"
    shim.mkdir()
    for name, text in (("date", DATE_SHIM), ("sha256sum", SHA256SUM_SHIM)):
        path = shim / name
        path.write_text(text, encoding="utf-8", newline="\n")
        path.chmod(0o755)
    return f"{shim}:/shim:ro"


def test_a_step_waits_for_its_hasher_and_counts_for_the_day_it_started(images: None, tmp_path: Path) -> None:
    """Review N7: a checksum is read only after the hasher has finished (a slow hasher used to leave it
    empty: "cannot hash"). Review N9: a daily step that ends after UTC midnight (here: while the hasher
    runs) is recorded for the day it started, so the next day's run is not skipped."""
    run = LocalRun(tmp_path, [], sa.engine.make_url("postgresql://unused@localhost/unused"))
    run.keygen()
    files = {"binance/depth/date=2026-09-27/hour=23/part-0.parquet": os.urandom(100_000)}
    for relative, content in files.items():
        path = run.lake / relative
        path.parent.mkdir(parents=True)
        path.write_bytes(content)
    shims = _shims(tmp_path)

    lake = "PATH=/shim:$PATH exec hdt-backup lake"
    run.run("bash", "-c", lake, env={"SHIM_ROLL_DAY": "1"}, volumes=(shims,))
    assert (run.state / "done.lake").read_text(encoding="utf-8").strip() == "2026-09-27"
    # The recorded sha256 is the content's: hdt-restore checks every restored file against it.
    run.run("hdt-restore", "lake", "latest", "/state/restored-lake")
    assert _tree(run.state / "restored-lake") == files


def test_daemon_reads_the_backup_hour_as_a_decimal_number(images: None, tmp_path: Path) -> None:
    """Review N8: HDT_BACKUP_HOUR_UTC=08 or 09 used to be read as an invalid octal number, so the daily
    steps never ran. Out-of-range or malformed hours stop the service at start."""
    run = LocalRun(tmp_path, [], sa.engine.make_url("postgresql://unused@localhost/unused"))
    run.keygen()
    shims = _shims(tmp_path)

    def daemon(hour: str, now: str, seconds: int) -> Any:
        """`hdt-backup daemon` for SECONDS with HDT_BACKUP_HOUR_UTC=HOUR while the UTC hour is NOW."""
        return run.run(
            "bash", "-c", f"PATH=/shim:$PATH exec timeout {seconds} hdt-backup daemon",
            env={"HDT_BACKUP_HOUR_UTC": hour, "SHIM_HOUR": now, "HDT_BACKUP_WAL_INTERVAL_S": "1"},
            check=False, volumes=(shims,),
        )  # fmt: skip

    early = daemon("09", "08", 4)
    assert "daily steps at or after 9:00 UTC" in early.stderr
    assert not (run.state / "done.lake").exists(), "no daily step before the backup hour"

    due = daemon("08", "10", 10)
    assert (run.state / "done.lake").exists(), due.stderr
    assert (run.state / "retry.base").exists(), "base ran (and failed without Postgres) in the same cycle"

    for hour in ("24", "-1", "8am"):
        refused = run.run("hdt-backup", "daemon", env={"HDT_BACKUP_HOUR_UTC": hour}, check=False)
        assert refused.returncode == 1
        assert f"HDT_BACKUP_HOUR_UTC must be a whole number from 0 to 23 (got '{hour}')" in refused.stderr


# Samples every minute: Prometheus treats a series as gone 5 minutes after its last sample.
BACKUP_RULE_TESTS = """
rule_files: [alerts.yml]
evaluation_interval: 1m
tests:
  # base failed and never succeeded; logical failed after its last success; lake recovered.
  - interval: 1m
    input_series:
      - series: 'hdt_backup_last_failure_timestamp_seconds{kind="base"}'
        values: '1000x60'
      - series: 'hdt_backup_last_success_timestamp_seconds{kind="logical"}'
        values: '500x60'
      - series: 'hdt_backup_last_failure_timestamp_seconds{kind="logical"}'
        values: '1000x60'
      - series: 'hdt_backup_last_success_timestamp_seconds{kind="lake"}'
        values: '2000x60'
      - series: 'hdt_backup_last_failure_timestamp_seconds{kind="lake"}'
        values: '1000x60'
    alert_rule_test:
      - eval_time: 10m
        alertname: BackupStepFailing
      - eval_time: 20m
        alertname: BackupStepFailing
        exp_alerts:
          - exp_labels: {kind: base, hdt_kind: backup_stale, severity: critical}
          - exp_labels: {kind: logical, hdt_kind: backup_stale, severity: critical}
  # A daily step that never reported at all pages too, not only WAL shipping.
  - interval: 1m
    input_series:
      - series: 'hdt_backup_last_success_timestamp_seconds{kind="wal"}'
        values: '0+60x2000'
      - series: 'hdt_backup_last_success_timestamp_seconds{kind="logical"}'
        values: '0+60x2000'
      - series: 'hdt_backup_last_success_timestamp_seconds{kind="lake"}'
        values: '0+60x2000'
    alert_rule_test:
      - eval_time: 29h
        alertname: BackupNeverSucceeded
      - eval_time: 31h
        alertname: BackupNeverSucceeded
        exp_alerts:
          - exp_labels: {kind: base, hdt_kind: backup_stale, severity: critical}
  # Monthly restore drill (review M17): the last one passed 35 days minus 1 hour before t=0.
  - interval: 1m
    input_series:
      - series: 'hdt_backup_last_success_timestamp_seconds{kind="restore_test"}'
        values: '-3020400x200'
    alert_rule_test:
      - eval_time: 90m
        alertname: RestoreDrillOverdue
      - eval_time: 150m
        alertname: RestoreDrillOverdue
        exp_alerts:
          - exp_labels: {kind: restore_test, hdt_kind: backup_stale, severity: warning}
  - interval: 1m
    input_series:
      - series: 'hdt_backup_last_success_timestamp_seconds{kind="wal"}'
        values: '0x3000'
    alert_rule_test:
      - eval_time: 47h
        alertname: RestoreDrillNeverRecorded
      - eval_time: 49h
        alertname: RestoreDrillNeverRecorded
        exp_alerts:
          - exp_labels: {kind: restore_test, hdt_kind: backup_stale, severity: warning}
"""


# Review N1: failed alert notifications page (through the dead-man path) only when they persist.
DELIVERY_RULE_TESTS = """
rule_files: [alerts.yml]
evaluation_interval: 1m
tests:
  # Failures in every window for 2 hours, then none.
  - interval: 1m
    input_series:
      - series: 'hdt_alerts_delivery_failures_total'
        values: '0+0.2x120 24x120'
    alert_rule_test:
      - eval_time: 20m
        alertname: AlertDeliveryFailing
      - eval_time: 40m
        alertname: AlertDeliveryFailing
        exp_alerts:
          - exp_labels: {hdt_kind: service_down, severity: critical, hdt_deadman: stop}
      - eval_time: 150m
        alertname: AlertDeliveryFailing
  # A 10-minute Telegram outage never pages.
  - interval: 1m
    input_series:
      - series: 'hdt_alerts_delivery_failures_total'
        values: '0+1x10 10x200'
    alert_rule_test:
      - eval_time: 30m
        alertname: AlertDeliveryFailing
      - eval_time: 60m
        alertname: AlertDeliveryFailing
"""


# Every process importing hdt.ops.metrics exports the bot heartbeat and the snapshot gauges at 0; only the
# owning job's series may drive TelegramBotStalled (which also stops the dead-man pings) and
# PublicSnapshotStale.
SHARED_GAUGE_RULE_TESTS = """
rule_files: [alerts.yml]
evaluation_interval: 1m
tests:
  # Healthy bot and publisher next to execution and risk exporting the same gauges at 0.
  - interval: 1m
    input_series:
      - series: 'hdt_telegram_poll_heartbeat_timestamp_seconds{job="telegram-bot"}'
        values: '0+60x30'
      - series: 'hdt_telegram_poll_heartbeat_timestamp_seconds{job="execution"}'
        values: '0x30'
      - series: 'hdt_public_snapshot_last_success_timestamp_seconds{job="public-publisher"}'
        values: '0+60x30'
      - series: 'hdt_public_snapshot_last_success_timestamp_seconds{job="risk"}'
        values: '0x30'
    alert_rule_test:
      - eval_time: 20m
        alertname: TelegramBotStalled
      - eval_time: 20m
        alertname: PublicSnapshotStale
  # The bot and the publisher stop; the other jobs' zero series alone must not hide or fake it.
  - interval: 1m
    input_series:
      - series: 'hdt_telegram_poll_heartbeat_timestamp_seconds{job="telegram-bot"}'
        values: '0+60x5 300x30'
      - series: 'hdt_telegram_poll_heartbeat_timestamp_seconds{job="execution"}'
        values: '0x35'
      - series: 'hdt_public_snapshot_last_success_timestamp_seconds{job="public-publisher"}'
        values: '0+60x5 300x30'
      - series: 'hdt_public_snapshot_last_success_timestamp_seconds{job="risk"}'
        values: '0x35'
    alert_rule_test:
      - eval_time: 20m
        alertname: TelegramBotStalled
        exp_alerts:
          - exp_labels:
              {job: telegram-bot, hdt_kind: service_down, severity: critical, hdt_deadman: stop}
      - eval_time: 20m
        alertname: PublicSnapshotStale
        exp_alerts:
          - exp_labels: {job: public-publisher, hdt_kind: public_snapshot_stale, severity: warning}
  # No bot series at all: absent() fires even though other jobs export the gauge.
  - interval: 1m
    input_series:
      - series: 'hdt_telegram_poll_heartbeat_timestamp_seconds{job="execution"}'
        values: '0x30'
    alert_rule_test:
      - eval_time: 10m
        alertname: TelegramBotStalled
        exp_alerts:
          - exp_labels: {job: telegram-bot, hdt_kind: service_down, severity: critical, hdt_deadman: stop}
"""


def _promtool_rule_test(tmp_path: Path, tests: str) -> None:
    """deploy/prometheus/alerts.yml evaluated by promtool against TESTS: expressions, `for` and labels."""
    if not _docker_available():
        pytest.skip("docker is not available")
    rules = yaml.safe_load((ROOT / "deploy" / "prometheus" / "alerts.yml").read_text(encoding="utf-8"))
    for group in rules["groups"]:
        for rule in group["rules"]:
            rule.pop("annotations", None)  # promtool compares annotations too; they are prose
    (tmp_path / "alerts.yml").write_text(yaml.safe_dump(rules), encoding="utf-8")
    (tmp_path / "rules.test.yml").write_text(tests, encoding="utf-8")
    image = str(_compose()["services"]["prometheus"]["image"])
    result = _docker(
        "run", "--rm", "-v", f"{tmp_path}:/t:ro", "--entrypoint", "promtool", image,
        "test", "rules", "/t/rules.test.yml",
        check=False,
    )  # fmt: skip
    assert result.returncode == 0, result.stdout + result.stderr


def test_backup_alerts_fire_for_failing_and_missing_steps(tmp_path: Path) -> None:
    """Review C2, M17."""
    _promtool_rule_test(tmp_path, BACKUP_RULE_TESTS)


def test_alert_delivery_failures_page_only_when_they_persist(tmp_path: Path) -> None:
    _promtool_rule_test(tmp_path, DELIVERY_RULE_TESTS)


def test_shared_gauges_of_other_jobs_never_drive_the_stall_alerts(tmp_path: Path) -> None:
    _promtool_rule_test(tmp_path, SHARED_GAUGE_RULE_TESTS)


# ------------------------------------------------------------------------- physical + WAL (throwaway)


def _compose_postgres_command() -> list[str]:
    command = _compose()["services"]["postgres"]["command"]
    assert isinstance(command, list)
    return [str(part) for part in command]


class Cluster:
    """Throwaway source cluster, WAL archive and backup work volume on a private network."""

    def __init__(self) -> None:
        self.prefix = f"hdtbk{secrets.token_hex(3)}"
        self.network = self.prefix
        self.source = f"{self.prefix}_src"
        self.pgdata = f"{self.prefix}_pgdata"
        self.wal = f"{self.prefix}_wal"
        self.work = f"{self.prefix}_work"
        self.containers: list[str] = []
        self.volumes = [self.pgdata, self.wal, self.work]
        self.passwords = {role: secrets.token_urlsafe(24) for role in PG_ROLES}
        self.recipient = ""

    def start(self) -> None:
        _docker("network", "create", "--internal", self.network)
        for volume in self.volumes:
            _docker("volume", "create", volume)
        env = {"POSTGRES_PASSWORD": secrets.token_urlsafe(24), "POSTGRES_DB": "hdt"}
        env |= {f"HDT_PG_PASSWORD_{role}": value for role, value in self.passwords.items()}
        _docker(
            "run", "-d", "--name", self.source, "--network", self.network, "--user", "postgres",
            "-v", f"{self.pgdata}:/var/lib/postgresql/data",
            "-v", f"{self.wal}:/var/lib/postgresql/wal_archive",
            "-v", f"{INIT_DIR}:/docker-entrypoint-initdb.d:ro",
            "-v", f"{ARCHIVE_SCRIPT}:/etc/hdt-postgres/archive-wal.sh:ro",
            *_env_args(env), POSTGRES_IMAGE, *_compose_postgres_command(),
        )  # fmt: skip
        self.containers.append(self.source)
        self.wait_ready(self.source, tcp=True)

    def wait_ready(self, container: str, *, tcp: bool = False, timeout: float = 180) -> None:
        host = ["-h", "127.0.0.1"] if tcp else ["-h", "/var/run/postgresql"]
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if _docker("exec", container, "pg_isready", *host, "-U", "postgres", check=False).returncode == 0:
                return
            time.sleep(1)
        logs = _docker("logs", container, check=False)
        raise AssertionError(f"{container} not ready:\n{logs.stdout[-3000:]}{logs.stderr[-3000:]}")

    def sql(self, container: str, statement: str, *, check: bool = True) -> str | None:
        out = _docker(
            "exec", container, "psql", "-h", "/var/run/postgresql", "-U", "postgres", "-d", "hdt",
            "-v", "ON_ERROR_STOP=1", "-tAc", statement, check=check,
        )  # fmt: skip
        return out.stdout.strip() if out.returncode == 0 else None

    def query(self, container: str, statement: str) -> str:
        result = self.sql(container, statement)
        assert result is not None
        return result

    def backup_env(self) -> dict[str, str]:
        return {
            "HDT_BACKUP_TARGET": "file:///var/lib/hdt-backup/target",
            "HDT_BACKUP_STATE_DIR": "/var/lib/hdt-backup/state",
            "HDT_BACKUP_METRICS_DIR": "/var/lib/hdt-backup/metrics",
            "HDT_BACKUP_WAL_DIR": "/var/lib/postgresql/wal_archive",
            "HDT_BACKUP_AGE_RECIPIENTS": self.recipient,
            "HDT_RESTORE_AGE_IDENTITY_FILE": "/var/lib/hdt-backup/keys/identity.txt",
            "PGHOST": self.source,
            "PGUSER": "hdt_backup",
            "PGDATABASE": "hdt",
            "HDT_PG_PASSWORD_BACKUP": self.passwords["BACKUP"],
        }

    def tool(self, *command: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return _docker(
            "run", "--rm", "--network", self.network,
            "-v", f"{self.work}:/var/lib/hdt-backup",
            "-v", f"{self.wal}:/var/lib/postgresql/wal_archive",
            *_env_args(self.backup_env()), BACKUP_IMAGE, *command,
            check=check,
        )  # fmt: skip

    def restore(self, name: str, *extra: str) -> str:
        """Restore the latest base backup into a new volume and start Postgres on it (archive recovery)."""
        container = f"{self.prefix}_{name}"
        volume = f"{self.prefix}_{name}_data"
        _docker("volume", "create", volume)
        self.volumes.append(volume)
        script = (
            "hdt-restore base latest /var/lib/postgresql/data "
            + " ".join(f"'{arg}'" for arg in extra)
            + " && exec postgres -D /var/lib/postgresql/data"
        )
        _docker(
            "run", "-d", "--name", container, "--network", self.network,
            "-v", f"{volume}:/var/lib/postgresql/data",
            "-v", f"{self.work}:/var/lib/hdt-backup:ro",
            *_env_args(self.backup_env()), BACKUP_IMAGE, "bash", "-c", script,
        )  # fmt: skip
        self.containers.append(container)
        self.wait_ready(container)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if self.sql(container, "SELECT pg_is_in_recovery()", check=False) == "f":
                return container
            time.sleep(1)
        raise AssertionError(
            f"{container} did not finish recovery:\n{_docker('logs', container).stderr[-3000:]}"
        )

    def cleanup(self) -> None:
        if self.containers:
            _docker("rm", "-f", *self.containers, check=False)
        for volume in self.volumes:
            _docker("volume", "rm", "-f", volume, check=False)
        _docker("network", "rm", self.network, check=False)


@pytest.fixture
def cluster(images: None) -> Iterator[Cluster]:
    instance = Cluster()
    try:
        instance.start()
        yield instance
    finally:
        instance.cleanup()


def _wait_archived(cluster: Cluster, segment: str, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        archived = cluster.query(
            cluster.source, "SELECT coalesce(last_archived_wal, '') FROM pg_stat_archiver"
        )
        if archived and archived >= segment:
            return
        time.sleep(0.5)
    raise AssertionError(f"segment {segment} was not archived")


def test_base_backup_and_wal_restore_to_end_and_to_point_in_time(cluster: Cluster) -> None:
    keys = "/var/lib/hdt-backup/keys"
    identity = cluster.tool(
        "bash",
        "-c",
        f"mkdir -p {keys} && age-keygen -o {keys}/identity.txt 2>/dev/null && cat {keys}/identity.txt",
    )
    cluster.recipient = _public_key(identity.stdout)
    # A new cluster gets the operations roles at init (02-ops-roles.sh), with the replication line.
    roles = (
        "SELECT rolname, rolreplication, rolbypassrls FROM pg_roles "
        "WHERE rolname LIKE 'hdt\\_%' AND (rolreplication OR rolbypassrls)"
    )
    assert cluster.query(cluster.source, roles).split() == ["hdt_backup|t|t"]

    cluster.query(cluster.source, "CREATE TABLE ledger (id int PRIMARY KEY, batch int NOT NULL)")
    cluster.query(cluster.source, "INSERT INTO ledger SELECT g, 1 FROM generate_series(1, 100) g")
    cluster.tool("hdt-backup", "base")

    cluster.query(cluster.source, "INSERT INTO ledger SELECT g, 2 FROM generate_series(101, 200) g")
    point_in_time = cluster.query(cluster.source, "SELECT clock_timestamp()")
    time.sleep(1.5)
    cluster.query(cluster.source, "INSERT INTO ledger SELECT g, 3 FROM generate_series(201, 300) g")
    segment = cluster.query(cluster.source, "SELECT pg_walfile_name(pg_switch_wal())")
    _wait_archived(cluster, segment)
    cluster.tool("hdt-backup", "wal")
    pending = cluster.tool("bash", "-c", "find /var/lib/postgresql/wal_archive -type f | wc -l")
    assert pending.stdout.strip() == "0", "shipped segments must leave the local archive"

    batches = "SELECT batch, count(*) FROM ledger GROUP BY batch ORDER BY batch"
    full = cluster.restore("full")
    assert cluster.query(full, batches).split() == ["1|100", "2|100", "3|100"]

    pitr = cluster.restore("pitr", "--target-time", point_in_time)
    assert cluster.query(pitr, batches).split() == ["1|100", "2|100"]

    # Review N10: a restore drill is recorded only with the ids of sets uploaded from this host.
    cluster.tool("hdt-backup", "logical")
    lake = "/var/lib/hdt-backup/lake"
    cluster.tool("bash", "-c", f"mkdir -p {lake} && HDT_BACKUP_LAKE_DIR={lake} exec hdt-backup lake")
    ids = [cluster.tool("hdt-restore", "latest", kind).stdout.strip() for kind in ("base", "logical", "lake")]
    made_up = _backup_id(datetime.now(UTC) - timedelta(days=1))
    metrics = "/var/lib/hdt-backup/metrics/hdt_backup.prom"
    drill = 'hdt_backup_last_success_timestamp_seconds{kind="restore_test"}'
    for position in range(3):
        claimed = [*ids]
        claimed[position] = made_up
        refused = cluster.tool("hdt-backup", "restore-tested", *claimed, check=False)
        assert refused.returncode != 0
        assert "was not uploaded from this host" in refused.stderr
    assert drill not in cluster.tool("cat", metrics).stdout
    cluster.tool("hdt-backup", "restore-tested", *ids)
    assert drill in cluster.tool("cat", metrics).stdout
