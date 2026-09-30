"""Create (or reset) a console admin on the VPS. Run inside the config-api container:

    python scripts/admin_create.py <username> [--reset]

Reads HDT_PG_DSN (role hdt_configapi) and HDT_SESSION_SECRET(_FILE) like config-api. The password is
typed twice at a hidden prompt (never an argument). A new TOTP seed is generated and its otpauth URI is
printed to this terminal only (never sent over the network); the admin is stored only after a current
code from the authenticator app is confirmed, so a mistyped enrollment cannot lock the account out.
"""

from __future__ import annotations

import argparse
import getpass
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from argon2 import PasswordHasher  # noqa: E402

from hdt.configapi.auth import MIN_PASSWORD_LENGTH, AdminProvisionError, provision_admin  # noqa: E402
from hdt.configapi.context import MIN_SESSION_SECRET_BYTES  # noqa: E402
from hdt.configapi.totp import new_totp_secret, provisioning_uri, verify_totp  # noqa: E402
from hdt.core.clock import utcnow  # noqa: E402
from hdt.core.config import require_env_value  # noqa: E402
from hdt.db.session import make_engine, make_session_factory, transaction  # noqa: E402

CODE_ATTEMPTS = 3


def read_password() -> str:
    first = getpass.getpass(f"Password (min {MIN_PASSWORD_LENGTH} chars): ")
    if len(first) < MIN_PASSWORD_LENGTH:
        raise SystemExit(f"password must be at least {MIN_PASSWORD_LENGTH} characters")
    if getpass.getpass("Repeat password: ") != first:
        raise SystemExit("passwords do not match")
    return first


def confirm_enrollment(secret: str, username: str) -> int:
    print("\nAdd this account to your authenticator app (shown on this terminal only):")
    print(f"  {provisioning_uri(secret, username)}")
    print(f"  manual entry key: {secret}\n")
    for _ in range(CODE_ATTEMPTS):
        code = input("Current 6-digit code from the app: ").strip()
        counter = verify_totp(secret, code, now=utcnow(), last_counter=None)
        if counter is not None:
            return counter
        print("code not accepted (check the device clock and try again)")
    raise SystemExit("TOTP enrollment not confirmed; nothing was stored")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("username")
    parser.add_argument("--reset", action="store_true", help="replace password and TOTP of an existing admin")
    args = parser.parse_args(argv)
    session_secret = require_env_value("HDT_SESSION_SECRET").encode("utf-8")
    if len(session_secret) < MIN_SESSION_SECRET_BYTES:
        raise SystemExit(f"HDT_SESSION_SECRET must be at least {MIN_SESSION_SECRET_BYTES} bytes")
    password = read_password()
    secret = new_totp_secret()
    counter = confirm_enrollment(secret, args.username)
    engine = make_engine()
    try:
        with transaction(make_session_factory(engine)) as session:
            provision_admin(
                session,
                PasswordHasher(),
                session_secret,
                username=args.username,
                password=password,
                totp_secret=secret,
                enrolled_counter=counter,
                reset=args.reset,
            )
    except AdminProvisionError as exc:
        raise SystemExit(str(exc)) from None
    finally:
        engine.dispose()
    print(
        f"admin '{args.username}' reset; its sessions were revoked"
        if args.reset
        else f"admin '{args.username}' created"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
