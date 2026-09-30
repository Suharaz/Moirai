"""config-api: the private FastAPI service behind the admin console (phase 12).

Reachable only through Tailscale / SSH tunnel. Server-side sessions (argon2 password + mandatory TOTP),
CSRF on every mutating request, step-up TOTP for sensitive changes, hard ceilings on every direct call,
immutable config versions, write-only sealed secrets and an append-only audit log.
"""
