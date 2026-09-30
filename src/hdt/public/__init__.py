"""Public performance data path (phase 10).

`publisher` runs inside the trading network and is the only writer; `store` is the S3 client used by both
the publisher (write credentials) and the public dashboard (read-only credentials); `allowlist` holds the
per-page field allowlists and the publication rules; `snapshot_schema.json` is the JSON schema every
snapshot must pass before it is written.

This package must stay importable without the database, Redis, vault or config-api code: the public
dashboard imports `hdt.public.store` and `hdt.public.allowlist` only.
"""
