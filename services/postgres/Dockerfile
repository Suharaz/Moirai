# postgres: trading database (same pinned image as docker-compose.dev.yml and CI).
# Adds only the WAL archive directory owned by `postgres`, so the named volume `pg_wal_archive` mounted
# there is created with the right owner. archive_command (docker-compose.yml) copies each completed WAL
# segment into it; the `backup` service encrypts and ships the segments off-site, then deletes them.
FROM postgres:16.13@sha256:71e27bf60b70bded003791b5573f8b808365613f341df20ffcf0c1ed7bc13ddf
RUN install -d -o postgres -g postgres -m 0750 /var/lib/postgresql/wal_archive
LABEL org.opencontainers.image.title="hdt-postgres"
