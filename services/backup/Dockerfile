# backup: client-side encrypted Postgres (base backup + WAL + logical dump) and lake backups (phase 10).
# Command (docker-compose.yml): hdt-backup daemon. Scripts: deploy/backup/{backup,restore,lib}.sh.
# Same Postgres image as the database, so pg_basebackup / pg_dump / pg_restore match the server version;
# adds age (encryption) and curl (S3 SigV4 uploads). Runs as `postgres` (uid 999, like the server) so it
# can delete the WAL segments it has shipped from the shared archive volume. The same image restores
# (hdt-restore) on the operator side.
FROM postgres:16.13@sha256:71e27bf60b70bded003791b5573f8b808365613f341df20ffcf0c1ed7bc13ddf
RUN apt-get update \
    && apt-get install -y --no-install-recommends age curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o postgres -g postgres -m 0750 /var/lib/postgresql/wal_archive \
    && install -d -o postgres -g postgres -m 0700 /var/lib/hdt-backup \
    && install -d -o postgres -g postgres -m 0755 /var/lib/hdt-metrics
COPY deploy/backup/lib.sh deploy/backup/backup.sh deploy/backup/restore.sh /usr/local/lib/hdt-backup/
RUN chmod 0755 /usr/local/lib/hdt-backup/*.sh \
    && ln -s /usr/local/lib/hdt-backup/backup.sh /usr/local/bin/hdt-backup \
    && ln -s /usr/local/lib/hdt-backup/restore.sh /usr/local/bin/hdt-restore \
    && age --version && curl --version | head -n 1
LABEL org.opencontainers.image.title="hdt-backup"
USER postgres
ENTRYPOINT []
CMD ["hdt-backup", "daemon"]
