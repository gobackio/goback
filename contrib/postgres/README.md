# Postgres backups with docker compose

`compose.yaml` backs up a Postgres container into a GCS bucket:

- Postgres archives each finished WAL file into a spool with `goback postgres archive`.
- [Ofelia](https://github.com/mcuadros/ofelia) runs inside the Postgres container:
  - `goback postgres wal` every minute, which commits the spool to the `db-wal` set.
  - `goback postgres base` daily, which takes a base backup over a replication connection into the `db-base` set; before Postgres 15, through `pg_basebackup`.
- goback reaches the Postgres container through the `goback-bin` volume. A one-shot service copies the static binary there on every `docker compose up`, so `docker compose build` followed by `docker compose up -d` upgrades it.

`compose.replication.yaml` backs up the same cluster in replication mode:

- Ofelia runs `goback postgres base` daily inside the Postgres container, as above.
- The `goback-receive` service runs `goback postgres receive`, which streams the WAL through a physical replication slot into a spool as it is written, and commits it to the `db-wal` set every minute. It needs no `archive_mode` and no access to the data directory, only the Postgres port, so it runs from the goback image and works for a cluster on another host too.

## Setup

1. Replace `BUCKET` in `compose.yaml` with your bucket.
2. Put a service account key with access to the bucket in `gcp-credentials.json`.
3. Create the store key with `goback key new <store name>` (it writes `store.key`) and move it to `goback.key`. Keep a copy elsewhere: without it, nothing can be restored.
4. Put the Postgres password in `postgres-password`.
5. Run `docker compose up -d`.

Details:

- **Base backups only.** Point-in-time recovery is optional: without `archive_mode` and the `postgres wal` job, the daily base backups restore on their own.
- **Point-in-time recovery window.** `postgres wal` keeps WAL back to the oldest base backup within `--window` (14 days by default). Base backups are kept by the `db-base` set's retention.
- **WAL retention during a base backup.** A base backup carries the WAL it needs. Postgres must keep that WAL until the backup ends, so `wal_keep_size` has to cover the WAL written while one runs.
- **Tablespaces.** Clusters with extra tablespaces aren't supported: a base backup takes the data directory's tablespace only.
- **`init: true` is required.** The jobs run inside the Postgres container. Without an init process, an orphaned child is reparented to the postmaster, and when that child dies the postmaster restarts the cluster.
- **Ofelia's scope.** Ofelia schedules every labeled container on the Docker host. Give the jobs host-unique names when you back up several clusters.

## Replication mode

Setup differs from the steps above:

1. Replace `BUCKET` in `compose.replication.yaml`.
2. Create a role for the receiver:

   ```sql
   CREATE ROLE goback LOGIN REPLICATION PASSWORD '…';
   ```

3. Let it connect for replication from the receiver's container, and reload:

   ```sh
   docker compose -f compose.replication.yaml exec -u postgres postgres sh -c '
     echo "host replication goback all scram-sha-256" >> "$PGDATA/pg_hba.conf" &&
     psql -c "SELECT pg_reload_conf()"'
   ```

   `all` in the database column doesn't match replication connections; the image's own `host all all all` line covers the receiver's second, normal connection to `postgres`.
4. Put its password in `goback.pgpass`:

   ```text
   postgres:5432:*:goback:…
   ```

Then run `docker compose -f compose.replication.yaml up -d`.

Details:

- **`max_slot_wal_keep_size`.** The slot keeps every WAL file the receiver hasn't committed, so `pg_wal` grows while the receiver or the bucket is down. The setting caps it; past the cap Postgres drops the slot's WAL and the receiver bridges the loss from its spool or reports a gap. The receiver refuses to stream without a cap unless given `--allow-uncapped-slot`. Pick what the database's disk can spare.
- **The first base backup.** Point-in-time recovery reaches back to the first base backup, so take one by hand after the first `up` rather than waiting for the daily job. The receiver keeps WAL back to the oldest base backup within `--window`.
- **The open segment.** Each commit holds the segment being written as `<segment>.partial`, so a restore reaches the last commit, not the last segment switch. No `archive_timeout` is needed.
- **Commits.** Every minute (`--commit-every`), sooner past 256 MiB (`--commit-bytes`), and none while the cluster writes nothing. The slot lets Postgres remove WAL only once a commit holding it is stored.
- **Stopping.** On SIGTERM the receiver makes a last commit within `--shutdown-grace` (30 s). `stop_grace_period` is longer, so Docker doesn't kill it first.
- **Retiring the receiver.** The slot keeps holding WAL after the receiver is gone. Drop it with `goback --set db-wal postgres receive --drop-slot`.
- **Spool.** The `goback-receive` volume holds what waits for its commit. Size it for the WAL written while the bucket is unreachable.
- **`goback agent check`** lists what the cluster lacks for streaming: the version, the `REPLICATION` attribute, `max_slot_wal_keep_size`, a free slot and the `pg_hba.conf` lines.

### Database settings

| setting | archive mode (`compose.yaml`) | replication mode (`compose.replication.yaml`) |
|---|---|---|
| `wal_level` | `replica` (the default since 10) | `replica` (the default since 10) |
| `archive_mode`, `archive_command`, `archive_timeout` | `on`, `goback postgres archive %p %f`, `60`. `archive_mode` needs a restart | not needed |
| `max_wal_senders` | one free for the base backup | one free for the receiver and one for a base backup. Default 10; restart to change |
| `max_replication_slots` | — | one free. Default 10; restart to change |
| `max_slot_wal_keep_size` | — | **set it**, see above. Default `-1`, unlimited |
| `wal_keep_size` | at least the WAL written during a base backup | not needed: the slot holds the base's WAL |
| role | `REPLICATION`, for the base backup | `CREATE ROLE goback LOGIN REPLICATION` |
| `pg_hba.conf` | local socket | a `host replication` line for the receiver's address, and a line for a normal connection to `postgres` |
| credentials | libpq's `.pgpass` / `PGPASSFILE` | the same |

Postgres 14 to 18 are supported.

## Restore

`goback postgres restore` writes a base backup into an empty data directory. It sets the directory up to recover from the WAL set when Postgres starts on it:

- **Choosing the base.** It picks the newest base backup that ended before `--at` (or the newest overall, without `--at`) and that the WAL set continues.
- **Fetching WAL.** It writes `restore_command` into `postgresql.auto.conf`. Postgres then fetches each WAL file it needs with `goback postgres wal-get`, using the global flags given to `restore`, so they must be valid where Postgres runs.
- **Without `--wal-set`.** It restores the newest base backup alone, and Postgres recovers to the end of that backup. `--at` needs `--wal-set`.
- **Finishing.** It writes `recovery.signal`. Postgres promotes once recovery reaches `--at`, or the end of the WAL set.
- **Finding the sets.** It finds them through the index, or through the set heads in the bucket when the index has none, as on a new host.

### Restoring next to a running cluster

To look at an earlier state without touching the live cluster, restore into any empty directory, then start a second Postgres on it on another port:

```sh
docker compose exec -u postgres postgres sh -c '
  /opt/goback/goback --storage "gcs://BUCKET" \
    --index /var/lib/goback/index --store-key /run/secrets/goback-key \
    postgres restore --base-set db-base --wal-set db-wal --at 2026-10-02T14:30:00Z /var/lib/goback/restored &&
  pg_ctl -D /var/lib/goback/restored -o "-p 5433 -c archive_mode=off" -l /var/lib/goback/restored.log start'
```

`archive_mode=off` keeps the copy from archiving into the live cluster's spool.
