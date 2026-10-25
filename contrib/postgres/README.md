# Postgres backups with docker compose

`compose.yaml` backs up a Postgres container into a GCS bucket:

- The `agent` service runs `goback agent run` with `goback/goback.yaml`. Its one backup, `db`, takes a base backup every day at 03:00 into the `db/base` set, over a replication connection; before Postgres 15, through `pg_basebackup`, which the goback image lacks.
- The same agent streams the WAL through a physical replication slot into a spool as it is written, and commits it to the `db/wal` set every minute. It needs no access to the data directory, only the Postgres port, so it works for a cluster on another host too.
- Restores run inside the Postgres container, which reaches goback through the `goback-bin` volume. A one-shot service copies the static binary there on every `docker compose up`, so `docker compose build` followed by `docker compose up -d` upgrades it.

## Setup

1. Replace `BUCKET` in `goback/goback.yaml` with your bucket.
2. Put a service account key with access to the bucket in `gcp-credentials.json`.
3. Create the store key with `goback key new <store name>` (it writes `store.key`) and move it to `goback.key`. Keep a copy elsewhere: without it, nothing can be restored.
4. Put the Postgres password in `postgres-password`.
5. Pick a password for the agent's role and put it in `goback.pgpass`:

   ```text
   postgres:5432:*:goback:…
   ```

6. Start the cluster with `docker compose up -d postgres`, and create the role:

   ```sql
   CREATE ROLE goback LOGIN REPLICATION PASSWORD '…';
   ```

7. Let it connect for replication from the agent's container, and reload:

   ```sh
   docker compose exec -u postgres postgres sh -c '
     echo "host replication goback all scram-sha-256" >> "$PGDATA/pg_hba.conf" &&
     psql -c "SELECT pg_reload_conf()"'
   ```

   `all` in the database column doesn't match replication connections; the image's own `host all all all` line covers the agent's normal connection to `postgres`.
8. Run `docker compose up -d`, and check with `docker compose exec agent goback agent check`, which lists what the cluster lacks: the version, the `REPLICATION` attribute, `max_slot_wal_keep_size`, a free slot and the `pg_hba.conf` lines.

Details:

- **Base backups only.** Point-in-time recovery is optional: with `pitr: false` under `postgres:`, the agent takes the daily base backups alone, and they restore on their own.
- **The first base backup.** The agent takes one when it starts and finds none in `db/base`; point-in-time recovery reaches back to it.
- **Point-in-time recovery window.** The agent keeps WAL back to the oldest base backup within `window` (14 days by default). Base backups are kept by the `db/base` set's retention.
- **Retention of the two sets.** The agent creates `db/base` keeping a base backup a day for 60 days, then one a week for 4 weeks, and `db/wal` keeping its newest 60 commits: each `db/wal` commit holds the whole window, so the newest suffice. `retention:` on the backup, as in `retention: "keep-last=1 keep=daily=30"`, replaces the policy `db/base` starts with. Both apply only when the agent creates the set; after that, `goback set retention` changes a set's policy. `db/base` must keep every base backup within the window, or recovery reaches back less far.
- **`max_slot_wal_keep_size`.** The slot keeps every WAL file the agent hasn't committed, so `pg_wal` grows while the agent or the bucket is down. The setting caps it; past the cap Postgres drops the slot's WAL and the agent bridges the loss from its spool or reports a gap. The agent refuses to stream without a cap unless given `allow-uncapped-slot: true`. Pick what the database's disk can spare.
- **The open segment.** Each commit holds the segment being written as `<segment>.partial`, so a restore reaches the last commit, not the last segment switch.
- **Commits.** Every minute (`commit-every`), sooner past 256 MiB (`commit-bytes`), and none while the cluster writes nothing. The slot lets Postgres remove WAL only once a commit holding it is stored.
- **Stopping.** On SIGTERM the agent makes a last commit within its shutdown grace (30 s). `stop_grace_period` is longer, so Docker doesn't kill it first.
- **Retiring the backup.** The slot, `goback_db_wal`, keeps holding WAL after the agent is gone. Drop it with `goback --set db/wal postgres receive --drop-slot`.
- **Spool.** The `goback-agent` volume holds what waits for its commit, with the agent's caches and status. Size it for the WAL written while the bucket is unreachable.
- **WAL retention during a base backup.** A base backup carries the WAL it needs, and Postgres must keep that WAL until the backup ends. The agent's slot does; without it, `wal_keep_size` has to cover the WAL written while a base backup runs.
- **Tablespaces.** Clusters with extra tablespaces aren't supported: a base backup takes the data directory's tablespace only.
- **Status.** `docker compose exec agent goback agent status` lists each job's last run; the container's health check fails when the agent hangs or a job goes stale.

## Database settings

| setting | base backups only | base backups + WAL |
|---|---|---|
| `wal_level` | `replica` (the default since 10) | `replica` (the default since 10) |
| `max_wal_senders` | one free for the base backup | one free for the WAL stream and one for a base backup. Default 10; restart to change |
| `max_replication_slots` | — | one free. Default 10; restart to change |
| `max_slot_wal_keep_size` | — | **set it**, see above. Default `-1`, unlimited |
| `wal_keep_size` | at least the WAL written during a base backup | not needed: the slot holds the base's WAL |
| role | `REPLICATION`, for the base backup | `CREATE ROLE goback LOGIN REPLICATION` |
| `pg_hba.conf` | a `host replication` line for the agent's address | a `host replication` line for the agent's address, and a line for a normal connection to `postgres` |
| credentials | libpq's `.pgpass` / `PGPASSFILE` | the same |

The WAL streams from Postgres 14 to 18. Base backups alone work from 9.6.

## Restore

`goback postgres restore` writes a base backup into an empty data directory. It sets the directory up to recover from the WAL set when Postgres starts on it:

- **Choosing the base.** It picks the newest base backup that ended before `--at` (or the newest overall, without `--at`) and that the WAL set continues.
- **Fetching WAL.** It writes `restore_command` into `postgresql.auto.conf`. Postgres then fetches each WAL file it needs with `goback postgres wal-get`, using the global flags given to `restore`, so they must be valid where Postgres runs. Flags that have an environment variable, such as `--storage` and `--passphrase`, go into `<data directory>.goback.env` with mode 0600 instead of `postgresql.auto.conf`, and recovery deletes that file when it ends.
- **Copying WAL instead.** With `--copy-wal`, it copies the WAL recovery reads into `pg_wal`, so Postgres starts without goback or the store. From Postgres 13, it copies up to one segment past `--at`; before 13, to the end of the WAL set.
- **Without `--wal-set`.** It restores the newest base backup alone, and Postgres recovers to the end of that backup. `--at` needs `--wal-set`.
- **Finishing.** It writes `recovery.signal`. Postgres promotes once recovery reaches `--at`, or the end of the WAL set.
- **Finding the sets.** It finds them through the index, or through the set heads in the bucket when the index has none, as on a new host.

### Restoring next to a running cluster

To look at an earlier state without touching the live cluster, restore into any empty directory, then start a second Postgres on it on another port:

```sh
docker compose exec -u postgres postgres sh -c '
  /opt/goback/goback --storage "gcs://BUCKET" \
    --index /var/lib/goback/index --store-key /run/secrets/goback-key \
    postgres restore --base-set db/base --wal-set db/wal --at 2026-10-02T14:30:00Z /var/lib/goback/restored &&
  pg_ctl -D /var/lib/goback/restored -o "-p 5433" -l /var/lib/goback/restored.log start'
```
