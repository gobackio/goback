# MySQL backups with docker compose

`compose.yaml` backs up a MySQL 8.0 container into a GCS bucket:

- The `agent` service runs `goback agent run` with `goback.yaml`. Its one backup, `db`, dumps the server every day at 03:00 into the `db/dump` set, through `mysqldump`, which is why the agent runs from the `mysql:8.0` image.
- The same agent connects to the server as a replica, receives its binlogs as they are written into a spool, and commits them to the `db/binlog` set every minute. It needs no access to the data directory, only the MySQL port, so it works for a server on another host too.
- goback reaches the agent's container through the `goback-bin` volume. A one-shot service copies the static binary there on every `docker compose up`, so `docker compose build` followed by `docker compose up -d` upgrades it.
- The dump and the receiver read the password from the option file `defaults-extra-file` names. The receiver keeps it in memory only.

## Setup

1. Replace `BUCKET` in `goback.yaml` with your bucket.
2. Put a service account key with access to the bucket in `gcp-credentials.json`.
3. Create the store key with `goback key new <store name>` (it writes `store.key`) and move it to `goback.key`. Keep a copy elsewhere: without it, nothing can be restored.
4. Put the root password in `mysql-root-password`.
5. Pick a password for goback's user and put it in `mysql-client.cnf`, readable by the agent's `mysql` user and writable by no one else, since `mysql` ignores a world-writable option file:

   ```ini
   [client]
   host = mysql
   user = goback
   password = …
   ```

6. Start the server with `docker compose up -d mysql`, and create the user with the privileges below:

   ```sql
   CREATE USER 'goback'@'%' IDENTIFIED BY '…';
   GRANT SELECT, SHOW VIEW, TRIGGER, EVENT, LOCK TABLES, PROCESS, RELOAD,
     REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'goback'@'%';
   ```

7. Run `docker compose up -d`, and check with `docker compose exec agent /opt/goback/goback agent check`. The agent dumps at once when `db/dump` holds no dump; the receiver starts at the binlog of the newest dump.

Details:

- **Dumps only.** Point-in-time recovery is optional: with `pitr: false` under `mysql:`, the agent takes the daily dumps alone, and they restore on their own. With `log_bin` on, a dump still records its binlog position, so binlogs received later can start from it.
- **Point-in-time recovery window.** The agent keeps binlogs back to the oldest dump within `window` (14 days by default). Dumps are kept by the `db/dump` set's retention.
- **Retention of the two sets.** Each `db/binlog` commit holds the whole window, so the set needs only its newest few: `goback set retention db/binlog --keep-last 60`. Kept by age, it holds binlogs from long before the window. `db/dump` must keep every dump within the window, or recovery reaches back less far: `goback set retention db/dump --keep-within 360h`, the window and a day.
- **Non-transactional tables.** A dump is consistent through one transaction, which covers InnoDB only. When user tables of another engine, such as MyISAM, exist, the dump locks all tables until it ends and logs which tables caused it. Writes wait for that long.
- **The open binlog.** Each commit holds the binlog being written as `<binlog>.partial`, so a restore reaches the last commit, not the last rotation.
- **Commits.** Every minute (`commit-every`), sooner past 256 MiB (`commit-bytes`), and none while the server writes nothing.
- **Server id.** The agent connects with a server id derived from the `db/binlog` set's name. When another replica uses it, receiving fails with an error; set `server-id`.
- **Stopping.** On SIGTERM the agent makes a last commit within its shutdown grace (30 s). `stop_grace_period` is longer, so Docker doesn't kill it first.
- **Gaps.** The server keeps binlogs for its expiry only. An agent that comes back after the server purged binlogs it hadn't received fails to receive until a newer dump exists.
- **Resets.** After `RESET MASTER`, or a server restored in place, the agent goes on in a new chain of the same set. A restore replays only within the chain of the dump it picks. A server of another identity fails.
- **Spool.** The `goback-agent` volume holds what waits for its commit, with the agent's caches and status. Size it for the binlogs written while the bucket is unreachable.
- **Status.** `docker compose exec agent /opt/goback/goback agent status` lists each job's last run; the container's health check fails when the agent hangs or a job goes stale.
- **`goback agent check`** lists what the server lacks for receiving: `log_bin`, `server_id`, the privileges and a free server id.

## Database settings

What to configure. Versions differ where noted; MySQL 5.5 to 8.4 are supported.

| setting | dumps only | dumps + binlogs |
|---|---|---|
| `log_bin` | not needed | **on**. Needs a `my.cnf` change and a restart on 5.5–5.7. On by default from MySQL 8.0 |
| `server_id` | — | non-zero, and not goback's. 5.5 logs with 0. 8.0 defaults to 1 |
| `binlog_format` | — | `ROW` advised, `MIXED` acceptable. 5.5 defaults to `STATEMENT`, 8.0 to `ROW`. `SET GLOBAL binlog_format = 'ROW'` takes effect for new sessions; also set it in `my.cnf` to survive a restart |
| binlog expiry | — | long enough to cover goback being down: 3 days or more, since nothing holds binlogs for goback. `expire_logs_days` on 5.5–5.7; `binlog_expire_logs_seconds` from MySQL 8.0 (default 30 days); 8.4 removed `expire_logs_days` |
| `sync_binlog` | — | `1` advised, so a host crash can't drop transactions goback already received. 5.5 defaults to 0, MySQL from 5.7.7 to 1 |
| `binlog_encryption` (8.0.14+) | — | any: the server decrypts the stream |
| privileges | `SELECT`, `SHOW VIEW`, `TRIGGER`, `EVENT`, `LOCK TABLES`; `PROCESS` on MySQL 8.0.21+ | the dump's plus `REPLICATION SLAVE` (the binlog dump) and `REPLICATION CLIENT`. `RELOAD` for the dump's position |
| network | the MySQL port from wherever the agent runs | the same |

With `log_bin` on, as by default on 8.0, a dump records its position and needs `RELOAD` and `REPLICATION CLIENT` even without binlogs received. With `pitr: false`, `no-binlog-position: true` under `mysql:` leaves it out.

## Restore

`goback mysql restore` loads a dump into an empty running server, then replays the binlog set after it:

- **Choosing the dump.** It picks the newest dump that ended before `--at` (or the newest overall, without `--at`) and whose chain the binlog set continues.
- **Replaying.** It fetches the binlogs into `--work-dir` and replays them in one `mysqlbinlog | mysql`, from the dump's position up to `--at`, to the second. The set ends with the open binlog's `.partial`, which is replayed as the last binlog under its plain name.
- **Without `--binlog-set`.** It loads the dump alone. `--at` needs `--binlog-set`.
- **The target.** It refuses a server that holds databases of its own; `--force` loads anyway. Its `max_allowed_packet` must be at least the source's.
- **`mysqlbinlog`.** It must be at least the source server's version. The `mysql:8.0` image has none: restore with `--to <dir>`, which writes the dump and the binlogs into a directory and prints the two commands that apply them, and run those where `mysqlbinlog` is installed.
- **Finding the sets.** It finds them through the index, or through the set heads in the bucket when the index has none, as on a new host.

### Restoring next to a running server

To look at an earlier state without touching the live server, start a second MySQL and restore into it:

```sh
docker run -d --name mysql-restored -e MYSQL_ROOT_PASSWORD=… mysql:8.0
/opt/goback/goback --storage "gcs://BUCKET" \
  --index /var/lib/goback/index --store-key goback.key \
  mysql restore --defaults-extra-file restored.cnf \
  --dump-set db/dump --binlog-set db/binlog --at 2026-10-02T14:30:00Z
```

`restored.cnf` names the second server in its `[client]` section.
