# MySQL backups with docker compose

`compose.yaml` backs up a MySQL 8.0 container into a GCS bucket:

- [Ofelia](https://github.com/mcuadros/ofelia) runs inside the MySQL container:
  - `goback mysql dump` daily, which streams `mysqldump` into the `db-dump` set.
  - `goback mysql binlog` every minute, which rotates the binary log when it holds anything new and commits the closed binlogs to the `db-binlog` set.
- goback reaches the MySQL container through the `goback-bin` volume. A one-shot service copies the static binary there on every `docker compose up`, so `docker compose build` followed by `docker compose up -d` upgrades it.
- goback runs the container's own `mysql` and `mysqldump`, and reads the binlogs from the data directory. It never handles a password: the client programs read it from the option file `--defaults-extra-file` names, or from `~/.my.cnf`.

`compose.replication.yaml` backs up the same server in replication mode:

- Ofelia runs `goback mysql dump` daily inside the MySQL container, as above.
- The `goback-receive` service runs `goback mysql receive`, which connects to the server as a replica, receives its binlogs as they are written into a spool, and commits them to the `db-binlog` set every minute. It needs no access to the data directory, only the MySQL port, so it runs from the goback image and works for a server on another host too.

## Setup

1. Replace `BUCKET` in `compose.yaml` with your bucket.
2. Put a service account key with access to the bucket in `gcp-credentials.json`.
3. Create the store key with `goback key new <store name>` (it writes `store.key`) and move it to `goback.key`. Keep a copy elsewhere: without it, nothing can be restored.
4. Put the root password in `mysql-root-password`.
5. Create a user for goback with the privileges below, and put it in `mysql-client.cnf`, readable by the container's `mysql` user:

   ```ini
   [client]
   user = goback
   password = …
   ```

6. Run `docker compose up -d`.

Details:

- **Dumps only.** Point-in-time recovery is optional: without the `mysql binlog` job, the daily dumps restore on their own. With `log_bin` on, a dump still records its binlog position, so a binlog job added later can use it; `--no-binlog-position` leaves it out.
- **Point-in-time recovery window.** `mysql binlog` keeps binlogs back to the oldest dump within `--window` (14 days by default). Dumps are kept by the `db-dump` set's retention.
- **Non-transactional tables.** A dump is consistent through one transaction, which covers InnoDB only. When user tables of another engine, such as MyISAM, exist, the dump locks all tables until it ends and logs which tables caused it. Writes wait for that long.
- **Gaps.** When the server purges binlogs before goback read them, every `mysql binlog` run fails and commits nothing until a newer dump exists.
- **Resets.** After `RESET MASTER`, or a server restored in place, the binlogs go on in a new chain of the same set. A restore replays only within the chain of the dump it picks. A server of another identity fails.
- **`init: true` is required.** The jobs run inside the MySQL container; an init process reaps what they leave behind.
- **Ofelia's scope.** Ofelia schedules every labeled container on the Docker host. Give the jobs host-unique names when you back up several servers.

## Replication mode

Setup differs from the steps above in three places:

1. Replace `BUCKET` in `compose.replication.yaml`.
2. Create the user for any host, and grant it the replication privileges too:

   ```sql
   CREATE USER 'goback'@'%' IDENTIFIED BY '…';
   GRANT SELECT, SHOW VIEW, TRIGGER, EVENT, LOCK TABLES, PROCESS, RELOAD,
     REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'goback'@'%';
   ```

3. Name the server in `mysql-client.cnf`, which both the dump and the receiver read:

   ```ini
   [client]
   host = mysql
   user = goback
   password = …
   ```

Then run `docker compose -f compose.replication.yaml up -d`.

Details:

- **The first dump.** The receiver starts at the binlog of the newest dump in `db-dump`. Until one exists it exits, and Docker restarts it. Run the dump once by hand after the first `up`: `docker compose -f compose.replication.yaml exec -u mysql mysql /opt/goback/goback … mysql dump --binlog-set db-binlog`, with the global flags of the Ofelia job.
- **The open binlog.** Each commit holds the binlog being written as `<binlog>.partial`, so a restore reaches the last commit, not the last rotation.
- **Commits.** Every minute (`--commit-every`), sooner past 256 MiB (`--commit-bytes`), and none while the server writes nothing.
- **Credentials.** goback reads the password from the `[client]` group of `--defaults-extra-file` and `~/.my.cnf`, as the client programs do, and keeps it in memory only.
- **Server id.** The receiver connects with a server id derived from the set's name. When another replica uses it, the receiver exits with an error; pass `--server-id`.
- **Stopping.** On SIGTERM the receiver makes a last commit within `--shutdown-grace` (30 s). `stop_grace_period` is longer, so Docker doesn't kill it first.
- **Down for longer.** The server keeps binlogs for its expiry only. A receiver that comes back after the server purged binlogs it hadn't received fails until a newer dump exists, as `mysql binlog` does.
- **Resets.** After `RESET MASTER`, or a server restored in place, the receiver goes on in a new chain of the same set.
- **Spool.** The `goback-receive` volume holds what waits for its commit. Size it for the binlogs written while the bucket is unreachable.
- **`goback agent check`** lists what the server lacks for receiving: `log_bin`, `server_id`, the privileges and a free server id.

## Database settings

What to configure, per mode. Versions differ where noted.

| setting | dumps only | dumps + binlog files (`compose.yaml`) | dumps + replication (`compose.replication.yaml`) |
|---|---|---|---|
| `log_bin` | not needed | **on**. Needs a `my.cnf` change and a restart on 5.5–5.7. On by default from MySQL 8.0 | **on**, the same |
| `server_id` | — | non-zero. 5.5 logs with 0. 8.0 defaults to 1 | non-zero, and not goback's |
| `binlog_format` | — | `ROW` advised, `MIXED` acceptable. 5.5 defaults to `STATEMENT`, 8.0 to `ROW`. `SET GLOBAL binlog_format = 'ROW'` takes effect for new sessions; also set it in `my.cnf` to survive a restart | the same |
| binlog expiry | — | long enough to cover goback being down: 3 days or more. `expire_logs_days` on 5.5–5.7; `binlog_expire_logs_seconds` from MySQL 8.0 (default 30 days); 8.4 removed `expire_logs_days` | the same: nothing holds binlogs for goback |
| `sync_binlog` | — | `1` advised, so a host crash can't drop transactions goback already copied. 5.5 defaults to 0, MySQL from 5.7.7 to 1 | `1` advised |
| `binlog_encryption` (8.0.14+) | — | off: goback reads the files directly | any: the server decrypts the stream |
| privileges | `SELECT`, `SHOW VIEW`, `TRIGGER`, `EVENT`, `LOCK TABLES`; `PROCESS` on MySQL 8.0.21+ | the same plus `RELOAD` (global read lock, `FLUSH BINARY LOGS`) and `REPLICATION CLIENT` (`SHOW BINARY LOGS`, the current position) | the dump's plus `REPLICATION SLAVE` (the binlog dump) and `REPLICATION CLIENT`. `RELOAD` only for the dump's position |
| file access | — | read access to the binlog files and their directory: run as `mysql` or in its group | none |
| network | local or remote | on the host | the MySQL port from wherever the receiver runs |

With `log_bin` on, as by default on 8.0, a dump records its position and needs `RELOAD` and `REPLICATION CLIENT` even without the binlog job, unless it runs with `--no-binlog-position`.

The binlog job finds the binlogs in the directory of `@@log_bin_basename`, or in `@@datadir` on 5.5, which has no `@@log_bin_basename`. A 5.5 server that logs elsewhere, as Ubuntu's stock `my.cnf` does to `/var/log/mysql/`, needs `--binlog-dir`.

## Restore

`goback mysql restore` loads a dump into an empty running server, then replays the binlog set after it:

- **Choosing the dump.** It picks the newest dump that ended before `--at` (or the newest overall, without `--at`) and whose chain the binlog set continues.
- **Replaying.** It fetches the binlogs into `--work-dir` and replays them in one `mysqlbinlog | mysql`, from the dump's position up to `--at`, to the second. A set from the receiver ends with the open binlog's `.partial`, which is replayed as the last binlog under its plain name.
- **Without `--binlog-set`.** It loads the dump alone. `--at` needs `--binlog-set`.
- **The target.** It refuses a server that holds databases of its own; `--force` loads anyway. Its `max_allowed_packet` must be at least the source's.
- **`mysqlbinlog`.** It must be at least the source server's version. The `mysql:8.0` image has none: restore with `--to <dir>`, which writes the dump and the binlogs into a directory and prints the two commands that apply them, and run those where `mysqlbinlog` is installed.
- **Finding the sets.** It finds them through the index.

### Restoring next to a running server

To look at an earlier state without touching the live server, start a second MySQL and restore into it:

```sh
docker run -d --name mysql-restored -e MYSQL_ROOT_PASSWORD=… mysql:8.0
/opt/goback/goback --storage "gcs://BUCKET" \
  --index /var/lib/goback/index --store-key goback.key \
  mysql restore --defaults-extra-file restored.cnf \
  --dump-set db-dump --binlog-set db-binlog --at 2026-10-02T14:30:00Z
```

`restored.cnf` names the second server in its `[client]` section.
