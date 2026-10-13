# goback

Deduplicating, encrypted backups for one user and one store. An agent backs
directories up into a store: a local directory, a bucket on Google Cloud
Storage or anything S3-compatible, or a goback server that gathers several
agents into one store.

## How it stores data

- Objects are content-addressed by SHA-256, like git's: a commit points at a
  tree, a tree at files and subtrees, a file at the chunks it is made of.
- Files are cut into chunks with FastCDC, so an edit stores only the chunks it
  changed, and the same chunk is stored once across files, commits and sets.
- Objects are packed into archives, each with an index. A local SQLite or
  Postgres index answers queries, and can always be rebuilt from the archives.
- A backup only uploads what the store doesn't already hold, and a restore
  only downloads what the target directory doesn't already hold.

## Encryption

- **Store key (client side).** With `--store-key`, names and contents are
  encrypted before they leave the agent; a server never sees them. The key can
  be escrowed in the store under a passphrase (`goback key escrow`) and opened
  with `--passphrase`. Without the key or the passphrase, nothing can be
  restored.
- **At-rest key (server side).** With `--at-rest-key`, archives are sealed
  where they are stored, independently of the store key.

## Quick start

```sh
go build ./cmd/goback

goback key new my-store                 # writes store.key; keep a copy elsewhere

export GOBACK_STORAGE=/backups/store GOBACK_INDEX=/backups/index

goback --store-key store.key --set home commit new ~/

goback --store-key store.key --set home commit list

goback --store-key store.key --set home \
  commit restore ~/restored             # the latest commit; add an age like 24h for an older one
```

`--storage` also takes `gcs://bucket`, `s3://bucket?endpoint=…`, or
`goback://<secret>@host:port` for a goback server. `--storage` and `--index`
(or `GOBACK_STORAGE` and `GOBACK_INDEX`) have no defaults; a command that
needs one says so. `--cache-dir` (or
`GOBACK_CACHE_DIR`) names a per-machine directory for every cache: a backup
skips unchanged files with it and a restore downloads less.

## Commands

| Command | Does |
| --- | --- |
| `agent` | run a machine's backups on their schedules from one config file |
| `commit` | back a directory up, list commits, restore, delete and undelete |
| `file` | list and restore single files and their versions |
| `set` | delete and undelete a set, show or change its retention |
| `pin` | keep a commit regardless of retention |
| `key` | make, escrow and recover store keys; make at-rest keys |
| `gc` | mark what live commits and pins reach, and rewrite archives that are mostly dead |
| `maintain` | run the housekeeping that is due: retire, compact, build presence filters, and collect garbage weekly |
| `scrub`, `repair` | rehash every stored object, and rewrite the archives holding corrupt ones |
| `fix` | rebuild the index from the store |
| `index` | index commits the store holds without a full rebuild |
| `object` | read stored objects by ref |
| `postgres` | back up a Postgres cluster with point-in-time recovery |
| `mysql` | back up a MySQL server with optional point-in-time recovery |
| `server` | serve one store to many agents over gRPC, with TLS and an operator API |

`goback <command> --help` describes each one.

## Retention

A set keeps the last commit of each calendar hour, day, week or month, by
brackets counted back from now, newest first:

```sh
goback set retention home \
  --keep hourly=336 --keep daily=60 --keep weekly=12 --keep monthly
```

A run that creates its set can give it a first policy with
`--initial-retention "keep-last=1 keep=daily=60 keep=weekly=4"`, written as
`goback set retention` prints one; a set that exists keeps its own.

Commits the policy lets go are tombstoned by the next retirement; a commit
deleted by hand waits in the trash first, where it can be taken back.
Once no live commit or pin reaches an object, garbage collection drops it.

Without a server, run `goback maintain` from cron, say hourly: it retires and
compacts every time and collects garbage once the last collection is a week old.

## A server for several agents

```sh
goback --storage gcs://my-bucket --index /var/lib/goback/index \
  server --address :6060 --secret-file secret --tls-cert cert.pem --tls-key key.pem
```

The server listens on 127.0.0.1:6060 unless `--address` says otherwise.
Every agent presents the shared secret and its own `--agent-id`. The server
runs maintenance itself: it sweeps, compacts, retires and collects on its own
intervals.

## The agent

`goback agent run --config goback.yaml` runs a machine's backups on their
schedules: directories, Postgres clusters and MySQL servers, listed once with
the store they go to. `goback agent check` validates the file, opens the store
and lists what each database lacks; `goback agent status --check` is a health
check. [contrib/systemd](contrib/systemd/goback-agent.service) runs it as a
service.

## Postgres

`goback postgres` takes base backups and streams the WAL through a replication
slot into two sets, and restores a cluster to any moment in between. [contrib/postgres](contrib/postgres/README.md)
runs it with Docker Compose.

## MySQL

`goback mysql` streams `mysqldump` into one set and, optionally, receives the
binary log as a replica into another, and restores a server to a dump or to
any second after it. [contrib/mysql](contrib/mysql/README.md) runs it with Docker Compose.

## As a Go library

Package `github.com/gobackio/goback` opens a store from the same settings the
CLI takes; the CLI is built on it. The API is not stable yet and may change
between minor versions.

```go
repo, err := goback.Open(ctx, goback.Settings{
	Storage:  "/backups/store",
	Index:    "/backups/index",
	StoreKey: "store.key",
})
if err != nil {
	return err
}
defer repo.Close()

env, err := repo.Env(ctx, goback.Caches{})
if err != nil {
	return err
}
defer env.Close()

result, err := job.Path(ctx, env, job.PathSettings{Set: "home", Dir: "/home/me"})
```

`backup/job` runs each kind of backup once, `agent` runs them on schedules,
`backup.Restorer` restores, `storage/maintenance` runs the housekeeping, and
`Repo.Serve` serves a store as `goback server` does.

## Development

```sh
go build ./...
go test ./...
```

Some tests start Postgres, MySQL and MinIO in Docker through testcontainers.

## License

Apache License 2.0; see [LICENSE](LICENSE).
