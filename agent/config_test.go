package agent

import (
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup/retention"

	"github.com/stretchr/testify/require"
)

func decode(t *testing.T, doc string) (*Config, error) {
	t.Helper()

	return Decode(strings.NewReader(doc))
}

func TestDecodeFillsInDefaultsAndDerivesSets(t *testing.T) {
	cfg, err := decode(t, `
store:
  storage: goback://store.goback.io?key-file=/run/secrets/api-key
  agent-id: db1
backups:
  - name: db
    postgres: {}
  - name: shop
    mysql:
      defaults-extra-file: /run/secrets/shop.cnf
  - name: world
    every: 6h
    path:
      dir: /srv/minecraft/world
`)
	require.NoError(t, err)

	require.Equal(t, 2, cfg.Schedule.Concurrency)
	require.Equal(t, 30*time.Second, cfg.Schedule.ShutdownGrace)
	require.True(t, cfg.Backups[0].Postgres.PITR)
	require.Equal(t, time.Minute, cfg.Backups[0].Postgres.CommitEvery)
	require.Equal(t, 14*24*time.Hour, cfg.Backups[1].MySQL.Window)
	require.Equal(t, 3, cfg.Backups[2].Path.ReadRetries)
	require.Equal(t, 30*time.Minute, cfg.Backups[2].Path.CheckpointInterval)

	type shape struct {
		set, kind, after string
		log, plan        bool
	}

	var got []shape
	for _, task := range cfg.Tasks() {
		require.Equal(t, task.Set, task.Name)
		got = append(got, shape{task.Set, task.Kind, task.After, task.Log, task.Plan != nil})
	}

	require.Equal(t, []shape{
		{"db/base", "postgres-base", "", false, true},
		{"db/wal", "postgres-receive", "", true, false},
		{"shop/dump", "mysql-dump", "", false, true},
		{"shop/binlog", "mysql-receive", "shop/dump", true, false},
		{"world", "path", "", false, true},
	}, got)
}

func TestAPathBackupIsWatchedUnlessItSaysOff(t *testing.T) {
	cfg, err := decode(t, `
backups:
  - {name: world, path: {dir: /srv/world, exclude: ["/logs/**"]}}
  - {name: mods, cron: "0 4 * * *", path: {dir: /srv/mods, min-gap: 1m, full-read-every: 48h}}
  - {name: etc, every: 1h, path: {dir: /etc, watch: off}}
`)
	require.NoError(t, err)

	tasks := cfg.Tasks()

	world := tasks[0]
	require.Equal(t, "every 24h0m0s", world.Plan.String(), "a watched backup without a plan runs daily while nothing changes")
	require.Equal(t, "/srv/world", world.Watch.Dir)
	require.Equal(t, 5*time.Minute, world.Watch.MinGap)
	require.Equal(t, 7*24*time.Hour, world.Watch.FullReadEvery)

	mods := tasks[1]
	require.Equal(t, "cron 0 4 * * *", mods.Plan.String())
	require.Equal(t, time.Minute, mods.Watch.MinGap)
	require.Equal(t, 48*time.Hour, mods.Watch.FullReadEvery)
	require.NotEqual(t, world.Watch.Settings, mods.Watch.Settings, "the filters are part of the settings")

	require.Nil(t, tasks[2].Watch)
	require.Equal(t, "every 1h0m0s", tasks[2].Plan.String())
}

func TestDecodeNestsNamesInGroups(t *testing.T) {
	cfg, err := decode(t, `
backups:
  - {name: srv1/db, postgres: {}}
  - {name: srv1/files, every: 1h, path: {dir: /srv}}
  - {name: srv1, every: 1h, path: {dir: /etc}}
`)
	require.NoError(t, err)

	var sets []string
	for _, task := range cfg.Tasks() {
		sets = append(sets, task.Set)
	}

	require.Equal(t, []string{"srv1/db/base", "srv1/db/wal", "srv1/files", "srv1"}, sets)
	require.Equal(t, "goback_srv1_db_wal", cfg.Backups[0].Postgres.slot(&cfg.Backups[0]))
}

func TestDecodeTakesPITROff(t *testing.T) {
	cfg, err := decode(t, `
backups:
  - name: db
    postgres:
      pitr: false
  - name: shop
    mysql:
      pitr: false
`)
	require.NoError(t, err)

	var sets []string
	for _, task := range cfg.Tasks() {
		sets = append(sets, task.Set)
	}

	require.Equal(t, []string{"db/base", "shop/dump"}, sets)
}

func TestDecodeRefuses(t *testing.T) {
	for name, doc := range map[string]string{
		"an unknown top-level key": `
stores: {}
backups: [{name: w, every: 1h, path: {dir: /w}}]`,
		"an unknown kind setting": `
backups: [{name: w, every: 1h, path: {dir: /w, excludes: [x]}}]`,
		"a passphrase in the file": `
store: {passphrase: secret}
backups: [{name: w, every: 1h, path: {dir: /w}}]`,
		"no kind": `
backups: [{name: w, every: 1h}]`,
		"two kinds": `
backups: [{name: w, every: 1h, path: {dir: /w}, mysql: {}}]`,
		"one name twice": `
backups:
  - {name: w, every: 1h, path: {dir: /w}}
  - {name: w, every: 2h, path: {dir: /v}}`,
		"an api key inline": `
store: {storage: "goback://secretkey@store.goback.io"}
backups: [{name: w, every: 1h, path: {dir: /w}}]`,
		"a password in a dbname": `
backups: [{name: db, postgres: {pitr: false, dbname: "host=db password=hunter2"}}]`,
		"a password in a dbname url": `
backups: [{name: db, postgres: {pitr: false, dbname: "postgres://u:hunter2@db/x"}}]`,
		"an unwatched path backup without a schedule": `
backups: [{name: w, path: {dir: /w, watch: off}}]`,
		"an unknown watch setting": `
backups: [{name: w, path: {dir: /w, watch: sometimes}}]`,
		"no gap after a change": `
backups: [{name: w, path: {dir: /w, min-gap: 0s}}]`,
		"every and cron": `
backups: [{name: w, every: 1h, cron: "0 3 * * *", path: {dir: /w}}]`,
		"a cron time zone": `
backups: [{name: w, cron: "CRON_TZ=UTC 0 3 * * *", path: {dir: /w}}]`,
		"a cron interval": `
backups: [{name: w, cron: "@every 1h", path: {dir: /w}}]`,
		"six cron fields": `
backups: [{name: w, cron: "0 0 3 * * *", path: {dir: /w}}]`,
		"a cron that never fires": `
backups: [{name: w, cron: "0 3 30 2 *", path: {dir: /w}}]`,
		"postgres commits more often than every 10s": `
backups: [{name: db, postgres: {commit-every: 5s}}]`,
		"postgres commit bytes that aren't a size": `
backups: [{name: db, postgres: {commit-bytes: lots}}]`,
		"mysql without binlog positions but with pitr": `
backups: [{name: shop, mysql: {no-binlog-position: true}}]`,
		"an empty name part": `
backups: [{name: srv1//db, every: 1h, path: {dir: /w}}]`,
		"a dot-dot name part": `
backups: [{name: srv1/../db, every: 1h, path: {dir: /w}}]`,
		"a backslash": `
backups: [{name: 'srv1\db', every: 1h, path: {dir: /w}}]`,
		"a path backup named all in digits": `
backups: [{name: "2024", every: 1h, path: {dir: /w}}]`,
		"a path backup writing a database backup's set": `
backups:
  - {name: srv1/db, postgres: {}}
  - {name: srv1/db/wal, every: 1h, path: {dir: /w}}`,
		"a log set under another log set": `
backups:
  - {name: srv1/db, postgres: {}}
  - {name: srv1/db/wal/x, mysql: {}}`,
		"no backups": `
store: {storage: /backups}`,
		"zero concurrency": `
schedule: {concurrency: 0}
backups: [{name: w, every: 1h, path: {dir: /w}}]`,
		"a retention period that isn't one": `
backups: [{name: db, retention: "keep=fortnightly=2", postgres: {}}]`,
		"a retention keeping no last commit": `
backups: [{name: w, every: 1h, retention: "keep-last=0", path: {dir: /w}}]`,
		"a retention that isn't a policy": `
backups: [{name: w, every: 1h, retention: "90 days", path: {dir: /w}}]`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decode(t, doc)
			require.Error(t, err)
		})
	}
}

func initialRetentions(cfg *Config) map[string]*retention.Policy {
	out := map[string]*retention.Policy{}
	for _, task := range cfg.Tasks() {
		out[task.Set] = task.InitialRetention
	}

	return out
}

func TestADatabaseBackupsSetsStartWithTheDatabasePolicies(t *testing.T) {
	cfg, err := decode(t, `
backups:
  - {name: db, postgres: {}}
  - {name: shop, mysql: {}}
  - {name: world, every: 6h, path: {dir: /srv/world}}
`)
	require.NoError(t, err)

	require.Equal(t, map[string]*retention.Policy{
		"db/base":     &retention.DatabaseBase,
		"db/wal":      &retention.DatabaseLog,
		"shop/dump":   &retention.DatabaseBase,
		"shop/binlog": &retention.DatabaseLog,
		"world":       nil,
	}, initialRetentions(cfg))
}

func TestARetentionKeyReplacesTheBaseSetsPolicyAndNotTheLogSets(t *testing.T) {
	cfg, err := decode(t, `
backups:
  - {name: db, retention: "keep-last=2 keep=daily=7", postgres: {}}
  - {name: shop, retention: "keep=weekly=8", mysql: {}}
  - {name: world, every: 6h, retention: "keep-within=720h", path: {dir: /srv/world}}
`)
	require.NoError(t, err)

	require.Equal(t, map[string]*retention.Policy{
		"db/base":     {KeepLast: 2, Brackets: []retention.Bracket{{Period: retention.Daily, Count: 7}}},
		"db/wal":      &retention.DatabaseLog,
		"shop/dump":   {KeepLast: 1, Brackets: []retention.Bracket{{Period: retention.Weekly, Count: 8}}},
		"shop/binlog": &retention.DatabaseLog,
		"world":       {KeepLast: 1, KeepWithin: 720 * time.Hour},
	}, initialRetentions(cfg))
}

func TestDecodeReportsEveryProblem(t *testing.T) {
	_, err := decode(t, `
backups:
  - {name: a, path: {dir: /a, watch: off}}
  - {name: b, postgres: {commit-every: 1s}}
`)
	require.Error(t, err)

	joined, ok := err.(interface{ Unwrap() []error })
	require.True(t, ok)
	require.Len(t, joined.Unwrap(), 2)
}

func TestBackupsOpenOnlyTheCachesTheirKindsUse(t *testing.T) {
	caches := func(doc string) goback.Caches {
		cfg, err := decode(t, doc)
		require.NoError(t, err)

		return cfg.Store.Caches(cfg.Backups)
	}

	require.Equal(t, goback.Caches{}, caches(`
backups:
  - {name: db, postgres: {pitr: false}}
  - {name: shop, mysql: {pitr: false}}
`))
	require.Equal(t, goback.Caches{Trees: true}, caches(`
backups:
  - {name: shop, mysql: {}}
`))
	require.Equal(t, goback.Caches{Stats: true, Objects: true, Trees: true, Blobs: true}, caches(`
backups:
  - {name: db, postgres: {}}
  - {name: files, every: 1h, path: {dir: /a}}
`))
	require.Equal(t, goback.Caches{Stats: true, Objects: true}, caches(`
store: {blob-cache-on-backup: false}
backups:
  - {name: files, every: 1h, path: {dir: /a}}
`))
}

func TestABackupNeedingOtherCachesTakesARestart(t *testing.T) {
	old, err := decode(t, "backups: [{name: shop, mysql: {}}]")
	require.NoError(t, err)

	cfg, err := decode(t, "backups: [{name: shop, mysql: {}}, {name: files, every: 1h, path: {dir: /a}}]")
	require.NoError(t, err)
	require.Equal(t, []string{"caches"}, cfg.RestartSections(old))

	cfg, err = decode(t, "backups: [{name: other, mysql: {}}]")
	require.NoError(t, err)
	require.Empty(t, cfg.RestartSections(old))
}

func TestStateDirDefaultsUnderTheCacheDir(t *testing.T) {
	cfg := &Config{}

	_, err := cfg.StateDir("")
	require.Error(t, err)

	dir, err := cfg.StateDir("cache")
	require.NoError(t, err)
	require.NotEmpty(t, dir)

	cfg.Schedule.StateDir = "state"
	dir, err = cfg.StateDir("cache")
	require.NoError(t, err)
	require.Equal(t, "state", dir)
}

func TestTasksKeepTheirSpecUnlessTheBackupChanges(t *testing.T) {
	spec := func(doc string) map[string]string {
		cfg, err := decode(t, doc)
		require.NoError(t, err)

		out := map[string]string{}
		for _, task := range cfg.Tasks() {
			out[task.Name] = task.Spec
		}

		return out
	}

	before := spec(`
backups:
  - {name: a, every: 1h, path: {dir: /a}}
  - {name: b, every: 1h, path: {dir: /b}}`)
	after := spec(`
backups:
  - {name: b, every: 2h, path: {dir: /b}}
  - {name: a, every: 1h, path: {dir: /a}}`)

	require.Equal(t, before["a"], after["a"])
	require.NotEqual(t, before["b"], after["b"])
}
