package agent

import (
	"strings"
	"testing"
	"time"

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
		{"db-base", "postgres-base", "", false, true},
		{"db-wal", "postgres-receive", "", true, false},
		{"shop-dump", "mysql-dump", "", false, true},
		{"shop-binlog", "mysql-receive", "shop-dump", true, false},
		{"world", "path", "", false, true},
	}, got)
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

	require.Equal(t, []string{"db-base", "shop-dump"}, sets)
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
		"two backups on one set": `
backups:
  - {name: db-base, every: 1h, path: {dir: /w}}
  - {name: db, postgres: {pitr: false}}`,
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
		"a path backup without a schedule": `
backups: [{name: w, path: {dir: /w}}]`,
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
		"no backups": `
store: {storage: /backups}`,
		"zero concurrency": `
schedule: {concurrency: 0}
backups: [{name: w, every: 1h, path: {dir: /w}}]`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decode(t, doc)
			require.Error(t, err)
		})
	}
}

func TestDecodeReportsEveryProblem(t *testing.T) {
	_, err := decode(t, `
backups:
  - {name: a, path: {dir: /a}}
  - {name: b, postgres: {commit-every: 1s}}
`)
	require.Error(t, err)

	joined, ok := err.(interface{ Unwrap() []error })
	require.True(t, ok)
	require.Len(t, joined.Unwrap(), 2)
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
